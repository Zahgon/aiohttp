import asyncio
import codecs
import contextlib
import functools
import io
import re
import sys
import traceback
import warnings
from collections.abc import Callable, Iterable, Sequence
from hashlib import md5, sha1, sha256
from http.cookies import BaseCookie, SimpleCookie
from types import MappingProxyType, TracebackType
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, TypedDict

from multidict import CIMultiDict, CIMultiDictProxy, MultiDict, MultiDictProxy
from yarl import URL, Query

from . import hdrs, multipart, payload
from ._cookie_helpers import (
    parse_cookie_header,
    parse_set_cookie_headers,
    preserve_morsel_with_coded_value,
)
from .abc import AbstractStreamWriter
from .base_protocol import BaseProtocol
from .client_exceptions import (
    ClientConnectionError,
    ClientOSError,
    ClientResponseError,
    ContentTypeError,
    InvalidURL,
    ServerFingerprintMismatch,
)
from .compression_utils import HAS_BROTLI, HAS_ZSTD
from .formdata import FormData
from .helpers import (
    _SENTINEL,
    HTTP_AND_EMPTY_SCHEMA_SET,
    BaseTimerContext,
    HeadersDictProxy,
    HeadersMixin,
    TimerNoop,
    encode_basic_auth,
    frozen_dataclass_decorator,
    is_expected_content_type,
    parse_mimetype,
    reify,
    sentinel,
    set_exception,
    set_result,
)
from .http import (
    SERVER_SOFTWARE,
    HttpProcessingError,
    HttpVersion,
    HttpVersion10,
    HttpVersion11,
    StreamWriter,
)
from .streams import EMPTY_PAYLOAD, StreamReader
from .typedefs import DEFAULT_JSON_DECODER, JSONDecoder, RawHeaders

try:
    import ssl
    from ssl import SSLContext
except ImportError:  # pragma: no cover
    ssl = None  # type: ignore[assignment]
    SSLContext = object  # type: ignore[misc,assignment]


__all__ = ("ClientRequest", "ClientResponse", "RequestInfo", "Fingerprint")


if TYPE_CHECKING:
    from .client import ClientSession
    from .connector import Connection
    from .tracing import Trace


_CONNECTION_CLOSED_EXCEPTION = ClientConnectionError("Connection closed")
_CONTAINS_CONTROL_CHAR_RE = re.compile(r"[^-!#$%&'*+.^_`|~0-9a-zA-Z]")
_DIGITS_RE = re.compile(r"\d+", re.ASCII)


@frozen_dataclass_decorator
class ClientTimeout:
    total: float | None = 5 * 60  # 5 minute default timeout
    connect: float | None = None
    sock_read: float | None = None
    sock_connect: float | None = None
    ceil_threshold: float = 5



    def __post_init__(self) -> None:
        if self.total is None:
            return
        object.__setattr__(
            self,
            "total",
            max(
                self.total,
                self.connect or 0,
                self.sock_read or 0,
                self.sock_connect or 0,
            ),
        )

        if self.total == 0:
            raise ValueError(
                "total timeout must be a positive number or None to disable, "
                "got 0. Using 0 to disable timeouts is no longer supported, "
                "use None instead."
            )


def _gen_default_accept_encoding() -> str:
    encodings = [
        "gzip",
        "deflate",
    ]
    if HAS_BROTLI:
        encodings.append("br")
    if HAS_ZSTD:
        encodings.append("zstd")
    return ", ".join(encodings)


@frozen_dataclass_decorator
class ContentDisposition:
    type: str | None
    parameters: "MappingProxyType[str, str]"
    filename: str | None


class _RequestInfo(NamedTuple):
    url: URL
    method: str
    headers: "CIMultiDictProxy[str]"
    real_url: URL


class RequestInfo(_RequestInfo):

    def __new__(
        cls,
        url: URL,
        method: str,
        headers: "CIMultiDictProxy[str]",
        real_url: URL | _SENTINEL = sentinel,
    ) -> "RequestInfo":
        """Create a new RequestInfo instance.

        For backwards compatibility, the real_url parameter is optional.
        """
        return tuple.__new__(
            cls, (url, method, headers, url if real_url is sentinel else real_url)
        )


class Fingerprint:
    HASHFUNC_BY_DIGESTLEN = {
        16: md5,
        20: sha1,
        32: sha256,
    }

    def __init__(self, fingerprint: bytes) -> None:
        digestlen = len(fingerprint)
        hashfunc = self.HASHFUNC_BY_DIGESTLEN.get(digestlen)
        if not hashfunc:
            raise ValueError("fingerprint has invalid length")
        elif hashfunc is md5 or hashfunc is sha1:
            raise ValueError("md5 and sha1 are insecure and not supported. Use sha256.")
        self._hashfunc = hashfunc
        self._fingerprint = fingerprint




if ssl is not None:
    SSL_ALLOWED_TYPES = (ssl.SSLContext, bool, Fingerprint)
else:  # pragma: no cover
    SSL_ALLOWED_TYPES = (bool,)  # type: ignore[unreachable]


_CONNECTION_CLOSED_EXCEPTION = ClientConnectionError("Connection closed")
_SSL_SCHEMES = frozenset(("https", "wss"))


class ConnectionKey(NamedTuple):
    host: str
    port: int | None
    is_ssl: bool
    ssl: SSLContext | bool | Fingerprint
    proxy: URL | None
    proxy_headers_hash: int | None  # hash(CIMultiDict)
    server_hostname: str | None = None


class ResponseParams(TypedDict):
    timer: BaseTimerContext | None
    skip_payload: bool
    read_until_eof: bool
    auto_decompress: bool
    read_timeout: float | None
    read_bufsize: int
    timeout_ceil_threshold: float
    max_line_size: int
    max_field_size: int
    max_headers: int


class ClientResponse(HeadersMixin):
    version: HttpVersion | None = None  # HTTP-Version
    status: int = None  # type: ignore[assignment] # Status-Code
    reason: str | None = None  # Reason-Phrase

    content: StreamReader = None  # type: ignore[assignment] # Payload stream
    _body: bytes | None = None
    _headers: HeadersDictProxy = None  # type: ignore[assignment]
    _history: tuple["ClientResponse", ...] = ()
    _raw_headers: RawHeaders = None  # type: ignore[assignment]
    _upgraded: bool = False  # parser saw a Connection: upgrade token

    _connection: "Connection | None" = None  # current connection
    _cookies: SimpleCookie | None = None
    _raw_cookie_headers: tuple[str, ...] | None = None
    _continue: asyncio.Future[bool] | None = None
    _source_traceback: traceback.StackSummary | None = None
    _session: "ClientSession | None" = None
    _closed = True  # to allow __del__ for non-initialized properly response
    _released = False
    _in_context = False

    _resolve_charset: Callable[["ClientResponse", bytes], str] = lambda *_: "utf-8"

    __writer: asyncio.Task[None] | None = None
    _stream_writer: AbstractStreamWriter | None = None
    _output_size: int = 0
    _upload_complete: asyncio.Future[None] | None = None

    def __init__(
        self,
        method: str,
        url: URL,
        *,
        writer: asyncio.Task[None] | None,
        continue100: asyncio.Future[bool] | None,
        timer: BaseTimerContext | None,
        traces: Sequence["Trace"],
        loop: asyncio.AbstractEventLoop,
        session: "ClientSession | None",
        request_headers: CIMultiDict[str],
        original_url: URL,
        stream_writer: AbstractStreamWriter,
        **kwargs: object,
    ) -> None:
        assert not kwargs, "Unexpected arguments to ClientResponse"
        assert type(url) is URL

        self.method = method

        self._real_url = url
        self._url = url.with_fragment(None) if url.raw_fragment else url
        if writer is None:  # Request already sent
            self._output_size = stream_writer.output_size
        else:
            self._stream_writer = stream_writer
            self._writer = writer
        if continue100 is not None:
            self._continue = continue100
        self._request_headers = request_headers
        self._original_url = original_url
        self._timer = timer if timer is not None else TimerNoop()
        self._cache: dict[str, Any] = {}
        self._traces = traces
        self._loop = loop
        if session is not None:
            self._session = session
            self._resolve_charset = session._resolve_charset
        if loop.get_debug():
            self._source_traceback = traceback.extract_stack(sys._getframe(1))


    @property
    def _writer(self) -> asyncio.Task[None] | None:
        pass

    @_writer.setter
    def _writer(self, writer: asyncio.Task[None] | None) -> None:
        pass

    @property
    def output_size(self) -> int:
        pass

    @property
    def upload_complete(self) -> "asyncio.Future[None]":
        pass










    def __del__(self, _warnings: Any = warnings) -> None:
        if self._closed:
            return

        if self._connection is not None:
            self._connection.release()
            self._cleanup_writer()

            if self._loop.get_debug():
                _warnings.warn(
                    f"Unclosed response {self!r}", ResourceWarning, source=self
                )
                context = {"client_response": self, "message": "Unclosed response"}
                if self._source_traceback:
                    context["source_traceback"] = self._source_traceback
                self._loop.call_exception_handler(context)

    def __repr__(self) -> str:
        out = io.StringIO()
        ascii_encodable_url = str(self.url)
        if self.reason:
            ascii_encodable_reason = self.reason.encode(
                "ascii", "backslashreplace"
            ).decode("ascii")
        else:
            ascii_encodable_reason = "None"
        print(
            f"<ClientResponse({ascii_encodable_url}) [{self.status} {ascii_encodable_reason}]>",
            file=out,
        )
        print(self.headers, file=out)
        return out.getvalue()


    @reify
    def history(self) -> tuple["ClientResponse", ...]:
        pass


    async def start(self, connection: "Connection") -> "ClientResponse":
        """Start response processing."""
        self._closed = False
        self._protocol = connection.protocol
        self._connection = connection

        with self._timer:
            while True:
                try:
                    protocol = self._protocol
                    message, payload = await protocol.read()  # type: ignore[union-attr]
                except HttpProcessingError as exc:
                    raise ClientResponseError(
                        self.request_info,
                        self.history,
                        status=exc.code,
                        message=exc.message,
                        headers=exc.headers,
                    ) from exc

                if message.code < 100 or message.code > 199 or message.code == 101:
                    break

                if self._continue is not None:
                    set_result(self._continue, True)
                    self._continue = None

        payload.on_eof(self._response_eof)

        self.version = message.version
        self.status = message.code
        self.reason = message.reason

        self._headers = message.headers
        self._raw_headers = message.raw_headers
        self._upgraded = message.upgrade

        self.content = payload

        if self._traces and payload is not EMPTY_PAYLOAD:
            payload._on_chunk_received = self._on_chunk_response_received

        if cookie_hdrs := self.headers._md.getall(hdrs.SET_COOKIE, ()):
            self._raw_cookie_headers = tuple(cookie_hdrs)
        return self



    def close(self) -> None:
        if not self._released:
            self._notify_content()

        self._closed = True
        if self._loop.is_closed():
            return

        self._cleanup_writer()
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def release(self) -> None:
        if not self._released:
            self._notify_content()

        self._closed = True

        self._cleanup_writer()
        self._release_connection()

    @property
    def ok(self) -> bool:
        pass

    def raise_for_status(self) -> None:
        if not self.ok:
            assert self.reason is not None

            if not self._in_context:
                self.release()

            raise ClientResponseError(
                self.request_info,
                self.history,
                status=self.status,
                message=self.reason,
                headers=self.headers,
            )

    def _release_connection(self) -> None:
        if self._connection is not None:
            if self.__writer is None:
                self._connection.release()
                self._connection = None
            else:
                self.__writer.add_done_callback(lambda f: self._release_connection())

    async def _wait_released(self) -> None:
        if self.__writer is not None:
            try:
                await self.__writer
            except asyncio.CancelledError:
                if (
                    sys.version_info >= (3, 11)
                    and (task := asyncio.current_task())
                    and task.cancelling()
                ):
                    raise
        self._release_connection()

    def _cleanup_writer(self) -> None:
        if self.__writer is not None:
            self.__writer.cancel()
        if self._stream_writer is not None:
            self._output_size = self._stream_writer.output_size
            self._stream_writer = None
        self._session = None

    def _notify_content(self) -> None:
        content = self.content
        if content:  # type: ignore[truthy-bool]
            if content.exception() is None:
                set_exception(content, _CONNECTION_CLOSED_EXCEPTION)
            if content._on_chunk_received is not None:
                content._on_chunk_received = None
        self._released = True

    async def wait_for_close(self) -> None:
        if self.__writer is not None:
            try:
                await self.__writer
            except asyncio.CancelledError:
                if (
                    sys.version_info >= (3, 11)
                    and (task := asyncio.current_task())
                    and task.cancelling()
                ):
                    raise
        self.release()


    async def read(self) -> bytes:
        """Read response payload."""
        if self._body is None:
            try:
                self._body = await self.content.read()
            except BaseException:
                self.close()
                raise
        elif self._released:  # Response explicitly released
            raise ClientConnectionError("Connection closed")

        protocol = self._connection and self._connection.protocol
        if protocol is None or not protocol.upgraded:
            await self._wait_released()  # Underlying connection released
        return self._body

    def get_encoding(self) -> str:
        ctype = self.headers.get(hdrs.CONTENT_TYPE, "").lower()
        mimetype = parse_mimetype(ctype)

        encoding = mimetype.parameters.get("charset")
        if encoding:
            with contextlib.suppress(LookupError, ValueError):
                return codecs.lookup(encoding).name

        if mimetype.type == "application" and (
            mimetype.subtype == "json" or mimetype.subtype == "rdap"
        ):
            return "utf-8"

        if self._body is None:
            raise RuntimeError(
                "Cannot compute fallback encoding of a not yet read body"
            )

        return self._resolve_charset(self, self._body)

    async def text(self, encoding: str | None = None, errors: str = "strict") -> str:
        """Read response payload and decode."""
        await self.read()

        if encoding is None:
            encoding = self.get_encoding()

        return self._body.decode(encoding, errors=errors)  # type: ignore[union-attr]

    async def json(
        self,
        *,
        encoding: str | None = None,
        loads: JSONDecoder = DEFAULT_JSON_DECODER,
        content_type: str | None = "application/json",
    ) -> Any:
        """Read and decodes JSON response."""
        await self.read()

        if content_type:
            if not is_expected_content_type(self.content_type, content_type):
                raise ContentTypeError(
                    self.request_info,
                    self.history,
                    status=self.status,
                    message=(
                        "Attempt to decode JSON with "
                        "unexpected mimetype: %s" % self.content_type
                    ),
                    headers=self.headers,
                )

        if encoding is None:
            encoding = self.get_encoding()

        return loads(self._body.decode(encoding))  # type: ignore[union-attr]

    async def __aenter__(self) -> "ClientResponse":
        self._in_context = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._in_context = False
        self.release()
        await self.wait_for_close()


class ClientRequestBase:

    POST_METHODS = {hdrs.METH_PATCH, hdrs.METH_POST, hdrs.METH_PUT}

    proxy: URL | None = None
    response_class = ClientResponse
    server_hostname: str | None = None  # Needed in connector.py
    version = HttpVersion11
    _response = None

    url = URL()
    method = "GET"

    _writer_task: asyncio.Task[None] | None = None  # async task for streaming data

    _skip_auto_headers: "CIMultiDict[None] | None" = None


    def __init__(
        self,
        method: str,
        url: URL,
        *,
        headers: CIMultiDict[str],
        loop: asyncio.AbstractEventLoop,
        ssl: SSLContext | bool | Fingerprint,
        trust_env: bool = False,
    ):
        if match := _CONTAINS_CONTROL_CHAR_RE.search(method):
            raise ValueError(
                f"Method cannot contain non-token characters {method!r} "
                f"(found at least {match.group()!r})"
            )
        assert type(url) is URL, url
        self.original_url = url
        self.url = url.with_fragment(None) if url.raw_fragment else url
        self.method = method.upper()
        self.loop = loop
        self._ssl = ssl

        if loop.get_debug():
            self._source_traceback = traceback.extract_stack(sys._getframe(1))

        if not url.raw_host:
            raise InvalidURL(url)
        self._update_headers(headers)
        if url.raw_user or url.raw_password:
            self.headers[hdrs.AUTHORIZATION] = encode_basic_auth(
                url.user or "", url.password or ""
            )


    def _get_content_length(self) -> int | None:
        """Extract and validate Content-Length header value.

        Returns parsed Content-Length value or None if not set.
        Raises ValueError if header exists but cannot be parsed as an integer.
        """
        if hdrs.CONTENT_LENGTH not in self.headers:
            return None

        content_length_hdr = self.headers[hdrs.CONTENT_LENGTH]
        if not _DIGITS_RE.fullmatch(content_length_hdr):
            raise ValueError(f"Invalid Content-Length header: {content_length_hdr!r}")
        return int(content_length_hdr)



    def is_ssl(self) -> bool:
        return self.url.scheme in _SSL_SCHEMES



    def _update_headers(self, headers: CIMultiDict[str]) -> None:
        """Update request headers."""
        self.headers: CIMultiDict[str] = CIMultiDict()

        host = self.url.host_port_subcomponent

        assert host is not None
        self.headers[hdrs.HOST] = headers.pop(hdrs.HOST, host)
        self.headers.extend(headers)

    def _create_response(
        self,
        task: asyncio.Task[None] | None,
        stream_writer: AbstractStreamWriter,
    ) -> ClientResponse:
        return self.response_class(
            self.method,
            self.original_url,
            writer=task,
            continue100=None,
            timer=TimerNoop(),
            traces=(),
            loop=self.loop,
            session=None,
            request_headers=self.headers,
            original_url=self.original_url,
            stream_writer=stream_writer,
        )

    def _create_writer(self, protocol: BaseProtocol) -> StreamWriter:
        return StreamWriter(protocol, self.loop)

    def _should_write(self, protocol: BaseProtocol) -> bool:
        return protocol.writing_paused

    async def _send(self, conn: "Connection") -> ClientResponse:
        if self.method == hdrs.METH_CONNECT:
            connect_host = self.url.host_subcomponent
            assert connect_host is not None
            path = f"{connect_host}:{self.url.port}"
        elif self.proxy and not self.is_ssl():
            path = str(self.url)
        else:
            path = self.url.raw_path_qs

        protocol = conn.protocol
        assert protocol is not None
        writer = self._create_writer(protocol)

        if (
            self.method in self.POST_METHODS
            and (
                self._skip_auto_headers is None
                or hdrs.CONTENT_TYPE not in self._skip_auto_headers
            )
            and hdrs.CONTENT_TYPE not in self.headers
        ):
            self.headers[hdrs.CONTENT_TYPE] = "application/octet-stream"

        v = self.version
        if hdrs.CONNECTION not in self.headers:
            if conn._connector.force_close:
                if v == HttpVersion11:
                    self.headers[hdrs.CONNECTION] = "close"
            elif v == HttpVersion10:
                self.headers[hdrs.CONNECTION] = "keep-alive"

        status_line = f"{self.method} {path} HTTP/{v.major}.{v.minor}"

        await writer.write_headers(status_line, self.headers)

        task: asyncio.Task[None] | None
        if self._should_write(protocol):
            coro = self._write_bytes(writer, conn, self._get_content_length())
            if sys.version_info >= (3, 12):
                task = asyncio.Task(coro, loop=self.loop, eager_start=True)
            else:
                task = self.loop.create_task(coro)
            if task.done():
                task = None
            else:
                self._writer = task
        else:
            protocol.start_timeout()
            writer.set_eof()
            task = None
        self._response = self._create_response(task, stream_writer=writer)
        return self._response

    async def _write_bytes(
        self,
        writer: AbstractStreamWriter,
        conn: "Connection",
        content_length: int | None,
    ) -> None:
        assert False


class ClientRequestArgs(TypedDict, total=False):
    params: Query
    headers: CIMultiDict[str]
    skip_auto_headers: Iterable[str] | None
    data: Any
    cookies: BaseCookie[str]
    version: HttpVersion
    compress: Literal["deflate", "gzip"] | bool
    chunked: bool | None
    expect100: bool
    loop: asyncio.AbstractEventLoop
    response_class: type[ClientResponse]
    proxy: URL | None
    response_params: ResponseParams
    timer: BaseTimerContext
    timeout: ClientTimeout
    session: "ClientSession"
    ssl: SSLContext | bool | Fingerprint
    proxy_headers: CIMultiDict[str] | None
    traces: list["Trace"]
    trust_env: bool
    server_hostname: str | None


class ClientRequest(ClientRequestBase):
    _EMPTY_BODY = payload.PAYLOAD_REGISTRY.get(b"", disposition=None)
    _body = _EMPTY_BODY
    _continue = None  # waiter future for '100 Continue' response
    _response_params: ResponseParams = None  # type: ignore[assignment]
    _session: "ClientSession" = None  # type: ignore[assignment]
    _timeout = ClientTimeout()
    _traces: list["Trace"] = ()  # type: ignore[assignment]

    GET_METHODS = {
        hdrs.METH_GET,
        hdrs.METH_HEAD,
        hdrs.METH_OPTIONS,
        hdrs.METH_TRACE,
    }
    DEFAULT_HEADERS = {
        hdrs.ACCEPT: "*/*",
        hdrs.ACCEPT_ENCODING: _gen_default_accept_encoding(),
    }

    def __init__(
        self,
        method: str,
        url: URL,
        *,
        params: Query,
        headers: CIMultiDict[str],
        skip_auto_headers: Iterable[str] | None,
        data: Any,
        cookies: BaseCookie[str],
        version: HttpVersion,
        compress: Literal["deflate", "gzip"] | bool,
        chunked: bool | None,
        expect100: bool,
        loop: asyncio.AbstractEventLoop,
        response_class: type[ClientResponse],
        proxy: URL | None,
        response_params: ResponseParams,
        timer: BaseTimerContext,
        timeout: ClientTimeout,
        session: "ClientSession",
        ssl: SSLContext | bool | Fingerprint,
        proxy_headers: CIMultiDict[str] | None,
        traces: list["Trace"],
        trust_env: bool,
        server_hostname: str | None,
        **kwargs: object,
    ):
        assert not kwargs, "Unexpected arguments to ClientRequest"

        if params:
            url = url.extend_query(params)
        super().__init__(method, url, headers=headers, loop=loop, ssl=ssl)

        if proxy is not None:
            assert type(proxy) is URL, proxy
        self._session = session
        self.chunked = chunked
        self.response_class = response_class
        self._response_params = response_params
        self._timer = timer
        self._timeout = timeout
        self.server_hostname = server_hostname
        self.version = version

        self._update_auto_headers(skip_auto_headers)
        self._update_cookies(cookies)
        self._update_content_encoding(data, compress)
        self._update_proxy(proxy, proxy_headers)

        self._update_body_from_data(data)
        if data is not None or self.method not in self.GET_METHODS:
            self._update_transfer_encoding()
        self._update_expect_continue(expect100)
        self._traces = traces

    @property
    def body(self) -> payload.Payload:
        return self._body



    @property
    def session(self) -> "ClientSession":
        pass

    def _update_auto_headers(self, skip_auto_headers: Iterable[str] | None) -> None:
        if skip_auto_headers is not None:
            self._skip_auto_headers = CIMultiDict(
                (hdr, None) for hdr in sorted(skip_auto_headers)
            )
            used_headers = self.headers.copy()
            used_headers.extend(self._skip_auto_headers)  # type: ignore[arg-type]
        else:
            used_headers = self.headers

        for hdr, val in self.DEFAULT_HEADERS.items():
            if hdr not in used_headers:
                self.headers[hdr] = val

        if hdrs.USER_AGENT not in used_headers:
            self.headers[hdrs.USER_AGENT] = SERVER_SOFTWARE

    def _update_cookies(self, cookies: BaseCookie[str]) -> None:
        """Update request cookies header."""
        if not cookies:
            return

        c = SimpleCookie()
        if hdrs.COOKIE in self.headers:
            c.update(parse_cookie_header(self.headers.get(hdrs.COOKIE, "")))
            del self.headers[hdrs.COOKIE]

        for name, value in cookies.items():
            c[name] = preserve_morsel_with_coded_value(value)

        self.headers[hdrs.COOKIE] = c.output(header="", sep=";").strip()

    def _update_content_encoding(
        self, data: Any, compress: bool | Literal["deflate", "gzip"]
    ) -> None:
        """Set request content encoding."""
        self.compress = None
        if not data:
            return

        if self.headers.get(hdrs.CONTENT_ENCODING):
            if compress:
                raise ValueError(
                    "compress can not be set if Content-Encoding header is set"
                )
        elif compress:
            if isinstance(compress, str) and compress not in {"deflate", "gzip"}:
                raise ValueError(
                    "compress must be one of True, False, 'deflate', or 'gzip'"
                )
            self.compress = compress if isinstance(compress, str) else "deflate"
            self.headers[hdrs.CONTENT_ENCODING] = self.compress
            self.chunked = True  # enable chunked, no need to deal with length

    def _update_transfer_encoding(self) -> None:
        """Analyze transfer-encoding header."""
        te = self.headers.get(hdrs.TRANSFER_ENCODING, "").lower()

        if "chunked" in te:
            if self.chunked:
                raise ValueError(
                    "chunked can not be set "
                    'if "Transfer-Encoding: chunked" header is set'
                )

        elif self.chunked:
            if hdrs.CONTENT_LENGTH in self.headers:
                raise ValueError(
                    "chunked can not be set if Content-Length header is set"
                )

            self.headers[hdrs.TRANSFER_ENCODING] = "chunked"

    def _update_body_from_data(self, body: Any) -> None:
        """Update request body from data."""
        if body is None:
            self._body = self._EMPTY_BODY
            if (
                self.method not in self.GET_METHODS
                and not self.chunked
                and hdrs.CONTENT_LENGTH not in self.headers
            ):
                self.headers[hdrs.CONTENT_LENGTH] = "0"
            return

        if isinstance(body, FormData):
            body = body()
        else:
            try:
                body = payload.PAYLOAD_REGISTRY.get(body, disposition=None)
            except payload.LookupError:
                boundary = None
                if hdrs.CONTENT_TYPE in self.headers:
                    boundary = parse_mimetype(
                        self.headers[hdrs.CONTENT_TYPE]
                    ).parameters.get("boundary")
                body = FormData(body, boundary=boundary)()

        self._body = body

        # enable chunked encoding if needed
        if not self.chunked and hdrs.CONTENT_LENGTH not in self.headers:
            if (size := body.size) is not None:
                self.headers[hdrs.CONTENT_LENGTH] = str(size)
            else:
                self.chunked = True

        assert body.headers
        headers = self.headers
        skip_headers = self._skip_auto_headers
        for key, value in body.headers.items():
            if key in headers or (skip_headers is not None and key in skip_headers):
                continue
            headers[key] = value

    def _update_body(self, body: Any) -> None:
        pass

    async def update_body(self, body: Any) -> None:
        pass

    def _update_expect_continue(self, expect: bool = False) -> None:
        if expect:
            self.headers[hdrs.EXPECT] = "100-continue"
        elif (
            hdrs.EXPECT in self.headers
            and self.headers[hdrs.EXPECT].lower() == "100-continue"
        ):
            expect = True

        if expect:
            self._continue = self.loop.create_future()

    def _update_proxy(
        self,
        proxy: URL | None,
        proxy_headers: CIMultiDict[str] | None,
    ) -> None:
        if proxy is None:
            self.proxy = None
            self.proxy_headers = None
            return

        if proxy.scheme not in HTTP_AND_EMPTY_SCHEMA_SET:
            raise ValueError(
                f"aiohttp only supports http(s) proxies (got: {proxy.scheme!r}).\n"
                "See third-party libraries for other proxy schemes."
            )

        if proxy.raw_user or proxy.raw_password:
            auth_header = encode_basic_auth(proxy.user or "", proxy.password or "")
            if proxy_headers is None:
                proxy_headers = CIMultiDict()
            proxy_headers.setdefault(hdrs.PROXY_AUTHORIZATION, auth_header)
            proxy = proxy.with_user(None)
        self.proxy = proxy
        self.proxy_headers = proxy_headers

    def _create_response(
        self,
        task: asyncio.Task[None] | None,
        stream_writer: AbstractStreamWriter,
    ) -> ClientResponse:
        return self.response_class(
            self.method,
            self.original_url,
            writer=task,
            continue100=self._continue,
            timer=self._timer,
            traces=self._traces,
            loop=self.loop,
            session=self._session,
            request_headers=self.headers,
            original_url=self.original_url,
            stream_writer=stream_writer,
        )

    def _create_writer(self, protocol: BaseProtocol) -> StreamWriter:
        writer = StreamWriter(
            protocol,
            self.loop,
            on_chunk_sent=(
                functools.partial(self._on_chunk_request_sent, self.method, self.url)
                if self._traces
                else None
            ),
            on_headers_sent=(
                functools.partial(self._on_headers_request_sent, self.method, self.url)
                if self._traces
                else None
            ),
        )

        if self.compress:
            writer.enable_compression(self.compress)

        if self.chunked is not None:
            writer.enable_chunking()
        return writer

    def _should_write(self, protocol: BaseProtocol) -> bool:
        return (
            self.body.size != 0 or self._continue is not None or protocol.writing_paused
        )

    async def _write_bytes(
        self,
        writer: AbstractStreamWriter,
        conn: "Connection",
        content_length: int | None,
    ) -> None:
        """
        Write the request body to the connection stream.

        This method handles writing different types of request bodies:
        1. Payload objects (using their specialized write_with_length method)
        2. Bytes/bytearray objects
        3. Iterable body content

        Args:
            writer: The stream writer to write the body to
            conn: The connection being used for this request
            content_length: Optional maximum number of bytes to write from the body
                            (None means write the entire body)

        The method properly handles:
        - Waiting for 100-Continue responses if required
        - Content length constraints for chunked encoding
        - Error handling for network issues, cancellation, and other exceptions
        - Signaling EOF and timeout management

        Raises:
            ClientOSError: When there's an OS-level error writing the body
            ClientConnectionError: When there's a general connection error
            asyncio.CancelledError: When the operation is cancelled

        """
        if self._continue is not None:
            writer.send_headers()
            await writer.drain()
            await self._continue

        protocol = conn.protocol
        assert protocol is not None
        try:
            await self._body.write_with_length(writer, content_length)
        except OSError as underlying_exc:
            reraised_exc = underlying_exc

            exc_is_not_timeout = underlying_exc.errno is not None or not isinstance(
                underlying_exc, asyncio.TimeoutError
            )
            if exc_is_not_timeout:
                reraised_exc = ClientOSError(
                    underlying_exc.errno,
                    f"Can not write request body for {self.url !s}",
                )

            set_exception(protocol, reraised_exc, underlying_exc)
        except asyncio.CancelledError:
            conn.close()
            raise
        except Exception as underlying_exc:
            set_exception(
                protocol,
                ClientConnectionError(
                    "Failed to send bytes into the underlying connection "
                    f"{conn !s}: {underlying_exc!r}",
                ),
                underlying_exc,
            )
        else:
            await writer.write_eof()
            protocol.start_timeout()

    async def _close(self) -> None:
        if self._writer_task is not None:
            try:
                await self._writer_task
            except asyncio.CancelledError:
                if (
                    sys.version_info >= (3, 11)
                    and (task := asyncio.current_task())
                    and task.cancelling()
                ):
                    raise



