import asyncio
import datetime
import io
import re
import string
import sys
import tempfile
import types
from collections.abc import Iterator, Mapping, MutableMapping
from re import Pattern
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    Optional,
    TypedDict,
    TypeVar,
    cast,
    overload,
)
from urllib.parse import parse_qsl

from multidict import CIMultiDict, MultiDict, MultiDictProxy
from yarl import URL

from . import hdrs
from ._cookie_helpers import parse_cookie_header
from .abc import AbstractStreamWriter
from .helpers import (
    _SENTINEL,
    DEFAULT_CHUNK_SIZE,
    ETAG_ANY,
    LIST_QUOTED_ETAG_RE,
    ChainMapProxy,
    ETag,
    HeadersDictProxy,
    HeadersMixin,
    RequestKey,
    frozen_dataclass_decorator,
    is_expected_content_type,
    parse_http_date,
    reify,
    sentinel,
    set_exception,
)
from .http_parser import RawRequestMessage
from .http_writer import HttpVersion
from .multipart import BodyPartReader, MultipartReader
from .streams import EmptyStreamReader, StreamReader
from .typedefs import (
    DEFAULT_JSON_DECODER,
    JSONDecoder,
    LooseHeaders,
    RawHeaders,
    StrOrURL,
)
from .web_exceptions import (
    HTTPBadRequest,
    HTTPRequestEntityTooLarge,
    HTTPUnsupportedMediaType,
)
from .web_response import StreamResponse

if sys.version_info >= (3, 11):
    from typing import Self
else:
    Self = Any

__all__ = ("BaseRequest", "FileField", "Request")


if TYPE_CHECKING:
    from .web_app import Application
    from .web_protocol import RequestHandler
    from .web_urldispatcher import UrlMappingMatchInfo


_T = TypeVar("_T")


class _CloneKwargs(TypedDict, total=False):
    scheme: str
    host: str
    remote: str


@frozen_dataclass_decorator
class FileField:
    name: str
    filename: str
    file: io.BufferedReader
    content_type: str
    headers: HeadersDictProxy


_TCHAR: Final[str] = string.digits + string.ascii_letters + r"!#$%&'*+.^_`|~-"

_TOKEN: Final[str] = rf"[{_TCHAR}]+"

_QDTEXT: Final[str] = r"[{}]".format(
    r"".join(chr(c) for c in (0x09, 0x20, 0x21) + tuple(range(0x23, 0x7F)))
)

_FORWARDED_PAIR: Final[str] = (
    rf'[ \t]*({_TOKEN})=({_TOKEN}|".*")(:\d{{1,4}})?[ \t]*(?:\Z|;)'
)
_FORWARDED_PAIR_RE: Final[Pattern[str]] = re.compile(_FORWARDED_PAIR)



class BaseRequest(MutableMapping[str | RequestKey[Any], Any], HeadersMixin):
    POST_METHODS = {
        hdrs.METH_PATCH,
        hdrs.METH_POST,
        hdrs.METH_PUT,
        hdrs.METH_TRACE,
        hdrs.METH_DELETE,
    }

    _post: MultiDictProxy[str | bytes | FileField] | None = None
    _read_bytes: bytes | None = None
    _pre_handler_error: HTTPBadRequest | None = None

    def __init__(
        self,
        message: RawRequestMessage,
        payload: StreamReader,
        protocol: "RequestHandler[Self]",
        payload_writer: AbstractStreamWriter,
        task: "asyncio.Task[None]",
        loop: asyncio.AbstractEventLoop,
        *,
        client_max_size: int = 1024**2,
        state: dict[RequestKey[Any] | str, Any] | None = None,
        scheme: str | None = None,
        host: str | None = None,
        remote: str | None = None,
        pre_handler_error: HTTPBadRequest | None = None,
    ) -> None:
        self._message = message
        self._protocol = protocol
        self._payload_writer = payload_writer
        if pre_handler_error is not None:
            self._pre_handler_error = pre_handler_error

        self._payload = payload
        self._headers: HeadersDictProxy = message.headers
        self._method = message.method
        self._version = message.version
        self._cache: dict[str, Any] = {}
        url = message.url
        if url.absolute:
            if scheme is not None:
                url = url.with_scheme(scheme)
            if host is not None:
                url = url.with_host(host)
            self._cache["url"] = url
            self._cache["host"] = url.host
            self._cache["scheme"] = url.scheme
            self._rel_url = url.relative()
        else:
            self._rel_url = url
            if scheme is not None:
                self._cache["scheme"] = scheme
            if host is not None:
                self._cache["host"] = host

        self._state = {} if state is None else state
        self._task = task
        self._client_max_size = client_max_size
        self._loop = loop

        self._transport_sslcontext = protocol.ssl_context
        self._transport_peername = protocol.peername
        self._transport_sockname = protocol.sockname

        if remote is not None:
            self._cache["remote"] = remote

    def clone(
        self,
        *,
        method: str | _SENTINEL = sentinel,
        rel_url: StrOrURL | _SENTINEL = sentinel,
        headers: LooseHeaders | _SENTINEL = sentinel,
        scheme: str | _SENTINEL = sentinel,
        host: str | _SENTINEL = sentinel,
        remote: str | _SENTINEL = sentinel,
        client_max_size: int | _SENTINEL = sentinel,
    ) -> "BaseRequest":
        """Clone itself with replacement some attributes.

        Creates and returns a new instance of Request object. If no parameters
        are given, an exact copy is returned. If a parameter is not passed, it
        will reuse the one from the current request object.
        """
        if self._read_bytes:
            raise RuntimeError("Cannot clone request after reading its content")

        dct: dict[str, Any] = {}
        if method is not sentinel:
            dct["method"] = method
        if rel_url is not sentinel:
            new_url: URL = URL(rel_url)
            dct["url"] = new_url
            dct["path"] = str(new_url)
        if headers is not sentinel:
            new_headers = HeadersDictProxy(CIMultiDict(headers))
            dct["headers"] = new_headers
            dct["raw_headers"] = tuple(
                (k.encode("utf-8"), v.encode("utf-8"))
                for k, v in new_headers._md.items()
            )

        message = self._message._replace(**dct)

        kwargs: _CloneKwargs = {}
        if scheme is not sentinel:
            kwargs["scheme"] = scheme
        if host is not sentinel:
            kwargs["host"] = host
        if remote is not sentinel:
            kwargs["remote"] = remote
        if client_max_size is sentinel:
            client_max_size = self._client_max_size

        return self.__class__(
            message,
            self._payload,
            self._protocol,  # type: ignore[arg-type]
            self._payload_writer,
            self._task,
            self._loop,
            client_max_size=client_max_size,
            state=self._state.copy(),
            pre_handler_error=self._pre_handler_error,
            **kwargs,
        )

    @property
    def task(self) -> "asyncio.Task[None]":
        return self._task








    @overload  # type: ignore[override]
    def __getitem__(self, key: RequestKey[_T]) -> _T: ...

    @overload
    def __getitem__(self, key: str) -> Any: ...

    def __getitem__(self, key: str | RequestKey[_T]) -> Any:
        return self._state[key]

    @overload  # type: ignore[override]
    def __setitem__(self, key: RequestKey[_T], value: _T) -> None: ...

    @overload
    def __setitem__(self, key: str, value: Any) -> None: ...

    def __setitem__(self, key: str | RequestKey[_T], value: Any) -> None:
        self._state[key] = value

    def __delitem__(self, key: str | RequestKey[_T]) -> None:
        del self._state[key]

    def __len__(self) -> int:
        return len(self._state)

    def __iter__(self) -> Iterator[str | RequestKey[Any]]:
        return iter(self._state)


    @reify
    def secure(self) -> bool:
        pass

    @reify
    def forwarded(self) -> tuple[Mapping[str, str], ...]:
        pass

    @reify
    def scheme(self) -> str:
        pass

    @reify
    def method(self) -> str:
        """Read only property for getting HTTP method.

        The value is upper-cased str like 'GET', 'POST', 'PUT' etc.
        """
        return self._method

    @reify
    def version(self) -> HttpVersion:
        pass

    @reify
    def host(self) -> str:
        pass

    @reify
    def remote(self) -> str | None:
        pass

    @reify
    def url(self) -> URL:
        pass

    @reify
    def path(self) -> str:
        pass

    @reify
    def path_qs(self) -> str:
        pass

    @reify
    def raw_path(self) -> str:
        pass

    @reify
    def query(self) -> MultiDictProxy[str]:
        pass

    @reify
    def query_string(self) -> str:
        pass

    @reify
    def headers(self) -> HeadersDictProxy:
        pass

    @reify
    def raw_headers(self) -> RawHeaders:
        pass

    @reify
    def if_modified_since(self) -> datetime.datetime | None:
        pass

    @reify
    def if_unmodified_since(self) -> datetime.datetime | None:
        pass

    @staticmethod
    def _etag_values(etag_header: str) -> Iterator[ETag]:
        pass


    @reify
    def if_match(self) -> tuple[ETag, ...] | None:
        pass

    @reify
    def if_none_match(self) -> tuple[ETag, ...] | None:
        pass

    @reify
    def if_range(self) -> datetime.datetime | None:
        pass

    @reify
    def keep_alive(self) -> bool:
        """Is keepalive enabled by client?"""
        return not self._message.should_close

    @reify
    def cookies(self) -> Mapping[str, str]:
        pass

    @reify
    def http_range(self) -> "slice[int, int, int]":
        pass

    @reify
    def content(self) -> StreamReader:
        pass

    @property
    def can_read_body(self) -> bool:
        pass

    @reify
    def body_exists(self) -> bool:
        pass

    async def release(self) -> None:
        """Release request.

        Eat unread part of HTTP BODY if present.
        """
        while not self._payload.at_eof():
            await self._payload.readany()

    async def read(self) -> bytes:
        """Read request body if present.

        Returns bytes object with full request content.
        """
        if self._read_bytes is None:
            if self._client_max_size:
                self._payload.set_read_chunk_size(self._client_max_size)
            body = bytearray()
            while True:
                chunk = await self._payload.readany()
                body.extend(chunk)
                if self._client_max_size:
                    body_size = len(body)
                    if body_size > self._client_max_size:
                        raise HTTPRequestEntityTooLarge(self._client_max_size)
                if not chunk:
                    break
            self._read_bytes = bytes(body)
        return self._read_bytes

    async def text(self) -> str:
        """Return BODY as text using encoding from .charset."""
        bytes_body = await self.read()
        encoding = self.charset or "utf-8"
        try:
            return bytes_body.decode(encoding)
        except LookupError:
            raise HTTPUnsupportedMediaType()

    async def json(
        self,
        *,
        loads: JSONDecoder = DEFAULT_JSON_DECODER,
        content_type: str | None = "application/json",
    ) -> Any:
        """Return BODY as JSON."""
        body = await self.text()
        if content_type:
            if not is_expected_content_type(self.content_type, content_type):
                raise HTTPBadRequest(
                    text=(
                        "Attempt to decode JSON with "
                        "unexpected mimetype: %s" % self.content_type
                    )
                )

        return loads(body)

    async def multipart(self) -> MultipartReader:
        """Return async iterator to process BODY as multipart."""
        return MultipartReader(
            self._headers,
            self._payload,
            client_max_size=self._client_max_size,
            max_field_size=self._protocol.max_field_size,
            max_headers=self._protocol.max_headers,
            max_size_error_cls=HTTPRequestEntityTooLarge,
        )

    async def post(self) -> "MultiDictProxy[str | bytes | FileField]":
        """Return POST parameters."""
        if self._post is not None:
            return self._post
        if self._method not in self.POST_METHODS:
            self._post = MultiDictProxy(MultiDict())
            return self._post

        content_type = self.content_type
        if content_type not in (
            "",
            "application/x-www-form-urlencoded",
            "multipart/form-data",
        ):
            self._post = MultiDictProxy(MultiDict())
            return self._post

        out: MultiDict[str | bytes | FileField] = MultiDict()

        if content_type == "multipart/form-data":
            multipart = await self.multipart()
            max_size = self._client_max_size

            size = 0
            while (field := await multipart.next()) is not None:
                field_ct = field.headers.get(hdrs.CONTENT_TYPE)

                if isinstance(field, BodyPartReader):
                    if field.name is None:
                        raise ValueError("Multipart field missing name.")

                    if field.filename:
                        tmp = await self._loop.run_in_executor(
                            None, tempfile.TemporaryFile
                        )
                        while chunk := await field.read_chunk(size=DEFAULT_CHUNK_SIZE):
                            async for decoded_chunk in field.decode_iter(chunk):
                                await self._loop.run_in_executor(
                                    None, tmp.write, decoded_chunk
                                )
                                size += len(decoded_chunk)
                                if 0 < max_size < size:
                                    await self._loop.run_in_executor(None, tmp.close)
                                    raise HTTPRequestEntityTooLarge(max_size)
                        await self._loop.run_in_executor(None, tmp.seek, 0)

                        if field_ct is None:
                            field_ct = "application/octet-stream"

                        ff = FileField(
                            field.name,
                            field.filename,
                            cast(io.BufferedReader, tmp),
                            field_ct,
                            field.headers,
                        )
                        out.add(field.name, ff)
                    else:
                        raw_data = bytearray()
                        while chunk := await field.read_chunk():
                            size += len(chunk)
                            if 0 < max_size < size:
                                raise HTTPRequestEntityTooLarge(max_size)
                            raw_data.extend(chunk)

                        value = bytearray()
                        async for d in field.decode_iter(raw_data):  # type: ignore[arg-type]
                            value.extend(d)

                        if field_ct is None or field_ct.startswith("text/"):
                            charset = field.get_charset(default="utf-8")
                            out.add(field.name, value.decode(charset))
                        else:
                            out.add(field.name, value)  # type: ignore[arg-type]
                else:
                    raise ValueError(
                        "To decode nested multipart you need to use custom reader",
                    )
        else:
            data = await self.read()
            if data:
                charset = self.charset or "utf-8"
                bytes_query = data.rstrip()
                try:
                    query = bytes_query.decode(charset)
                except LookupError:
                    raise HTTPUnsupportedMediaType()
                out.extend(
                    parse_qsl(qs=query, keep_blank_values=True, encoding=charset)
                )

        self._post = MultiDictProxy(out)
        return self._post

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        """Extra info from protocol transport"""
        transport = self._protocol.transport
        if transport is None:
            return default

        return transport.get_extra_info(name, default)

    def __repr__(self) -> str:
        ascii_encodable_path = self.path.encode("ascii", "backslashreplace").decode(
            "ascii"
        )
        return f"<{self.__class__.__name__} {self._method} {ascii_encodable_path} >"

    def __eq__(self, other: object) -> bool:
        return id(self) == id(other)

    def __bool__(self) -> bool:
        return True

    async def _prepare_hook(self, response: StreamResponse) -> None:
        return

    def _cancel(self, exc: BaseException) -> None:
        set_exception(self._payload, exc)

    def _finish(self) -> None:
        if self._post is None or self.content_type != "multipart/form-data":
            return

        for file_name, file_field_object in self._post.items():
            if isinstance(file_field_object, FileField):
                file_field_object.file.close()


class Request(BaseRequest):

    _match_info: Optional["UrlMappingMatchInfo"] = None

    def clone(
        self,
        *,
        method: str | _SENTINEL = sentinel,
        rel_url: StrOrURL | _SENTINEL = sentinel,
        headers: LooseHeaders | _SENTINEL = sentinel,
        scheme: str | _SENTINEL = sentinel,
        host: str | _SENTINEL = sentinel,
        remote: str | _SENTINEL = sentinel,
        client_max_size: int | _SENTINEL = sentinel,
    ) -> "Request":
        ret = super().clone(
            method=method,
            rel_url=rel_url,
            headers=headers,
            scheme=scheme,
            host=host,
            remote=remote,
            client_max_size=client_max_size,
        )
        new_ret = cast(Request, ret)
        new_ret._match_info = self._match_info
        return new_ret

    @reify
    def match_info(self) -> "UrlMappingMatchInfo":
        pass

    @property
    def app(self) -> "Application":
        pass


    async def _prepare_hook(self, response: StreamResponse) -> None:
        match_info = self._match_info
        assert match_info is not None
        for app in match_info._apps:
            if on_response_prepare := app.on_response_prepare:
                await on_response_prepare.send(self, response)
