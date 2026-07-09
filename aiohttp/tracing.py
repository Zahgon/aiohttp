from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, overload

from aiosignal import Signal
from multidict import CIMultiDict
from yarl import URL

from .client_reqrep import ClientResponse
from .helpers import frozen_dataclass_decorator

if TYPE_CHECKING:
    from .client import ClientSession


__all__ = (
    "TraceConfig",
    "TraceRequestStartParams",
    "TraceRequestEndParams",
    "TraceRequestExceptionParams",
    "TraceConnectionQueuedStartParams",
    "TraceConnectionQueuedEndParams",
    "TraceConnectionCreateStartParams",
    "TraceConnectionCreateEndParams",
    "TraceConnectionReuseconnParams",
    "TraceDnsResolveHostStartParams",
    "TraceDnsResolveHostEndParams",
    "TraceDnsCacheHitParams",
    "TraceDnsCacheMissParams",
    "TraceRequestRedirectParams",
    "TraceRequestChunkSentParams",
    "TraceResponseChunkReceivedParams",
    "TraceRequestHeadersSentParams",
)

_T = TypeVar("_T", covariant=True)
_ParamT_contra = TypeVar("_ParamT_contra", contravariant=True)
_TracingSignal = Signal["ClientSession", _T, _ParamT_contra]


class _Factory(Protocol[_T]):
    def __call__(self, **kwargs: Any) -> _T: ...


class TraceConfig(Generic[_T]):

    @overload
    def __init__(self: "TraceConfig[SimpleNamespace]") -> None: ...
    @overload
    def __init__(self, trace_config_ctx_factory: _Factory[_T]) -> None: ...
    def __init__(
        self, trace_config_ctx_factory: _Factory[Any] = SimpleNamespace
    ) -> None:
        self._on_request_start: _TracingSignal[_T, TraceRequestStartParams] = Signal(
            self
        )
        self._on_request_chunk_sent: _TracingSignal[_T, TraceRequestChunkSentParams] = (
            Signal(self)
        )
        self._on_response_chunk_received: _TracingSignal[
            _T, TraceResponseChunkReceivedParams
        ] = Signal(self)
        self._on_request_end: _TracingSignal[_T, TraceRequestEndParams] = Signal(self)
        self._on_request_exception: _TracingSignal[_T, TraceRequestExceptionParams] = (
            Signal(self)
        )
        self._on_request_redirect: _TracingSignal[_T, TraceRequestRedirectParams] = (
            Signal(self)
        )
        self._on_connection_queued_start: _TracingSignal[
            _T, TraceConnectionQueuedStartParams
        ] = Signal(self)
        self._on_connection_queued_end: _TracingSignal[
            _T, TraceConnectionQueuedEndParams
        ] = Signal(self)
        self._on_connection_create_start: _TracingSignal[
            _T, TraceConnectionCreateStartParams
        ] = Signal(self)
        self._on_connection_create_end: _TracingSignal[
            _T, TraceConnectionCreateEndParams
        ] = Signal(self)
        self._on_connection_reuseconn: _TracingSignal[
            _T, TraceConnectionReuseconnParams
        ] = Signal(self)
        self._on_dns_resolvehost_start: _TracingSignal[
            _T, TraceDnsResolveHostStartParams
        ] = Signal(self)
        self._on_dns_resolvehost_end: _TracingSignal[
            _T, TraceDnsResolveHostEndParams
        ] = Signal(self)
        self._on_dns_cache_hit: _TracingSignal[_T, TraceDnsCacheHitParams] = Signal(
            self
        )
        self._on_dns_cache_miss: _TracingSignal[_T, TraceDnsCacheMissParams] = Signal(
            self
        )
        self._on_request_headers_sent: _TracingSignal[
            _T, TraceRequestHeadersSentParams
        ] = Signal(self)

        self._trace_config_ctx_factory: _Factory[_T] = trace_config_ctx_factory

    def trace_config_ctx(self, trace_request_ctx: Any = None) -> _T:
        """Return a new trace_config_ctx instance"""
        return self._trace_config_ctx_factory(trace_request_ctx=trace_request_ctx)

    def freeze(self) -> None:
        self._on_request_start.freeze()
        self._on_request_chunk_sent.freeze()
        self._on_response_chunk_received.freeze()
        self._on_request_end.freeze()
        self._on_request_exception.freeze()
        self._on_request_redirect.freeze()
        self._on_connection_queued_start.freeze()
        self._on_connection_queued_end.freeze()
        self._on_connection_create_start.freeze()
        self._on_connection_create_end.freeze()
        self._on_connection_reuseconn.freeze()
        self._on_dns_resolvehost_start.freeze()
        self._on_dns_resolvehost_end.freeze()
        self._on_dns_cache_hit.freeze()
        self._on_dns_cache_miss.freeze()
        self._on_request_headers_sent.freeze()


















@frozen_dataclass_decorator
class TraceRequestStartParams:

    method: str
    url: URL
    headers: "CIMultiDict[str]"


@frozen_dataclass_decorator
class TraceRequestChunkSentParams:

    method: str
    url: URL
    chunk: bytes


@frozen_dataclass_decorator
class TraceResponseChunkReceivedParams:

    method: str
    url: URL
    chunk: bytes


@frozen_dataclass_decorator
class TraceRequestEndParams:

    method: str
    url: URL
    headers: "CIMultiDict[str]"
    response: ClientResponse


@frozen_dataclass_decorator
class TraceRequestExceptionParams:

    method: str
    url: URL
    headers: "CIMultiDict[str]"
    exception: BaseException


@frozen_dataclass_decorator
class TraceRequestRedirectParams:

    method: str
    url: URL
    headers: "CIMultiDict[str]"
    response: ClientResponse


@frozen_dataclass_decorator
class TraceConnectionQueuedStartParams:
    pass


@frozen_dataclass_decorator
class TraceConnectionQueuedEndParams:
    pass


@frozen_dataclass_decorator
class TraceConnectionCreateStartParams:
    pass


@frozen_dataclass_decorator
class TraceConnectionCreateEndParams:
    pass


@frozen_dataclass_decorator
class TraceConnectionReuseconnParams:
    pass


@frozen_dataclass_decorator
class TraceDnsResolveHostStartParams:

    host: str


@frozen_dataclass_decorator
class TraceDnsResolveHostEndParams:

    host: str


@frozen_dataclass_decorator
class TraceDnsCacheHitParams:

    host: str


@frozen_dataclass_decorator
class TraceDnsCacheMissParams:

    host: str


@frozen_dataclass_decorator
class TraceRequestHeadersSentParams:

    method: str
    url: URL
    headers: "CIMultiDict[str]"


class Trace:

    def __init__(
        self,
        session: "ClientSession",
        trace_config: TraceConfig[object],
        trace_config_ctx: Any,
    ) -> None:
        self._trace_config = trace_config
        self._trace_config_ctx = trace_config_ctx
        self._session = session

    async def send_request_start(
        self, method: str, url: URL, headers: "CIMultiDict[str]"
    ) -> None:
        return await self._trace_config.on_request_start.send(
            self._session,
            self._trace_config_ctx,
            TraceRequestStartParams(method, url, headers),
        )



    async def send_request_end(
        self,
        method: str,
        url: URL,
        headers: "CIMultiDict[str]",
        response: ClientResponse,
    ) -> None:
        return await self._trace_config.on_request_end.send(
            self._session,
            self._trace_config_ctx,
            TraceRequestEndParams(method, url, headers, response),
        )

    async def send_request_exception(
        self,
        method: str,
        url: URL,
        headers: "CIMultiDict[str]",
        exception: BaseException,
    ) -> None:
        return await self._trace_config.on_request_exception.send(
            self._session,
            self._trace_config_ctx,
            TraceRequestExceptionParams(method, url, headers, exception),
        )

    async def send_request_redirect(
        self,
        method: str,
        url: URL,
        headers: "CIMultiDict[str]",
        response: ClientResponse,
    ) -> None:
        return await self._trace_config._on_request_redirect.send(
            self._session,
            self._trace_config_ctx,
            TraceRequestRedirectParams(method, url, headers, response),
        )










