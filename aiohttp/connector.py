import asyncio
import functools
import random
import socket
import sys
import traceback
import warnings
from collections import OrderedDict, defaultdict, deque
from collections.abc import Awaitable, Callable, Iterator, Sequence
from contextlib import suppress
from http import HTTPStatus
from itertools import chain, cycle, islice
from time import monotonic
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal, cast

import aiohappyeyeballs
from aiohappyeyeballs import AddrInfoType, SocketFactoryType
from multidict import CIMultiDict

from . import hdrs, helpers
from .abc import AbstractResolver, ResolveResult
from .client_exceptions import (
    ClientConnectionError,
    ClientConnectorCertificateError,
    ClientConnectorDNSError,
    ClientConnectorError,
    ClientConnectorSSLError,
    ClientHttpProxyError,
    ClientProxyConnectionError,
    InvalidUrlClientError,
    ServerFingerprintMismatch,
    UnixClientConnectorError,
    cert_errors,
    ssl_errors,
)
from .client_proto import ResponseHandler
from .client_reqrep import (
    SSL_ALLOWED_TYPES,
    ClientRequest,
    ClientRequestBase,
    Fingerprint,
)
from .helpers import (
    _SENTINEL,
    HIGH_LEVEL_SCHEMA_SET,
    ceil_timeout,
    is_canonical_ipv4_address,
    is_ip_address,
    sentinel,
    set_exception,
    set_result,
)
from .log import client_logger
from .resolver import DefaultResolver

if sys.version_info >= (3, 12):
    from collections.abc import Buffer
else:
    Buffer = "bytes | bytearray | memoryview[int] | memoryview[bytes]"

try:
    import ssl

    SSLContext = ssl.SSLContext
except ImportError:  # pragma: no cover
    ssl = None  # type: ignore[assignment]
    SSLContext = object  # type: ignore[misc,assignment]

NEEDS_CLEANUP_CLOSED = (3, 13, 0) <= sys.version_info < (
    3,
    13,
    1,
) or sys.version_info < (3, 12, 8)


__all__ = (
    "BaseConnector",
    "TCPConnector",
    "UnixConnector",
    "NamedPipeConnector",
    "AddrInfoType",
    "SocketFactoryType",
)


if TYPE_CHECKING:
    from .client import ClientTimeout
    from .client_reqrep import ConnectionKey
    from .tracing import Trace


class Connection:

    __slots__ = (
        "_key",
        "_connector",
        "_loop",
        "_protocol",
        "_callbacks",
        "_source_traceback",
    )

    def __init__(
        self,
        connector: "BaseConnector",
        key: "ConnectionKey",
        protocol: ResponseHandler,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._key = key
        self._connector = connector
        self._loop = loop
        self._protocol: ResponseHandler | None = protocol
        self._callbacks: list[Callable[[], None]] = []
        self._source_traceback = (
            traceback.extract_stack(sys._getframe(1)) if loop.get_debug() else None
        )

    def __repr__(self) -> str:
        return f"Connection<{self._key}>"

    def __del__(self, _warnings: Any = warnings) -> None:
        if self._protocol is not None:
            _warnings.warn(
                f"Unclosed connection {self!r}", ResourceWarning, source=self
            )
            if self._loop.is_closed():
                return

            self._connector._release(self._key, self._protocol, should_close=True)

            context = {"client_connection": self, "message": "Unclosed connection"}
            if self._source_traceback is not None:
                context["source_traceback"] = self._source_traceback
            self._loop.call_exception_handler(context)

    def __bool__(self) -> Literal[True]:
        """Force subclasses to not be falsy, to make checks simpler."""
        return True



    def add_callback(self, callback: Callable[[], None]) -> None:
        if callback is not None:
            self._callbacks.append(callback)

    def _notify_release(self) -> None:
        callbacks, self._callbacks = self._callbacks[:], []

        for cb in callbacks:
            with suppress(Exception):
                cb()

    def close(self) -> None:
        self._notify_release()

        if self._protocol is not None:
            self._connector._release(self._key, self._protocol, should_close=True)
            self._protocol = None

    def release(self) -> None:
        self._notify_release()

        if self._protocol is not None:
            self._connector._release(self._key, self._protocol)
            self._protocol = None



class _ConnectTunnelConnection(Connection):

    def release(self) -> None:
        """Do nothing - don't pool or close the connection.

        These connections are an intermediate state during the CONNECT tunnel
        setup and will be cleaned up naturally after the TLS upgrade. If they
        were to be pooled, they would never be properly closed, causing
        session.close() to wait forever for their 'closed' future.
        """


class _TransportPlaceholder:

    __slots__ = ("closed", "transport")

    def __init__(self, closed_future: asyncio.Future[Exception | None]) -> None:
        """Initialize a placeholder for a transport."""
        self.closed = closed_future
        self.transport = None

    def close(self) -> None:
        """Close the placeholder."""

    def abort(self) -> None:
        """Abort the placeholder (does nothing)."""


class BaseConnector:

    _closed = True  # prevent AttributeError in __del__ if ctor was failed
    _source_traceback = None

    _cleanup_closed_period = 2.0

    allowed_protocol_schema_set = HIGH_LEVEL_SCHEMA_SET

    def __init__(
        self,
        *,
        keepalive_timeout: _SENTINEL | None | float = sentinel,
        force_close: bool = False,
        limit: int = 100,
        limit_per_host: int = 0,
        enable_cleanup_closed: bool = False,
        timeout_ceil_threshold: float = 5,
    ) -> None:
        if force_close:
            if keepalive_timeout is not None and keepalive_timeout is not sentinel:
                raise ValueError(
                    "keepalive_timeout cannot be set if force_close is True"
                )
        else:
            if keepalive_timeout is sentinel:
                keepalive_timeout = 15.0

        self._timeout_ceil_threshold = timeout_ceil_threshold

        loop = asyncio.get_running_loop()

        self._closed = False
        if loop.get_debug():
            self._source_traceback = traceback.extract_stack(sys._getframe(1))

        self._conns: defaultdict[
            ConnectionKey, deque[tuple[ResponseHandler, float]]
        ] = defaultdict(deque)
        self._limit = limit
        self._limit_per_host = limit_per_host
        self._acquired: set[ResponseHandler] = set()
        self._acquired_per_host: defaultdict[ConnectionKey, set[ResponseHandler]] = (
            defaultdict(set)
        )
        self._keepalive_timeout = cast(float, keepalive_timeout)
        self._force_close = force_close

        self._waiters: defaultdict[
            ConnectionKey, OrderedDict[asyncio.Future[None], None]
        ] = defaultdict(OrderedDict)

        self._loop = loop
        self._factory = functools.partial(ResponseHandler, loop=loop)

        self._cleanup_handle: asyncio.TimerHandle | None = None

        self._cleanup_closed_handle: asyncio.TimerHandle | None = None

        if enable_cleanup_closed and not NEEDS_CLEANUP_CLOSED:
            warnings.warn(
                "enable_cleanup_closed ignored because "
                "https://github.com/python/cpython/pull/118960 is fixed "
                f"in Python version {sys.version_info}",
                DeprecationWarning,
                stacklevel=2,
            )
            enable_cleanup_closed = False

        self._cleanup_closed_disabled = not enable_cleanup_closed
        self._cleanup_closed_transports: list[asyncio.Transport | None] = []

        self._placeholder_future: asyncio.Future[Exception | None] = (
            loop.create_future()
        )
        self._placeholder_future.set_result(None)
        self._cleanup_closed()

    def __del__(self, _warnings: Any = warnings) -> None:
        if self._closed:
            return
        if not self._conns:
            return

        conns = [repr(c) for c in self._conns.values()]

        self._close_immediately()

        _warnings.warn(f"Unclosed connector {self!r}", ResourceWarning, source=self)
        context = {
            "connector": self,
            "connections": conns,
            "message": "Unclosed connector",
        }
        if self._source_traceback is not None:
            context["source_traceback"] = self._source_traceback
        self._loop.call_exception_handler(context)

    async def __aenter__(self) -> "BaseConnector":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        exc_traceback: TracebackType | None = None,
    ) -> None:
        await self.close()

    @property
    def force_close(self) -> bool:
        """Ultimately close connection on releasing if True."""
        return self._force_close

    @property
    def limit(self) -> int:
        pass

    @property
    def limit_per_host(self) -> int:
        pass

    def _cleanup(self) -> None:
        pass

    def _cleanup_closed(self) -> None:
        """Double confirmation for transport close.

        Some broken ssl servers may leave socket open without proper close.
        """
        if self._cleanup_closed_handle:
            self._cleanup_closed_handle.cancel()

        for transport in self._cleanup_closed_transports:
            if transport is not None:
                transport.abort()

        self._cleanup_closed_transports = []

        if not self._cleanup_closed_disabled:
            self._cleanup_closed_handle = helpers.weakref_handle(
                self,
                "_cleanup_closed",
                self._cleanup_closed_period,
                self._loop,
                timeout_ceil_threshold=self._timeout_ceil_threshold,
            )

    async def close(self, *, abort_ssl: bool = False) -> None:
        """Close all opened transports.

        :param abort_ssl: If True, SSL connections will be aborted immediately
                         without performing the shutdown handshake. This provides
                         faster cleanup at the cost of less graceful disconnection.
        """
        waiters = self._close_immediately(abort_ssl=abort_ssl)
        if waiters:
            results = await asyncio.gather(*waiters, return_exceptions=True)
            for res in results:
                if isinstance(res, Exception):
                    err_msg = "Error while closing connector: " + repr(res)
                    client_logger.debug(err_msg)

    def _close_immediately(self, *, abort_ssl: bool = False) -> list[Awaitable[object]]:
        waiters: list[Awaitable[object]] = []

        if self._closed:
            return waiters

        self._closed = True

        try:
            if self._loop.is_closed():
                return waiters

            if self._cleanup_handle:
                self._cleanup_handle.cancel()

            if self._cleanup_closed_handle:
                self._cleanup_closed_handle.cancel()

            for data in self._conns.values():
                for proto, _ in data:
                    if (
                        abort_ssl
                        and proto.transport
                        and proto.transport.get_extra_info("sslcontext") is not None
                    ):
                        proto.abort()
                    else:
                        proto.close()
                    if closed := proto.closed:
                        waiters.append(closed)

            for proto in self._acquired:
                if (
                    abort_ssl
                    and proto.transport
                    and proto.transport.get_extra_info("sslcontext") is not None
                ):
                    proto.abort()
                else:
                    proto.close()
                if closed := proto.closed:
                    waiters.append(closed)

            for transport in self._cleanup_closed_transports:
                if transport is not None:
                    transport.abort()

            return waiters

        finally:
            self._conns.clear()
            self._acquired.clear()
            for keyed_waiters in self._waiters.values():
                for keyed_waiter in keyed_waiters:
                    keyed_waiter.cancel()
            self._waiters.clear()
            self._cleanup_handle = None
            self._cleanup_closed_transports.clear()
            self._cleanup_closed_handle = None

    @property
    def closed(self) -> bool:
        pass

    def _available_connections(self, key: "ConnectionKey") -> int:
        """
        Return number of available connections.

        The limit, limit_per_host and the connection key are taken into account.

        If it returns less than 1 means that there are no connections
        available.
        """
        total_remain = 1

        if self._limit and (total_remain := self._limit - len(self._acquired)) <= 0:
            return total_remain

        if host_remain := self._limit_per_host:
            if acquired := self._acquired_per_host.get(key):
                host_remain -= len(acquired)
            if total_remain > host_remain:
                return host_remain

        return total_remain

    def _update_proxy_auth_header_and_build_proxy_req(
        self, req: ClientRequest
    ) -> ClientRequestBase:
        pass

    async def connect(
        self, req: ClientRequest, traces: list["Trace"], timeout: "ClientTimeout"
    ) -> Connection:
        pass

    async def _wait_for_available_connection(
        self, key: "ConnectionKey", traces: list["Trace"]
    ) -> None:
        pass

    async def _get(
        self, key: "ConnectionKey", traces: list["Trace"]
    ) -> Connection | None:
        pass

    def _release_waiter(self) -> None:
        """
        Iterates over all waiters until one to be released is found.

        The one to be released is not finished and
        belongs to a host that has available connections.
        """
        if not self._waiters:
            return

        queues = list(self._waiters)
        random.shuffle(queues)

        for key in queues:
            if self._available_connections(key) < 1:
                continue

            waiters = self._waiters[key]
            while waiters:
                waiter, _ = waiters.popitem(last=False)
                if not waiter.done():
                    waiter.set_result(None)
                    return

    def _release_acquired(self, key: "ConnectionKey", proto: ResponseHandler) -> None:
        """Release acquired connection."""
        if self._closed:
            return

        self._acquired.discard(proto)
        if self._limit_per_host and (conns := self._acquired_per_host.get(key)):
            conns.discard(proto)
            if not conns:
                del self._acquired_per_host[key]
        self._release_waiter()

    def _release(
        self,
        key: "ConnectionKey",
        protocol: ResponseHandler,
        *,
        should_close: bool = False,
    ) -> None:
        if self._closed:
            return

        self._release_acquired(key, protocol)

        if self._force_close or should_close or protocol.should_close:
            transport = protocol.transport
            protocol.close()
            if key.is_ssl and not self._cleanup_closed_disabled:
                self._cleanup_closed_transports.append(transport)
            return

        self._conns[key].append((protocol, monotonic()))

        if self._cleanup_handle is None:
            self._cleanup_handle = helpers.weakref_handle(
                self,
                "_cleanup",
                self._keepalive_timeout,
                self._loop,
                timeout_ceil_threshold=self._timeout_ceil_threshold,
            )

    async def _create_connection(
        self, req: ClientRequest, traces: list["Trace"], timeout: "ClientTimeout"
    ) -> ResponseHandler:
        raise NotImplementedError()


class _DNSCacheTable:
    def __init__(self, ttl: float | None = None, max_size: int = 1000) -> None:
        self._addrs_rr: OrderedDict[
            tuple[str, int], tuple[Iterator[ResolveResult], int]
        ] = OrderedDict()
        self._timestamps: dict[tuple[str, int], float] = {}
        self._ttl = ttl
        self._max_size = max_size

    def __contains__(self, host: object) -> bool:
        return host in self._addrs_rr

    def add(self, key: tuple[str, int], addrs: list[ResolveResult]) -> None:
        if key in self._addrs_rr:
            self._addrs_rr.move_to_end(key)

        self._addrs_rr[key] = (cycle(addrs), len(addrs))

        if self._ttl is not None:
            self._timestamps[key] = monotonic()

        if len(self._addrs_rr) > self._max_size:
            oldest_key, _ = self._addrs_rr.popitem(last=False)
            self._timestamps.pop(oldest_key, None)

    def remove(self, key: tuple[str, int]) -> None:
        self._addrs_rr.pop(key, None)
        self._timestamps.pop(key, None)

    def clear(self) -> None:
        self._addrs_rr.clear()
        self._timestamps.clear()




def _make_ssl_context(verified: bool) -> SSLContext:
    """Create SSL context.

    This method is not async-friendly and should be called from a thread
    because it will load certificates from disk and do other blocking I/O.
    """
    if ssl is None:
        return None  # type: ignore[unreachable]
    if verified:
        sslcontext = ssl.create_default_context()
    else:
        sslcontext = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        sslcontext.options |= ssl.OP_NO_SSLv2
        sslcontext.options |= ssl.OP_NO_SSLv3
        sslcontext.check_hostname = False
        sslcontext.verify_mode = ssl.CERT_NONE
        sslcontext.options |= ssl.OP_NO_COMPRESSION
        sslcontext.set_default_verify_paths()
    sslcontext.set_alpn_protocols(("http/1.1",))
    return sslcontext


_SSL_CONTEXT_VERIFIED = _make_ssl_context(True)
_SSL_CONTEXT_UNVERIFIED = _make_ssl_context(False)


class TCPConnector(BaseConnector):

    allowed_protocol_schema_set = HIGH_LEVEL_SCHEMA_SET | frozenset({"tcp"})

    def __init__(
        self,
        *,
        use_dns_cache: bool = True,
        ttl_dns_cache: int | None = 10,
        dns_cache_max_size: int = 1000,
        family: socket.AddressFamily = socket.AddressFamily.AF_UNSPEC,
        ssl: bool | Fingerprint | SSLContext = True,
        local_addr: tuple[str, int] | None = None,
        resolver: AbstractResolver | None = None,
        keepalive_timeout: None | float | _SENTINEL = sentinel,
        force_close: bool = False,
        limit: int = 100,
        limit_per_host: int = 0,
        enable_cleanup_closed: bool = False,
        timeout_ceil_threshold: float = 5,
        happy_eyeballs_delay: float | None = 0.25,
        interleave: int | None = None,
        socket_factory: SocketFactoryType | None = None,
        ssl_shutdown_timeout: _SENTINEL | None | float = sentinel,
    ):
        super().__init__(
            keepalive_timeout=keepalive_timeout,
            force_close=force_close,
            limit=limit,
            limit_per_host=limit_per_host,
            enable_cleanup_closed=enable_cleanup_closed,
            timeout_ceil_threshold=timeout_ceil_threshold,
        )

        if not isinstance(ssl, SSL_ALLOWED_TYPES):
            raise TypeError(
                "ssl should be SSLContext, Fingerprint, or bool, "
                f"got {ssl!r} instead."
            )
        self._ssl = ssl

        self._resolver: AbstractResolver
        if resolver is None:
            self._resolver = DefaultResolver()
            self._resolver_owner = True
        else:
            self._resolver = resolver
            self._resolver_owner = False

        self._use_dns_cache = use_dns_cache
        self._cached_hosts = _DNSCacheTable(
            ttl=ttl_dns_cache, max_size=dns_cache_max_size
        )
        self._throttle_dns_futures: dict[tuple[str, int], set[asyncio.Future[None]]] = (
            {}
        )
        self._family = family
        self._local_addr_infos = aiohappyeyeballs.addr_to_addr_infos(local_addr)
        self._happy_eyeballs_delay = happy_eyeballs_delay
        self._interleave = interleave
        self._resolve_host_tasks: set[asyncio.Task[list[ResolveResult]]] = set()
        self._socket_factory = socket_factory
        self._ssl_shutdown_timeout: float | None

        if ssl_shutdown_timeout is sentinel:
            self._ssl_shutdown_timeout = 0
        else:
            warnings.warn(
                "The ssl_shutdown_timeout parameter is deprecated and will be removed in aiohttp 4.0",
                DeprecationWarning,
                stacklevel=2,
            )
            if (
                sys.version_info < (3, 11)
                and ssl_shutdown_timeout is not None
                and ssl_shutdown_timeout != 0
            ):
                warnings.warn(
                    f"ssl_shutdown_timeout={ssl_shutdown_timeout} is ignored on Python < 3.11; "
                    "only ssl_shutdown_timeout=0 is supported. The timeout will be ignored.",
                    RuntimeWarning,
                    stacklevel=2,
                )
            self._ssl_shutdown_timeout = ssl_shutdown_timeout

    async def close(self, *, abort_ssl: bool = False) -> None:
        """Close all opened transports.

        :param abort_ssl: If True, SSL connections will be aborted immediately
                         without performing the shutdown handshake. If False (default),
                         the behavior is determined by ssl_shutdown_timeout:
                         - If ssl_shutdown_timeout=0: connections are aborted
                         - If ssl_shutdown_timeout>0: graceful shutdown is performed
        """
        await super().close(abort_ssl=abort_ssl or self._ssl_shutdown_timeout == 0)
        if self._resolver_owner:
            await self._resolver.close()

    def _close_immediately(self, *, abort_ssl: bool = False) -> list[Awaitable[object]]:
        for fut in chain.from_iterable(self._throttle_dns_futures.values()):
            fut.cancel()

        waiters = super()._close_immediately(abort_ssl=abort_ssl)

        for t in self._resolve_host_tasks:
            t.cancel()
            waiters.append(t)

        return waiters

    @property
    def family(self) -> int:
        pass

    @property
    def use_dns_cache(self) -> bool:
        pass

    def clear_dns_cache(self, host: str | None = None, port: int | None = None) -> None:
        pass

    async def _resolve_host(
        self, host: str, port: int, traces: Sequence["Trace"] | None = None
    ) -> list[ResolveResult]:
        pass

    async def _resolve_host_with_throttle(
        self,
        key: tuple[str, int],
        host: str,
        port: int,
        futures: set[asyncio.Future[None]],
        traces: Sequence["Trace"] | None,
    ) -> list[ResolveResult]:
        pass

    async def _create_connection(
        self, req: ClientRequest, traces: list["Trace"], timeout: "ClientTimeout"
    ) -> ResponseHandler:
        pass

    def _get_ssl_context(self, req: ClientRequestBase) -> SSLContext | None:
        pass



    def _warn_about_tls_in_tls(
        self,
        underlying_transport: asyncio.Transport,
        req: ClientRequest,
    ) -> None:
        pass

    async def _start_tls_connection(
        self,
        underlying_transport: asyncio.Transport,
        req: ClientRequest,
        timeout: "ClientTimeout",
        client_error: type[Exception] = ClientConnectorError,
    ) -> tuple[asyncio.BaseTransport, ResponseHandler]:
        pass

    def _convert_hosts_to_addr_infos(
        self, hosts: list[ResolveResult]
    ) -> list[AddrInfoType]:
        pass




class UnixConnector(BaseConnector):

    allowed_protocol_schema_set = HIGH_LEVEL_SCHEMA_SET | frozenset({"unix"})

    def __init__(
        self,
        path: str,
        force_close: bool = False,
        keepalive_timeout: _SENTINEL | float | None = sentinel,
        limit: int = 100,
        limit_per_host: int = 0,
    ) -> None:
        super().__init__(
            force_close=force_close,
            keepalive_timeout=keepalive_timeout,
            limit=limit,
            limit_per_host=limit_per_host,
        )
        self._path = path

    @property
    def path(self) -> str:
        pass



class NamedPipeConnector(BaseConnector):

    allowed_protocol_schema_set = HIGH_LEVEL_SCHEMA_SET | frozenset({"npipe"})

    def __init__(
        self,
        path: str,
        force_close: bool = False,
        keepalive_timeout: _SENTINEL | float | None = sentinel,
        limit: int = 100,
        limit_per_host: int = 0,
    ) -> None:
        super().__init__(
            force_close=force_close,
            keepalive_timeout=keepalive_timeout,
            limit=limit,
            limit_per_host=limit_per_host,
        )
        if not isinstance(
            self._loop,
            asyncio.ProactorEventLoop,  # type: ignore[attr-defined]
        ):
            raise RuntimeError(
                "Named Pipes only available in proactor loop under windows"
            )
        self._path = path

    @property
    def path(self) -> str:
        pass

