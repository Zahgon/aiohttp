import asyncio
import socket
import sys
import weakref
from typing import Any, Optional

from .abc import AbstractResolver, ResolveResult

__all__ = ("ThreadedResolver", "AsyncResolver", "DefaultResolver")


try:
    import aiodns

    aiodns_default = hasattr(aiodns.DNSResolver, "getaddrinfo")
except ImportError:
    aiodns = None  # type: ignore[assignment]
    aiodns_default = False


_NUMERIC_SOCKET_FLAGS = socket.AI_NUMERICHOST | socket.AI_NUMERICSERV
_NAME_SOCKET_FLAGS = socket.NI_NUMERICHOST | socket.NI_NUMERICSERV
_AI_ADDRCONFIG = socket.AI_ADDRCONFIG
if hasattr(socket, "AI_MASK"):
    _AI_ADDRCONFIG &= socket.AI_MASK
_IS_WINDOWS = sys.platform == "win32"


def _is_windows_localhost(host: str) -> bool:
    return _IS_WINDOWS and host.rstrip(".").casefold() == "localhost"


class ThreadedResolver(AbstractResolver):

    def __init__(self) -> None:
        self._loop = asyncio.get_running_loop()

    async def resolve(
        self, host: str, port: int = 0, family: socket.AddressFamily = socket.AF_INET
    ) -> list[ResolveResult]:
        try:
            infos = await self._loop.getaddrinfo(
                host,
                port,
                type=socket.SOCK_STREAM,
                family=family,
                flags=_AI_ADDRCONFIG,
            )
        except socket.gaierror:
            if not _is_windows_localhost(host):
                raise
            infos = await self._loop.getaddrinfo(
                host,
                port,
                type=socket.SOCK_STREAM,
                family=family,
                flags=0,
            )

        hosts: list[ResolveResult] = []
        for family, _, proto, _, address in infos:
            if family == socket.AF_INET6:
                if len(address) < 3:
                    continue
                if address[3]:
                    resolved_host, _port = await self._loop.getnameinfo(
                        address, _NAME_SOCKET_FLAGS
                    )
                    port = int(_port)
                else:
                    resolved_host, port = address[:2]
            else:  # IPv4
                assert family == socket.AF_INET
                resolved_host, port = address  # type: ignore[misc]
            hosts.append(
                ResolveResult(
                    hostname=host,
                    host=resolved_host,
                    port=port,
                    family=family,
                    proto=proto,
                    flags=_NUMERIC_SOCKET_FLAGS,
                )
            )

        return hosts

    async def close(self) -> None:
        pass


class AsyncResolver(AbstractResolver):

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if aiodns is None:
            raise RuntimeError("Resolver requires aiodns library")

        self._loop = asyncio.get_running_loop()
        self._manager: _DNSResolverManager | None = None
        if args or kwargs:
            self._resolver = aiodns.DNSResolver(*args, **kwargs)
            return
        self._manager = _DNSResolverManager()
        self._resolver = self._manager.get_resolver(self, self._loop)

    async def resolve(
        self, host: str, port: int = 0, family: socket.AddressFamily = socket.AF_INET
    ) -> list[ResolveResult]:
        try:
            try:
                resp = await self._resolver.getaddrinfo(
                    host,
                    port=port,
                    type=socket.SOCK_STREAM,
                    family=family,
                    flags=_AI_ADDRCONFIG,
                )
            except aiodns.error.DNSError:
                if not _is_windows_localhost(host):
                    raise
                resp = await self._resolver.getaddrinfo(
                    host,
                    port=port,
                    type=socket.SOCK_STREAM,
                    family=family,
                    flags=0,
                )
        except aiodns.error.DNSError as exc:
            msg = exc.args[1] if len(exc.args) >= 1 else "DNS lookup failed"
            raise OSError(None, msg) from exc
        hosts: list[ResolveResult] = []
        for node in resp.nodes:
            address: tuple[bytes, int] | tuple[bytes, int, int, int] = node.addr
            if node.family == socket.AF_INET6:
                if len(address) > 3 and address[3]:
                    result = await self._resolver.getnameinfo(
                        (address[0].decode("ascii"), *address[1:]),
                        _NAME_SOCKET_FLAGS,
                    )
                    resolved_host = result.node
                else:
                    resolved_host = address[0].decode("ascii")
                    port = address[1]
            else:  # IPv4
                assert node.family == socket.AF_INET
                resolved_host = address[0].decode("ascii")
                port = address[1]
            hosts.append(
                ResolveResult(
                    hostname=host,
                    host=resolved_host,
                    port=port,
                    family=node.family,
                    proto=0,
                    flags=_NUMERIC_SOCKET_FLAGS,
                )
            )

        if not hosts:
            raise OSError(None, "DNS lookup failed")

        return hosts

    async def close(self) -> None:
        if self._manager:
            self._manager.release_resolver(self, self._loop)
            self._manager = None  # Clear reference to manager
            self._resolver = None  # type: ignore[assignment] # Clear reference to resolver
            return
        if self._resolver is not None:
            self._resolver.cancel()
        self._resolver = None  # type: ignore[assignment] # Clear reference


class _DNSResolverManager:

    _instance: Optional["_DNSResolverManager"] = None

    def __new__(cls) -> "_DNSResolverManager":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init()
        return cls._instance


    def get_resolver(
        self, client: "AsyncResolver", loop: asyncio.AbstractEventLoop
    ) -> "aiodns.DNSResolver":
        """Get or create the shared aiodns.DNSResolver instance for a specific event loop.

        Args:
            client: The AsyncResolver instance requesting the resolver.
                   This is required to track resolver usage.
            loop: The event loop to use for the resolver.
        """
        if loop not in self._loop_data:
            resolver = aiodns.DNSResolver(loop=loop)
            client_set: weakref.WeakSet[AsyncResolver] = weakref.WeakSet()
            self._loop_data[loop] = (resolver, client_set)
        else:
            resolver, client_set = self._loop_data[loop]

        client_set.add(client)
        return resolver

    def release_resolver(
        self, client: "AsyncResolver", loop: asyncio.AbstractEventLoop
    ) -> None:
        """Release the resolver for an AsyncResolver client when it's closed.

        Args:
            client: The AsyncResolver instance to release.
            loop: The event loop the resolver was using.
        """
        current_loop_data = self._loop_data.get(loop)
        if current_loop_data is None:
            return
        resolver, client_set = current_loop_data
        client_set.discard(client)
        if not client_set:
            if resolver is not None:
                resolver.cancel()
            del self._loop_data[loop]


_DefaultType = type[AsyncResolver | ThreadedResolver]
DefaultResolver: _DefaultType = AsyncResolver if aiodns_default else ThreadedResolver
