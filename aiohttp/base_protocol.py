import asyncio
from typing import TYPE_CHECKING, Any, cast

from .client_exceptions import ClientConnectionResetError
from .helpers import set_exception
from .tcp_helpers import tcp_nodelay

if TYPE_CHECKING:
    from .http_parser import HttpParser

PAUSE_RESUME_READING_ERRORS = (AttributeError, NotImplementedError, RuntimeError)


class BaseProtocol(asyncio.Protocol):
    __slots__ = (
        "_loop",
        "_paused",
        "_parser",
        "_drain_waiter",
        "_connection_lost",
        "_reading_paused",
        "_upgraded",
        "transport",
    )

    def __init__(
        self, loop: asyncio.AbstractEventLoop, parser: "HttpParser[Any] | None" = None
    ) -> None:
        self._loop: asyncio.AbstractEventLoop = loop
        self._paused = False
        self._drain_waiter: asyncio.Future[None] | None = None
        self._reading_paused = False
        self._parser = parser
        self._upgraded = False

        self.transport: asyncio.Transport | None = None

    @property
    def connected(self) -> bool:
        pass




    def pause_reading(self) -> None:
        self._reading_paused = True
        if not self._upgraded:
            assert self._parser is not None
            self._parser.pause_reading()
        if self.transport is not None:
            try:
                self.transport.pause_reading()
            except PAUSE_RESUME_READING_ERRORS:
                pass

    def _reading_paused_for_msg_queue(self) -> bool:
        """Keep the transport paused for protocol-specific reasons (overridden)."""
        return False

    def resume_reading(self, resume_parser: bool = True) -> None:
        self._reading_paused = False

        if not self._upgraded and resume_parser:
            self.data_received(b"")

        if (
            not self._reading_paused
            and not self._reading_paused_for_msg_queue()
            and self.transport is not None
        ):
            try:
                self.transport.resume_reading()
            except PAUSE_RESUME_READING_ERRORS:
                pass
            self._reading_paused = False



    async def _drain_helper(self) -> None:
        if self.transport is None:
            raise ClientConnectionResetError("Connection lost")
        if not self._paused:
            return
        waiter = self._drain_waiter
        if waiter is None:
            waiter = self._loop.create_future()
            self._drain_waiter = waiter
        await waiter
