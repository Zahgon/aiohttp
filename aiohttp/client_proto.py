import asyncio
from contextlib import suppress
from typing import Callable, Protocol

from ._websocket.reader import WebSocketDataQueue
from .base_protocol import BaseProtocol
from .client_exceptions import (
    ClientConnectionError,
    ClientOSError,
    ClientPayloadError,
    ServerDisconnectedError,
    SocketTimeoutError,
)
from .helpers import (
    _EXC_SENTINEL,
    DEFAULT_CHUNK_SIZE,
    EMPTY_BODY_STATUS_CODES,
    BaseTimerContext,
    ErrorableProtocol,
    set_exception,
    set_result,
)
from .http import HttpResponseParser, RawResponseMessage, WebSocketReader
from .http_exceptions import HttpProcessingError
from .streams import EMPTY_PAYLOAD, DataQueue, StreamReader


class _Payload(ErrorableProtocol, Protocol):
    def is_eof(self) -> bool: ...


class ResponseHandler(BaseProtocol, DataQueue[tuple[RawResponseMessage, StreamReader]]):

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        BaseProtocol.__init__(self, loop=loop, parser=None)
        DataQueue.__init__(self, loop)

        self._should_close = False

        self._payload: _Payload | None = None
        self._skip_payload = False
        self._payload_parser: WebSocketReader | None = None
        self._data_received_cb: Callable[[], None] | None = None

        self._timer = None
        self._tail = b""

        self._read_timeout: float | None = None
        self._read_timeout_handle: asyncio.TimerHandle | None = None

        self._timeout_ceil_threshold: float | None = 5

        self._closed: None | asyncio.Future[None] = None
        self._connection_lost_called = False

    @property
    def closed(self) -> None | asyncio.Future[None]:
        pass



    def force_close(self) -> None:
        self._should_close = True

    def close(self) -> None:
        self._exception = None  # Break cyclic references
        transport = self.transport
        if transport is not None:
            transport.close()
            self.transport = None
            self._payload = None
            self._drop_timeout()

    def abort(self) -> None:
        self._exception = None  # Break cyclic references
        transport = self.transport
        if transport is not None:
            transport.abort()
            self.transport = None
            self._payload = None
            self._drop_timeout()




    def pause_reading(self) -> None:
        super().pause_reading()
        self._drop_timeout()

    def resume_reading(self, resume_parser: bool = True) -> None:
        was_paused = self._reading_paused
        super().resume_reading(resume_parser)
        if was_paused:
            self._reschedule_timeout()

    def set_exception(
        self,
        exc: type[BaseException] | BaseException,
        exc_cause: BaseException = _EXC_SENTINEL,
    ) -> None:
        self._should_close = True
        self._drop_timeout()
        super().set_exception(exc, exc_cause)

    def set_parser(
        self,
        parser: WebSocketReader,
        payload: WebSocketDataQueue,
        data_received_cb: Callable[[], None] | None = None,
    ) -> None:
        self._payload = payload
        self._payload_parser = parser
        self._data_received_cb = data_received_cb

        self._drop_timeout()

        if self._tail:
            data, self._tail = self._tail, b""
            self.data_received(data)


    def _drop_timeout(self) -> None:
        if self._read_timeout_handle is not None:
            self._read_timeout_handle.cancel()
            self._read_timeout_handle = None

    def _reschedule_timeout(self) -> None:
        timeout = self._read_timeout
        if self._read_timeout_handle is not None:
            self._read_timeout_handle.cancel()

        if timeout:
            self._read_timeout_handle = self._loop.call_later(
                timeout, self._on_read_timeout
            )
        else:
            self._read_timeout_handle = None

    def start_timeout(self) -> None:
        self._reschedule_timeout()




    def data_received(self, data: bytes) -> None:
        if data:
            self._reschedule_timeout()

        if self._payload_parser is not None:
            if self._data_received_cb is not None:
                self._data_received_cb()
            eof, tail = self._payload_parser.feed_data(data)
            if eof:
                self._payload = None
                self._payload_parser = None

                if tail:
                    self.data_received(tail)
            return

        if self._upgraded or self._parser is None:
            self._tail += data
            return

        try:
            messages, upgraded, tail = self._parser.feed_data(data)
        except BaseException as underlying_exc:
            if self.transport is not None:
                self.transport.close()
            if not isinstance(underlying_exc, Exception):
                raise
            if isinstance(underlying_exc, HttpProcessingError):
                exc = HttpProcessingError(
                    code=underlying_exc.code,
                    message=underlying_exc.message,
                    headers=underlying_exc.headers,
                )
            else:
                exc = HttpProcessingError()
            self.set_exception(exc, underlying_exc)
            return

        self._upgraded = upgraded

        payload: StreamReader | None = None
        for message, payload in messages:
            if message.should_close:
                self._should_close = True

            self._payload = payload

            if self._skip_payload or message.code in EMPTY_BODY_STATUS_CODES:
                self.feed_data((message, EMPTY_PAYLOAD))
            else:
                self.feed_data((message, payload))

        if payload is not None:
            if payload is not EMPTY_PAYLOAD:
                payload.on_eof(self._drop_timeout)
            else:
                self._drop_timeout()

        if upgraded and tail:
            self.data_received(tail)
