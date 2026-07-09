
import asyncio
import sys
from collections.abc import Callable
from types import TracebackType
from typing import Any, Final, Generic, Literal, overload

from ._websocket.reader import WebSocketDataQueue
from .client_exceptions import ClientError, ServerTimeoutError, WSMessageTypeError
from .client_reqrep import ClientResponse
from .helpers import calculate_timeout_when, frozen_dataclass_decorator, set_result
from .http import (
    WS_CLOSED_MESSAGE,
    WS_CLOSING_MESSAGE,
    WebSocketError,
    WSCloseCode,
    WSMessageDecodeText,
    WSMessageNoDecodeText,
    WSMsgType,
)
from .http_websocket import _INTERNAL_RECEIVE_TYPES, WebSocketWriter, WSMessageError
from .streams import EofStream
from .typedefs import (
    DEFAULT_JSON_DECODER,
    DEFAULT_JSON_ENCODER,
    JSONBytesEncoder,
    JSONDecoder,
    JSONEncoder,
)

if sys.version_info >= (3, 13):
    from typing import TypeVar
else:
    from typing_extensions import TypeVar

if sys.version_info >= (3, 11):
    import asyncio as async_timeout
    from typing import Self
else:
    import async_timeout
    from typing_extensions import Self

_DecodeText = TypeVar("_DecodeText", bound=bool, covariant=True, default=Literal[True])


@frozen_dataclass_decorator
class ClientWSTimeout:
    ws_receive: float | None = None
    ws_close: float | None = None


DEFAULT_WS_CLIENT_TIMEOUT: Final[ClientWSTimeout] = ClientWSTimeout(
    ws_receive=None, ws_close=10.0
)


class ClientWebSocketResponse(Generic[_DecodeText]):
    def __init__(
        self,
        reader: WebSocketDataQueue,
        writer: WebSocketWriter,
        protocol: str | None,
        response: ClientResponse,
        timeout: ClientWSTimeout,
        autoclose: bool,
        autoping: bool,
        loop: asyncio.AbstractEventLoop,
        *,
        heartbeat: float | None = None,
        compress: int = 0,
        client_notakeover: bool = False,
    ) -> None:
        self._response = response
        self._conn = response.connection

        self._writer = writer
        self._reader = reader
        self._protocol = protocol
        self._closed = False
        self._closing = False
        self._close_code: int | None = None
        self._timeout = timeout
        self._autoclose = autoclose
        self._autoping = autoping
        self._heartbeat = heartbeat
        self._heartbeat_cb: asyncio.TimerHandle | None = None
        self._heartbeat_when: float = 0.0
        if heartbeat is not None:
            self._pong_heartbeat = heartbeat / 2.0
        self._pong_response_cb: asyncio.TimerHandle | None = None
        self._loop = loop
        self._waiting: bool = False
        self._close_wait: asyncio.Future[None] | None = None
        self._exception: BaseException | None = None
        self._compress = compress
        self._client_notakeover = client_notakeover
        self._ping_task: asyncio.Task[None] | None = None
        self._need_heartbeat_reset = False
        self._heartbeat_reset_handle: asyncio.Handle | None = None

        self._reset_heartbeat()

    def _cancel_heartbeat(self) -> None:
        self._cancel_pong_response_cb()
        if self._heartbeat_reset_handle is not None:
            self._heartbeat_reset_handle.cancel()
            self._heartbeat_reset_handle = None
        self._need_heartbeat_reset = False
        if self._heartbeat_cb is not None:
            self._heartbeat_cb.cancel()
            self._heartbeat_cb = None
        if self._ping_task is not None:
            self._ping_task.cancel()
            self._ping_task = None

    def _cancel_pong_response_cb(self) -> None:
        if self._pong_response_cb is not None:
            self._pong_response_cb.cancel()
            self._pong_response_cb = None



    def _reset_heartbeat(self) -> None:
        if self._heartbeat is None:
            return
        self._cancel_pong_response_cb()
        loop = self._loop
        assert loop is not None
        conn = self._conn
        timeout_ceil_threshold = (
            conn._connector._timeout_ceil_threshold if conn is not None else 5
        )
        now = loop.time()
        when = calculate_timeout_when(now, self._heartbeat, timeout_ceil_threshold)
        self._heartbeat_when = when
        if self._heartbeat_cb is None:
            self._heartbeat_cb = loop.call_at(when, self._send_heartbeat)


    def _ping_task_done(self, task: "asyncio.Task[None]") -> None:
        pass


    def _handle_ping_pong_exception(self, exc: BaseException) -> None:
        pass

    def _set_closed(self) -> None:
        """Set the connection to closed.

        Cancel any heartbeat timers and set the closed flag.
        """
        self._closed = True
        self._cancel_heartbeat()

    def _set_closing(self) -> None:
        """Set the connection to closing.

        Cancel any heartbeat timers and set the closing flag.
        """
        self._closing = True
        self._cancel_heartbeat()




    @property
    def compress(self) -> int:
        return self._compress


    def get_extra_info(self, name: str, default: Any = None) -> Any:
        """extra info from connection transport"""
        conn = self._response.connection
        if conn is None:
            return default
        transport = conn.transport
        if transport is None:
            return default
        return transport.get_extra_info(name, default)

    def exception(self) -> BaseException | None:
        return self._exception

    async def ping(self, message: bytes = b"") -> None:
        await self._writer.send_frame(message, WSMsgType.PING)

    async def pong(self, message: bytes = b"") -> None:
        await self._writer.send_frame(message, WSMsgType.PONG)

    async def send_frame(
        self, message: bytes, opcode: WSMsgType, compress: int | None = None
    ) -> None:
        """Send a frame over the websocket."""
        await self._writer.send_frame(message, opcode, compress)

    async def send_str(self, data: str, compress: int | None = None) -> None:
        if not isinstance(data, str):
            raise TypeError("data argument must be str (%r)" % type(data))
        await self._writer.send_frame(
            data.encode("utf-8"), WSMsgType.TEXT, compress=compress
        )

    async def send_bytes(self, data: bytes, compress: int | None = None) -> None:
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("data argument must be byte-ish (%r)" % type(data))
        await self._writer.send_frame(data, WSMsgType.BINARY, compress=compress)

    async def send_json(
        self,
        data: Any,
        compress: int | None = None,
        *,
        dumps: JSONEncoder = DEFAULT_JSON_ENCODER,
    ) -> None:
        await self.send_str(dumps(data), compress=compress)

    async def send_json_bytes(
        self,
        data: Any,
        compress: int | None = None,
        *,
        dumps: JSONBytesEncoder,
    ) -> None:
        """Send JSON data using a bytes-returning encoder as a binary frame.

        Use this when your JSON encoder (like orjson) returns bytes
        instead of str, avoiding the encode/decode overhead.
        """
        await self.send_bytes(dumps(data), compress=compress)

    async def close(self, *, code: int = WSCloseCode.OK, message: bytes = b"") -> bool:
        if self._waiting and not self._closing:
            assert self._loop is not None
            self._close_wait = self._loop.create_future()
            self._set_closing()
            self._reader.feed_data(WS_CLOSING_MESSAGE)
            await self._close_wait

        if self._closed:
            return False

        self._set_closed()
        try:
            await self._writer.close(code, message)
        except asyncio.CancelledError:
            self._close_code = WSCloseCode.ABNORMAL_CLOSURE
            self._response.close()
            raise
        except Exception as exc:
            self._close_code = WSCloseCode.ABNORMAL_CLOSURE
            self._exception = exc
            self._response.close()
            return True

        if self._close_code:
            self._response.close()
            return True

        while True:
            try:
                async with async_timeout.timeout(self._timeout.ws_close):
                    msg = await self._reader.read()
            except asyncio.CancelledError:
                self._close_code = WSCloseCode.ABNORMAL_CLOSURE
                self._response.close()
                raise
            except Exception as exc:
                self._close_code = WSCloseCode.ABNORMAL_CLOSURE
                self._exception = exc
                self._response.close()
                return True

            if msg.type is WSMsgType.CLOSE:
                self._close_code = msg.data
                self._response.close()
                return True

    @overload
    async def receive(
        self: "ClientWebSocketResponse[Literal[True]]", timeout: float | None = None
    ) -> WSMessageDecodeText: ...

    @overload
    async def receive(
        self: "ClientWebSocketResponse[Literal[False]]", timeout: float | None = None
    ) -> WSMessageNoDecodeText: ...

    @overload
    async def receive(
        self: "ClientWebSocketResponse[_DecodeText]", timeout: float | None = None
    ) -> WSMessageDecodeText | WSMessageNoDecodeText: ...

    async def receive(
        self, timeout: float | None = None
    ) -> WSMessageDecodeText | WSMessageNoDecodeText:
        receive_timeout = timeout or self._timeout.ws_receive

        while True:
            if self._waiting:
                raise RuntimeError("Concurrent call to receive() is not allowed")

            if self._closed:
                return WS_CLOSED_MESSAGE
            elif self._closing:
                await self.close()
                return WS_CLOSED_MESSAGE

            try:
                self._waiting = True
                try:
                    if receive_timeout:
                        async with async_timeout.timeout(receive_timeout):
                            msg = await self._reader.read()
                    else:
                        msg = await self._reader.read()
                finally:
                    self._waiting = False
                    if self._close_wait:
                        set_result(self._close_wait, None)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                self._close_code = WSCloseCode.ABNORMAL_CLOSURE
                raise
            except EofStream:
                self._close_code = WSCloseCode.OK
                await self.close()
                return WS_CLOSED_MESSAGE
            except ClientError:
                self._set_closed()
                self._close_code = WSCloseCode.ABNORMAL_CLOSURE
                return WS_CLOSED_MESSAGE
            except WebSocketError as exc:
                self._close_code = exc.code
                await self.close(code=exc.code)
                return WSMessageError(data=exc)
            except Exception as exc:
                self._exception = exc
                self._set_closing()
                self._close_code = WSCloseCode.ABNORMAL_CLOSURE
                await self.close()
                return WSMessageError(data=exc)

            if msg.type not in _INTERNAL_RECEIVE_TYPES:
                return msg

            if msg.type is WSMsgType.CLOSE:
                self._set_closing()
                self._close_code = msg.data
                if not self._closed and self._autoclose:  # type: ignore[redundant-expr]
                    await self.close()
            elif msg.type is WSMsgType.CLOSING:
                self._set_closing()
            elif msg.type is WSMsgType.PING and self._autoping:
                await self.pong(msg.data)
                continue
            elif msg.type is WSMsgType.PONG and self._autoping:
                continue

            return msg

    @overload
    async def receive_str(
        self: "ClientWebSocketResponse[Literal[True]]", *, timeout: float | None = None
    ) -> str: ...

    @overload
    async def receive_str(
        self: "ClientWebSocketResponse[Literal[False]]", *, timeout: float | None = None
    ) -> bytes: ...

    @overload
    async def receive_str(
        self: "ClientWebSocketResponse[_DecodeText]", *, timeout: float | None = None
    ) -> str | bytes: ...

    async def receive_str(self, *, timeout: float | None = None) -> str | bytes:
        """Receive TEXT message.

        Returns str when decode_text=True (default), bytes when decode_text=False.
        """
        msg = await self.receive(timeout)
        if msg.type is not WSMsgType.TEXT:
            raise WSMessageTypeError(
                f"Received message {msg.type}:{msg.data!r} is not WSMsgType.TEXT"
            )
        return msg.data

    async def receive_bytes(self, *, timeout: float | None = None) -> bytes:
        msg = await self.receive(timeout)
        if msg.type is not WSMsgType.BINARY:
            raise WSMessageTypeError(
                f"Received message {msg.type}:{msg.data!r} is not WSMsgType.BINARY"
            )
        return msg.data

    @overload
    async def receive_json(
        self: "ClientWebSocketResponse[Literal[True]]",
        *,
        loads: JSONDecoder = ...,
        timeout: float | None = None,
    ) -> Any: ...

    @overload
    async def receive_json(
        self: "ClientWebSocketResponse[Literal[False]]",
        *,
        loads: Callable[[bytes], Any] = ...,
        timeout: float | None = None,
    ) -> Any: ...

    @overload
    async def receive_json(
        self: "ClientWebSocketResponse[_DecodeText]",
        *,
        loads: JSONDecoder | Callable[[bytes], Any] = ...,
        timeout: float | None = None,
    ) -> Any: ...

    async def receive_json(
        self,
        *,
        loads: JSONDecoder | Callable[[bytes], Any] = DEFAULT_JSON_DECODER,
        timeout: float | None = None,
    ) -> Any:
        data = await self.receive_str(timeout=timeout)
        return loads(data)  # type: ignore[arg-type]

    def __aiter__(self) -> Self:
        return self

    @overload
    async def __anext__(
        self: "ClientWebSocketResponse[Literal[True]]",
    ) -> WSMessageDecodeText: ...

    @overload
    async def __anext__(
        self: "ClientWebSocketResponse[Literal[False]]",
    ) -> WSMessageNoDecodeText: ...

    @overload
    async def __anext__(
        self: "ClientWebSocketResponse[_DecodeText]",
    ) -> WSMessageDecodeText | WSMessageNoDecodeText: ...

    async def __anext__(self) -> WSMessageDecodeText | WSMessageNoDecodeText:
        msg = await self.receive()
        if msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED):
            raise StopAsyncIteration
        return msg

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()
