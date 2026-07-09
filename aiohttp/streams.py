import asyncio
import collections
import sys
import warnings
from collections.abc import Awaitable, Callable, Coroutine
from typing import Final, Generic, TypeVar

from .base_protocol import BaseProtocol
from .helpers import (
    _EXC_SENTINEL,
    DEFAULT_CHUNK_SIZE,
    BaseTimerContext,
    TimerNoop,
    set_exception,
    set_result,
)
from .http_exceptions import LineTooLong
from .log import internal_logger

__all__ = (
    "EMPTY_PAYLOAD",
    "EofStream",
    "StreamReader",
    "DataQueue",
)

_T = TypeVar("_T")


class EofStream(Exception):
    pass


class AsyncStreamIterator(Generic[_T]):

    __slots__ = ("read_func",)

    def __init__(self, read_func: Callable[[], Awaitable[_T]]) -> None:
        self.read_func = read_func

    def __aiter__(self) -> "AsyncStreamIterator[_T]":
        return self

    async def __anext__(self) -> _T:
        try:
            rv = await self.read_func()
        except EofStream:
            raise StopAsyncIteration
        if rv == b"":
            raise StopAsyncIteration
        return rv


class ChunkTupleAsyncStreamIterator:

    __slots__ = ("_stream",)

    def __init__(self, stream: "StreamReader") -> None:
        self._stream = stream

    def __aiter__(self) -> "ChunkTupleAsyncStreamIterator":
        return self

    async def __anext__(self) -> tuple[bytes, bool]:
        rv = await self._stream.readchunk()
        if rv == (b"", False):
            raise StopAsyncIteration
        return rv


class StreamReader:

    __slots__ = (
        "_protocol",
        "_low_water",
        "_high_water",
        "_low_water_chunks",
        "_high_water_chunks",
        "_loop",
        "_size",
        "_cursor",
        "_http_chunk_splits",
        "_buffer",
        "_buffer_offset",
        "_eof",
        "_waiter",
        "_eof_waiter",
        "_exception",
        "_timer",
        "_eof_callbacks",
        "_eof_counter",
        "_on_chunk_received",
        "total_bytes",
        "total_compressed_bytes",
    )

    def __init__(
        self,
        protocol: BaseProtocol,
        limit: int,
        *,
        timer: BaseTimerContext | None = None,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._protocol = protocol
        self._low_water = limit
        self._high_water = limit * 2
        self._high_water_chunks = max(4, limit // 16)
        self._low_water_chunks = self._high_water_chunks // 2
        self._loop = loop
        self._size = 0
        self._cursor = 0
        self._http_chunk_splits: collections.deque[int] | None = None
        self._buffer: collections.deque[bytes] = collections.deque()
        self._buffer_offset = 0
        self._eof = False
        self._waiter: asyncio.Future[None] | None = None
        self._eof_waiter: asyncio.Future[None] | None = None
        self._exception: type[BaseException] | BaseException | None = None
        self._timer = TimerNoop() if timer is None else timer
        self._eof_callbacks: list[Callable[[], None]] = []
        self._eof_counter = 0
        self._on_chunk_received: (
            Callable[[bytes], Coroutine[None, None, None]] | None
        ) = None
        self.total_bytes = 0
        self.total_compressed_bytes: int | None = None

    def __repr__(self) -> str:
        info = [self.__class__.__name__]
        if self._size:
            info.append("%d bytes" % self._size)
        if self._eof:
            info.append("eof")
        if self._low_water != DEFAULT_CHUNK_SIZE:
            info.append("low=%d high=%d" % (self._low_water, self._high_water))
        if self._waiter:
            info.append("w=%r" % self._waiter)
        if self._exception:
            info.append("e=%r" % self._exception)
        return "<%s>" % " ".join(info)

    def __aiter__(self) -> AsyncStreamIterator[bytes]:
        return AsyncStreamIterator(self.readline)

    def iter_chunked(self, n: int) -> AsyncStreamIterator[bytes]:
        """Returns an asynchronous iterator that yields chunks of size n."""
        self.set_read_chunk_size(n)
        return AsyncStreamIterator(lambda: self.read(n))

    def iter_any(self) -> AsyncStreamIterator[bytes]:
        """Yield all available data as soon as it is received."""
        return AsyncStreamIterator(self.readany)

    def iter_chunks(self) -> ChunkTupleAsyncStreamIterator:
        """Yield chunks of data as they are received by the server.

        The yielded objects are tuples
        of (bytes, bool) as returned by the StreamReader.readchunk method.
        """
        return ChunkTupleAsyncStreamIterator(self)

    def get_read_buffer_limits(self) -> tuple[int, int]:
        return (self._low_water, self._high_water)

    def set_read_chunk_size(self, n: int) -> None:
        """Raise buffer limits to match the consumer's chunk size."""
        if n > self._low_water:
            self._low_water = n
            self._high_water = n * 2

    def exception(self) -> type[BaseException] | BaseException | None:
        return self._exception

    def set_exception(
        self,
        exc: type[BaseException] | BaseException,
        exc_cause: BaseException = _EXC_SENTINEL,
    ) -> None:
        self._exception = exc
        self._eof_callbacks.clear()

        waiter = self._waiter
        if waiter is not None:
            self._waiter = None
            set_exception(waiter, exc, exc_cause)

        waiter = self._eof_waiter
        if waiter is not None:
            self._eof_waiter = None
            set_exception(waiter, exc, exc_cause)

    def on_eof(self, callback: Callable[[], None]) -> None:
        if self._eof:
            try:
                callback()
            except Exception:
                internal_logger.exception("Exception in eof callback")
        else:
            self._eof_callbacks.append(callback)

    def feed_eof(self) -> None:
        self._eof = True

        waiter = self._waiter
        if waiter is not None:
            self._waiter = None
            set_result(waiter, None)

        waiter = self._eof_waiter
        if waiter is not None:
            self._eof_waiter = None
            set_result(waiter, None)

        self._protocol.resume_reading(resume_parser=False)

        for cb in self._eof_callbacks:
            try:
                cb()
            except Exception:
                internal_logger.exception("Exception in eof callback")

        self._eof_callbacks.clear()

    def is_eof(self) -> bool:
        """Return True if  'feed_eof' was called."""
        return self._eof

    def at_eof(self) -> bool:
        """Return True if the buffer is empty and 'feed_eof' was called."""
        return self._eof and not self._buffer



    def unread_data(self, data: bytes) -> None:
        """rollback reading some data from stream, inserting it to buffer head."""
        warnings.warn(
            "unread_data() is deprecated "
            "and will be removed in future releases (#3260)",
            DeprecationWarning,
            stacklevel=2,
        )
        if not data:
            return

        if self._buffer_offset:
            self._buffer[0] = self._buffer[0][self._buffer_offset :]
            self._buffer_offset = 0
        self._size += len(data)
        self._cursor -= len(data)
        self._buffer.appendleft(data)
        self._eof_counter = 0

    def feed_data(self, data: bytes) -> bool:
        assert not self._eof, "feed_data after feed_eof"

        if not data:
            return False

        data_len = len(data)
        self._size += data_len
        self._buffer.append(data)
        self.total_bytes += data_len

        waiter = self._waiter
        if waiter is not None:
            self._waiter = None
            set_result(waiter, None)

        if self._size > self._high_water:
            self._protocol.pause_reading()
        return False

    def begin_http_chunk_receiving(self) -> None:
        if self._http_chunk_splits is None:
            if self.total_bytes:
                raise RuntimeError(
                    "Called begin_http_chunk_receiving when some data was already fed"
                )
            self._http_chunk_splits = collections.deque()

    def end_http_chunk_receiving(self) -> None:
        if self._http_chunk_splits is None:
            raise RuntimeError(
                "Called end_chunk_receiving without calling "
                "begin_chunk_receiving first"
            )

        pos = self._http_chunk_splits[-1] if self._http_chunk_splits else 0

        if self.total_bytes == pos:
            return

        self._http_chunk_splits.append(self.total_bytes)

        if len(self._http_chunk_splits) > self._high_water_chunks:
            self._protocol.pause_reading()

        waiter = self._waiter
        if waiter is not None:
            self._waiter = None
            set_result(waiter, None)

    async def _wait(self, func_name: str) -> None:
        if not self._protocol.connected:
            raise RuntimeError("Connection closed.")

        if self._waiter is not None:
            raise RuntimeError(
                "%s() called while another coroutine is "
                "already waiting for incoming data" % func_name
            )

        waiter = self._waiter = self._loop.create_future()
        try:
            with self._timer:
                await waiter
        finally:
            self._waiter = None

    async def _fire_chunk_received(self, chunk: bytes) -> None:
        cb = self._on_chunk_received
        assert cb is not None
        with self._timer:
            await cb(chunk)

    async def readline(self, *, max_line_length: int | None = None) -> bytes:
        return await self.readuntil(max_size=max_line_length)

    async def readuntil(
        self, separator: bytes = b"\n", *, max_size: int | None = None
    ) -> bytes:
        seplen = len(separator)
        if seplen == 0:
            raise ValueError("Separator should be at least one-byte string")

        if self._exception is not None:
            raise self._exception

        chunk = b""
        chunk_size = 0
        not_enough = True
        max_size = max_size or self._high_water

        while not_enough:
            while self._buffer and not_enough:
                offset = self._buffer_offset
                ichar = self._buffer[0].find(separator, offset) + 1
                data = self._read_nowait_chunk(
                    ichar - offset + seplen - 1 if ichar else -1
                )
                chunk += data
                chunk_size += len(data)
                if ichar:
                    not_enough = False

                if chunk_size > max_size:
                    raise LineTooLong(chunk[:100] + b"...", max_size)

            if self._eof:
                break

            if not_enough:
                await self._wait("readuntil")

        if chunk and self._on_chunk_received is not None:
            await self._fire_chunk_received(chunk)
        return chunk

    async def read(self, n: int = -1) -> bytes:
        if self._exception is not None:
            raise self._exception

        if not n:
            return b""

        if n < 0:
            self.set_read_chunk_size(sys.maxsize)
            blocks = []
            while True:
                block = await self.readany()
                if not block:
                    break
                blocks.append(block)
            return b"".join(blocks)

        self.set_read_chunk_size(n)
        while not self._buffer and not self._eof:
            await self._wait("read")

        chunk = self._read_nowait(n)
        if chunk and self._on_chunk_received is not None:
            await self._fire_chunk_received(chunk)
        return chunk

    async def readany(self) -> bytes:
        if self._exception is not None:
            raise self._exception

        while not self._buffer and not self._eof:
            await self._wait("readany")

        chunk = self._read_nowait(-1)
        if chunk and self._on_chunk_received is not None:
            await self._fire_chunk_received(chunk)
        return chunk

    async def readchunk(self) -> tuple[bytes, bool]:
        pass



    def _read_nowait_chunk(self, n: int) -> bytes:
        first_buffer = self._buffer[0]
        offset = self._buffer_offset
        if n != -1 and len(first_buffer) - offset > n:
            data = first_buffer[offset : offset + n]
            self._buffer_offset += n

        elif offset:
            self._buffer.popleft()
            data = first_buffer[offset:]
            self._buffer_offset = 0

        else:
            data = self._buffer.popleft()

        data_len = len(data)
        self._size -= data_len
        self._cursor += data_len

        chunk_splits = self._http_chunk_splits
        while chunk_splits and chunk_splits[0] < self._cursor:
            chunk_splits.popleft()

        if self._size < self._low_water and (
            self._http_chunk_splits is None
            or len(self._http_chunk_splits) < self._low_water_chunks
        ):
            self._protocol.resume_reading()
        return data

    def _read_nowait(self, n: int) -> bytes:
        """Read not more than n bytes, or whole buffer if n == -1"""
        self._timer.assert_timeout()

        if n == -1:
            count = len(self._buffer)
            if count == 1:
                return self._read_nowait_chunk(-1)
            return b"".join([self._read_nowait_chunk(-1) for _ in range(count)])

        chunks: list[bytes] = []
        while self._buffer:
            chunk = self._read_nowait_chunk(n)
            chunks.append(chunk)
            n -= len(chunk)
            if n == 0:
                break

        return b"".join(chunks) if chunks else b""


class EmptyStreamReader(StreamReader):  # lgtm [py/missing-call-to-init]

    __slots__ = ("_read_eof_chunk",)

    def __init__(self) -> None:
        self._read_eof_chunk = False
        self.total_bytes = 0


    @_on_chunk_received.setter
    def _on_chunk_received(
        self, value: Callable[[bytes], Coroutine[None, None, None]] | None
    ) -> None:
        raise AttributeError("EmptyStreamReader._on_chunk_received is read-only")

    def __repr__(self) -> str:
        return "<%s>" % self.__class__.__name__

    def exception(self) -> BaseException | None:
        return None

    def set_exception(
        self,
        exc: type[BaseException] | BaseException,
        exc_cause: BaseException = _EXC_SENTINEL,
    ) -> None:
        pass

    def on_eof(self, callback: Callable[[], None]) -> None:
        try:
            callback()
        except Exception:
            internal_logger.exception("Exception in eof callback")

    def feed_eof(self) -> None:
        pass

    def is_eof(self) -> bool:
        return True

    def at_eof(self) -> bool:
        return True


    def feed_data(self, data: bytes) -> bool:
        return False

    def set_read_chunk_size(self, n: int) -> None:
        return

    async def readline(self, *, max_line_length: int | None = None) -> bytes:
        return b""

    async def read(self, n: int = -1) -> bytes:
        return b""


    async def readany(self) -> bytes:
        return b""


    async def readexactly(self, n: int) -> bytes:
        raise asyncio.IncompleteReadError(b"", n)



EMPTY_PAYLOAD: Final[StreamReader] = EmptyStreamReader()


class DataQueue(Generic[_T]):

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._eof = False
        self._waiter: asyncio.Future[None] | None = None
        self._exception: type[BaseException] | BaseException | None = None
        self._buffer: collections.deque[_T] = collections.deque()

    def __len__(self) -> int:
        return len(self._buffer)

    def is_eof(self) -> bool:
        return self._eof

    def at_eof(self) -> bool:
        return self._eof and not self._buffer

    def exception(self) -> type[BaseException] | BaseException | None:
        return self._exception

    def set_exception(
        self,
        exc: type[BaseException] | BaseException,
        exc_cause: BaseException = _EXC_SENTINEL,
    ) -> None:
        self._eof = True
        self._exception = exc
        if (waiter := self._waiter) is not None:
            self._waiter = None
            set_exception(waiter, exc, exc_cause)

    def feed_data(self, data: _T) -> None:
        self._buffer.append(data)
        if (waiter := self._waiter) is not None:
            self._waiter = None
            set_result(waiter, None)

    def feed_eof(self) -> None:
        self._eof = True
        if (waiter := self._waiter) is not None:
            self._waiter = None
            set_result(waiter, None)

    async def read(self) -> _T:
        if not self._buffer and not self._eof:
            assert not self._waiter
            self._waiter = self._loop.create_future()
            try:
                await self._waiter
            except (asyncio.CancelledError, asyncio.TimeoutError):
                self._waiter = None
                raise
        if self._buffer:
            return self._buffer.popleft()
        if self._exception is not None:
            raise self._exception
        raise EofStream

    def __aiter__(self) -> AsyncStreamIterator[_T]:
        return AsyncStreamIterator(self.read)
