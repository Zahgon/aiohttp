import asyncio
import enum
import io
import json
import mimetypes
import os
import sys
import warnings
from abc import ABC, abstractmethod
from collections.abc import AsyncIterable, AsyncIterator, Iterable
from itertools import chain
from typing import IO, Any, Final, TextIO

from multidict import CIMultiDict

from . import hdrs
from .abc import AbstractStreamWriter
from .helpers import (
    _SENTINEL,
    DEFAULT_CHUNK_SIZE,
    content_disposition_header,
    guess_filename,
    parse_mimetype,
    sentinel,
)
from .http_writer import _safe_header
from .streams import StreamReader
from .typedefs import JSONBytesEncoder, JSONEncoder

__all__ = (
    "PAYLOAD_REGISTRY",
    "get_payload",
    "payload_type",
    "Payload",
    "BytesPayload",
    "StringPayload",
    "IOBasePayload",
    "BytesIOPayload",
    "BufferedReaderPayload",
    "TextIOPayload",
    "StringIOPayload",
    "JsonPayload",
    "JsonBytesPayload",
    "AsyncIterablePayload",
)

TOO_LARGE_BYTES_BODY: Final[int] = 2**20  # 1 MB
_CLOSE_FUTURES: set[asyncio.Future[None]] = set()


class LookupError(Exception):
    pass


class Order(str, enum.Enum):
    normal = "normal"
    try_first = "try_first"
    try_last = "try_last"


def get_payload(data: Any, *args: Any, **kwargs: Any) -> "Payload":
    return PAYLOAD_REGISTRY.get(data, *args, **kwargs)




class payload_type:
    def __init__(self, type: Any, *, order: Order = Order.normal) -> None:
        self.type = type
        self.order = order

    def __call__(self, factory: type["Payload"]) -> type["Payload"]:
        register_payload(factory, self.type, order=self.order)
        return factory


PayloadType = type["Payload"]
_PayloadRegistryItem = tuple[PayloadType, Any]


class PayloadRegistry:

    __slots__ = ("_first", "_normal", "_last", "_normal_lookup")

    def __init__(self) -> None:
        self._first: list[_PayloadRegistryItem] = []
        self._normal: list[_PayloadRegistryItem] = []
        self._last: list[_PayloadRegistryItem] = []
        self._normal_lookup: dict[Any, PayloadType] = {}

    def get(
        self,
        data: Any,
        *args: Any,
        _CHAIN: "type[chain[_PayloadRegistryItem]]" = chain,
        **kwargs: Any,
    ) -> "Payload":
        if self._first:
            for factory, type_ in self._first:
                if isinstance(data, type_):
                    return factory(data, *args, **kwargs)
        if lookup_factory := self._normal_lookup.get(type(data)):
            return lookup_factory(data, *args, **kwargs)
        if isinstance(data, Payload):
            return data
        for factory, type_ in _CHAIN(self._normal, self._last):
            if isinstance(data, type_):
                return factory(data, *args, **kwargs)
        raise LookupError()

    def register(
        self, factory: PayloadType, type: Any, *, order: Order = Order.normal
    ) -> None:
        if order is Order.try_first:
            self._first.append((factory, type))
        elif order is Order.normal:
            self._normal.append((factory, type))
            if isinstance(type, Iterable):
                for t in type:
                    self._normal_lookup[t] = factory
            else:
                self._normal_lookup[type] = factory
        elif order is Order.try_last:
            self._last.append((factory, type))
        else:
            raise ValueError(f"Unsupported order {order!r}")


class Payload(ABC):
    _default_content_type: str = "application/octet-stream"
    _size: int | None = None
    _consumed: bool = False  # Default: payload has not been consumed yet
    _autoclose: bool = False  # Default: assume resource needs explicit closing

    def __init__(
        self,
        value: Any,
        headers: (
            CIMultiDict[str] | dict[str, str] | Iterable[tuple[str, str]] | None
        ) = None,
        content_type: None | str | _SENTINEL = sentinel,
        filename: str | None = None,
        encoding: str | None = None,
        **kwargs: Any,
    ) -> None:
        self._encoding = encoding
        self._filename = filename
        self._headers = CIMultiDict[str]()
        self._value = value
        if content_type is not sentinel and content_type is not None:
            assert isinstance(content_type, str)
            self._headers[hdrs.CONTENT_TYPE] = content_type
        elif self._filename is not None:
            if sys.version_info >= (3, 13):
                guesser = mimetypes.guess_file_type
            else:
                guesser = mimetypes.guess_type
            content_type = guesser(self._filename)[0]
            if content_type is None:
                content_type = self._default_content_type
            self._headers[hdrs.CONTENT_TYPE] = content_type
        else:
            self._headers[hdrs.CONTENT_TYPE] = self._default_content_type
        if headers:
            self._headers.update(headers)

    @property
    def size(self) -> int | None:
        pass

    @property
    def filename(self) -> str | None:
        pass

    @property
    def headers(self) -> CIMultiDict[str]:
        pass


    @property
    def encoding(self) -> str | None:
        pass

    @property
    def content_type(self) -> str:
        pass

    @property
    def consumed(self) -> bool:
        pass

    @property
    def autoclose(self) -> bool:
        pass

    def set_content_disposition(
        self,
        disptype: str,
        quote_fields: bool = True,
        _charset: str = "utf-8",
        **params: str,
    ) -> None:
        """Sets ``Content-Disposition`` header."""
        self._headers[hdrs.CONTENT_DISPOSITION] = content_disposition_header(
            disptype, quote_fields=quote_fields, _charset=_charset, params=params
        )

    @abstractmethod
    def decode(self, encoding: str = "utf-8", errors: str = "strict") -> str:
        """
        Return string representation of the value.

        This is named decode() to allow compatibility with bytes objects.
        """

    @abstractmethod
    async def write(self, writer: AbstractStreamWriter) -> None:
        """
        Write payload to the writer stream.

        Args:
            writer: An AbstractStreamWriter instance that handles the actual writing

        This is a legacy method that writes the entire payload without length constraints.

        Important:
            For new implementations, use write_with_length() instead of this method.
            This method is maintained for backwards compatibility and will eventually
            delegate to write_with_length(writer, None) in all implementations.

        All payload subclasses must override this method for backwards compatibility,
        but new code should use write_with_length for more flexibility and control.

        """

    async def write_with_length(
        self, writer: AbstractStreamWriter, content_length: int | None
    ) -> None:
        """
        Write payload with a specific content length constraint.

        Args:
            writer: An AbstractStreamWriter instance that handles the actual writing
            content_length: Maximum number of bytes to write (None for unlimited)

        This method allows writing payload content with a specific length constraint,
        which is particularly useful for HTTP responses with Content-Length header.

        Note:
            This is the base implementation that provides backwards compatibility
            for subclasses that don't override this method. Specific payload types
            should override this method to implement proper length-constrained writing.

        """
        await self.write(writer)

    async def as_bytes(self, encoding: str = "utf-8", errors: str = "strict") -> bytes:
        pass

    def _close(self) -> None:
        """
        Async safe synchronous close operations for backwards compatibility.

        This method exists only for backwards compatibility with code that
        needs to clean up payloads synchronously. In the future, we will
        drop this method and only support the async close() method.

        WARNING: This method must be safe to call from within the event loop
        without blocking. Subclasses should not perform any blocking I/O here.

        WARNING: This method must be called from within an event loop for
        certain payload types (e.g., IOBasePayload). Calling it outside an
        event loop may raise RuntimeError.
        """

    async def close(self) -> None:
        """
        Close the payload if it holds any resources.

        IMPORTANT: This method must not await anything that might not finish
        immediately, as it may be called during cleanup/cancellation. Schedule
        any long-running operations without awaiting them.

        In the future, this will be the only close method supported.
        """
        self._close()


class BytesPayload(Payload):
    _value: bytes
    _autoclose = True  # No file handle, just bytes in memory

    def __init__(
        self, value: bytes | bytearray | memoryview, *args: Any, **kwargs: Any
    ) -> None:
        if "content_type" not in kwargs:
            kwargs["content_type"] = "application/octet-stream"

        super().__init__(value, *args, **kwargs)

        if isinstance(value, memoryview):
            self._size = value.nbytes
        elif isinstance(value, (bytes, bytearray)):
            self._size = len(value)
        else:
            raise TypeError(f"value argument must be byte-ish, not {type(value)!r}")

        if self._size > TOO_LARGE_BYTES_BODY:
            warnings.warn(
                "Sending a large body directly with raw bytes might"
                " lock the event loop. You should probably pass an "
                "io.BytesIO object instead",
                ResourceWarning,
                source=self,
            )

    def decode(self, encoding: str = "utf-8", errors: str = "strict") -> str:
        return self._value.decode(encoding, errors)

    async def as_bytes(self, encoding: str = "utf-8", errors: str = "strict") -> bytes:
        pass

    async def write(self, writer: AbstractStreamWriter) -> None:
        """
        Write the entire bytes payload to the writer stream.

        Args:
            writer: An AbstractStreamWriter instance that handles the actual writing

        This method writes the entire bytes content without any length constraint.

        Note:
            For new implementations that need length control, use write_with_length().
            This method is maintained for backwards compatibility and is equivalent
            to write_with_length(writer, None).

        """
        await writer.write(self._value)

    async def write_with_length(
        self, writer: AbstractStreamWriter, content_length: int | None
    ) -> None:
        """
        Write bytes payload with a specific content length constraint.

        Args:
            writer: An AbstractStreamWriter instance that handles the actual writing
            content_length: Maximum number of bytes to write (None for unlimited)

        This method writes either the entire byte sequence or a slice of it
        up to the specified content_length. For BytesPayload, this operation
        is performed efficiently using array slicing.

        """
        if content_length is not None:
            await writer.write(self._value[:content_length])
        else:
            await writer.write(self._value)


class StringPayload(BytesPayload):
    def __init__(
        self,
        value: str,
        *args: Any,
        encoding: str | None = None,
        content_type: str | None = None,
        **kwargs: Any,
    ) -> None:
        if encoding is None:
            if content_type is None:
                real_encoding = "utf-8"
                content_type = "text/plain; charset=utf-8"
            else:
                mimetype = parse_mimetype(content_type)
                real_encoding = mimetype.parameters.get("charset", "utf-8")
        else:
            if content_type is None:
                content_type = "text/plain; charset=%s" % encoding
            real_encoding = encoding

        super().__init__(
            value.encode(real_encoding),
            encoding=real_encoding,
            content_type=content_type,
            *args,
            **kwargs,
        )


class StringIOPayload(StringPayload):
    def __init__(self, value: IO[str], *args: Any, **kwargs: Any) -> None:
        super().__init__(value.read(), *args, **kwargs)


class IOBasePayload(Payload):
    _value: io.IOBase
    _start_position: int | None = None

    def __init__(
        self, value: IO[Any], disposition: str = "attachment", *args: Any, **kwargs: Any
    ) -> None:
        if "filename" not in kwargs:
            kwargs["filename"] = guess_filename(value)

        super().__init__(value, *args, **kwargs)

        if self._filename is not None and disposition is not None:
            if hdrs.CONTENT_DISPOSITION not in self.headers:
                self.set_content_disposition(disposition, filename=self._filename)

    def _set_or_restore_start_position(self) -> None:
        """Set or restore the start position of the file-like object."""
        if self._start_position is None:
            try:
                self._start_position = self._value.tell()
            except (OSError, AttributeError):
                self._consumed = True  # Cannot seek, mark as consumed
            return
        try:
            self._value.seek(self._start_position)
        except (OSError, AttributeError):
            self._consumed = True

    def _read_and_available_len(
        self, remaining_content_len: int | None
    ) -> tuple[int | None, bytes]:
        pass

    def _read(self, remaining_content_len: int | None) -> bytes:
        pass

    @property
    def size(self) -> int | None:
        pass

    async def write(self, writer: AbstractStreamWriter) -> None:
        """
        Write the entire file-like payload to the writer stream.

        Args:
            writer: An AbstractStreamWriter instance that handles the actual writing

        This method writes the entire file content without any length constraint.
        It delegates to write_with_length() with no length limit for implementation
        consistency.

        Note:
            For new implementations that need length control, use write_with_length() directly.
            This method is maintained for backwards compatibility with existing code.

        """
        await self.write_with_length(writer, None)

    async def write_with_length(
        self, writer: AbstractStreamWriter, content_length: int | None
    ) -> None:
        """
        Write file-like payload with a specific content length constraint.

        Args:
            writer: An AbstractStreamWriter instance that handles the actual writing
            content_length: Maximum number of bytes to write (None for unlimited)

        This method implements optimized streaming of file content with length constraints:

        1. File reading is performed in a thread pool to avoid blocking the event loop
        2. Content is read and written in chunks to maintain memory efficiency
        3. Writing stops when either:
           - All available file content has been written (when size is known)
           - The specified content_length has been reached
        4. File resources are properly closed even if the operation is cancelled

        The implementation carefully handles both known-size and unknown-size payloads,
        as well as constrained and unconstrained content lengths.

        """
        loop = asyncio.get_running_loop()
        total_written_len = 0
        remaining_content_len = content_length

        available_len, chunk = await loop.run_in_executor(
            None, self._read_and_available_len, remaining_content_len
        )
        while chunk:
            chunk_len = len(chunk)

            if remaining_content_len is None:
                await writer.write(chunk)
            else:
                await writer.write(chunk[:remaining_content_len])
                remaining_content_len -= chunk_len

            total_written_len += chunk_len

            if self._should_stop_writing(
                available_len, total_written_len, remaining_content_len
            ):
                return

            chunk = await loop.run_in_executor(
                None,
                self._read,
                (
                    min(DEFAULT_CHUNK_SIZE, remaining_content_len)
                    if remaining_content_len is not None
                    else DEFAULT_CHUNK_SIZE
                ),
            )

    def _should_stop_writing(
        self,
        available_len: int | None,
        total_written_len: int,
        remaining_content_len: int | None,
    ) -> bool:
        """
        Determine if we should stop writing data.

        Args:
            available_len: Known size of the payload if available (None if unknown)
            total_written_len: Number of bytes already written
            remaining_content_len: Remaining bytes to be written for content-length limited responses

        Returns:
            True if we should stop writing data, based on either:
            - Having written all available data (when size is known)
            - Having written all requested content (when content-length is specified)

        """
        return (available_len is not None and total_written_len >= available_len) or (
            remaining_content_len is not None and remaining_content_len <= 0
        )

    def _close(self) -> None:
        """
        Async safe synchronous close operations for backwards compatibility.

        This method exists only for backwards
        compatibility. Use the async close() method instead.

        WARNING: This method MUST be called from within an event loop.
        Calling it outside an event loop will raise RuntimeError.
        """
        if self._consumed:
            return
        self._consumed = True  # Mark as consumed to prevent further writes
        loop = asyncio.get_running_loop()
        close_future = loop.run_in_executor(None, self._value.close)
        _CLOSE_FUTURES.add(close_future)
        close_future.add_done_callback(_CLOSE_FUTURES.remove)

    async def close(self) -> None:
        """
        Close the payload if it holds any resources.

        IMPORTANT: This method must not await anything that might not finish
        immediately, as it may be called during cleanup/cancellation. Schedule
        any long-running operations without awaiting them.
        """
        self._close()

    def decode(self, encoding: str = "utf-8", errors: str = "strict") -> str:
        """
        Return string representation of the value.

        WARNING: This method does blocking I/O and should not be called in the event loop.
        """
        return self._read_all().decode(encoding, errors)

    def _read_all(self) -> bytes:
        """Read the entire file-like object and return its content as bytes."""
        self._set_or_restore_start_position()
        return b"".join(self._value.readlines())

    async def as_bytes(self, encoding: str = "utf-8", errors: str = "strict") -> bytes:
        pass


class TextIOPayload(IOBasePayload):
    _value: io.TextIOBase

    def __init__(
        self,
        value: TextIO,
        *args: Any,
        encoding: str | None = None,
        content_type: str | None = None,
        **kwargs: Any,
    ) -> None:
        if encoding is None:
            if content_type is None:
                encoding = "utf-8"
                content_type = "text/plain; charset=utf-8"
            else:
                mimetype = parse_mimetype(content_type)
                encoding = mimetype.parameters.get("charset", "utf-8")
        else:
            if content_type is None:
                content_type = "text/plain; charset=%s" % encoding

        super().__init__(
            value,
            content_type=content_type,
            encoding=encoding,
            *args,
            **kwargs,
        )

    def _read_and_available_len(
        self, remaining_content_len: int | None
    ) -> tuple[int | None, bytes]:
        pass

    def _read(self, remaining_content_len: int | None) -> bytes:
        pass

    def decode(self, encoding: str = "utf-8", errors: str = "strict") -> str:
        """
        Return string representation of the value.

        WARNING: This method does blocking I/O and should not be called in the event loop.
        """
        self._set_or_restore_start_position()
        return self._value.read()

    async def as_bytes(self, encoding: str = "utf-8", errors: str = "strict") -> bytes:
        pass


class BytesIOPayload(IOBasePayload):
    _value: io.BytesIO
    _size: int  # Always initialized in __init__
    _autoclose = True  # BytesIO is in-memory, safe to auto-close

    def __init__(self, value: io.BytesIO, *args: Any, **kwargs: Any) -> None:
        super().__init__(value, *args, **kwargs)
        self._size = len(self._value.getbuffer()) - self._value.tell()

    @property
    def size(self) -> int:
        pass

    def decode(self, encoding: str = "utf-8", errors: str = "strict") -> str:
        self._set_or_restore_start_position()
        return self._value.read().decode(encoding, errors)

    async def write(self, writer: AbstractStreamWriter) -> None:
        return await self.write_with_length(writer, None)

    async def write_with_length(
        self, writer: AbstractStreamWriter, content_length: int | None
    ) -> None:
        """
        Write BytesIO payload with a specific content length constraint.

        Args:
            writer: An AbstractStreamWriter instance that handles the actual writing
            content_length: Maximum number of bytes to write (None for unlimited)

        This implementation is specifically optimized for BytesIO objects:

        1. Reads content in chunks to maintain memory efficiency
        2. Yields control back to the event loop periodically to prevent blocking
           when dealing with large BytesIO objects
        3. Respects content_length constraints when specified
        4. Properly cleans up by closing the BytesIO object when done or on error

        The periodic yielding to the event loop is important for maintaining
        responsiveness when processing large in-memory buffers.

        """
        self._set_or_restore_start_position()
        loop_count = 0
        remaining_bytes = content_length
        while chunk := self._value.read(DEFAULT_CHUNK_SIZE):
            if loop_count > 0:
                await asyncio.sleep(0)
            if remaining_bytes is None:
                await writer.write(chunk)
            else:
                await writer.write(chunk[:remaining_bytes])
                remaining_bytes -= len(chunk)
                if remaining_bytes <= 0:
                    return
            loop_count += 1

    async def as_bytes(self, encoding: str = "utf-8", errors: str = "strict") -> bytes:
        pass

    async def close(self) -> None:
        """
        Close the BytesIO payload.

        This does nothing since BytesIO is in-memory and does not require explicit closing.
        """


class BufferedReaderPayload(IOBasePayload):
    _value: io.BufferedIOBase

    def decode(self, encoding: str = "utf-8", errors: str = "strict") -> str:
        self._set_or_restore_start_position()
        return self._value.read().decode(encoding, errors)


class JsonPayload(BytesPayload):
    def __init__(
        self,
        value: Any,
        encoding: str = "utf-8",
        content_type: str = "application/json",
        dumps: JSONEncoder = json.dumps,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            dumps(value).encode(encoding),
            content_type=content_type,
            encoding=encoding,
            *args,
            **kwargs,
        )


class JsonBytesPayload(BytesPayload):

    def __init__(
        self,
        value: Any,
        dumps: JSONBytesEncoder,
        content_type: str = "application/json",
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            dumps(value),
            content_type=content_type,
            *args,
            **kwargs,
        )


class AsyncIterablePayload(Payload):
    _iter: AsyncIterator[bytes] | None = None
    _value: AsyncIterable[bytes]
    _cached_chunks: list[bytes] | None = None
    _autoclose = True  # Iterator doesn't need explicit closing

    def __init__(self, value: AsyncIterable[bytes], *args: Any, **kwargs: Any) -> None:
        if not isinstance(value, AsyncIterable):
            raise TypeError(
                "value argument must support "
                "collections.abc.AsyncIterable interface, "
                f"got {type(value)!r}"
            )

        if "content_type" not in kwargs:
            kwargs["content_type"] = "application/octet-stream"

        super().__init__(value, *args, **kwargs)

        self._iter = value.__aiter__()

    async def write(self, writer: AbstractStreamWriter) -> None:
        """
        Write the entire async iterable payload to the writer stream.

        Args:
            writer: An AbstractStreamWriter instance that handles the actual writing

        This method iterates through the async iterable and writes each chunk
        to the writer without any length constraint.

        Note:
            For new implementations that need length control, use write_with_length() directly.
            This method is maintained for backwards compatibility with existing code.

        """
        await self.write_with_length(writer, None)

    async def write_with_length(
        self, writer: AbstractStreamWriter, content_length: int | None
    ) -> None:
        """
        Write async iterable payload with a specific content length constraint.

        Args:
            writer: An AbstractStreamWriter instance that handles the actual writing
            content_length: Maximum number of bytes to write (None for unlimited)

        This implementation handles streaming of async iterable content with length constraints:

        1. If cached chunks are available, writes from them
        2. Otherwise iterates through the async iterable one chunk at a time
        3. Respects content_length constraints when specified
        4. Does NOT generate cache - that's done by as_bytes()

        """
        if self._cached_chunks is not None:
            remaining_bytes = content_length
            for chunk in self._cached_chunks:
                if remaining_bytes is None:
                    await writer.write(chunk)
                elif remaining_bytes > 0:
                    await writer.write(chunk[:remaining_bytes])
                    remaining_bytes -= len(chunk)
                else:
                    break
            return

        if self._iter is None:
            return

        remaining_bytes = content_length

        try:
            while True:
                chunk = await anext(self._iter)
                if remaining_bytes is None:
                    await writer.write(chunk)
                elif remaining_bytes > 0:
                    await writer.write(chunk[:remaining_bytes])
                    remaining_bytes -= len(chunk)
        except StopAsyncIteration:
            self._iter = None
            self._consumed = True  # Mark as consumed when streamed without caching

    def decode(self, encoding: str = "utf-8", errors: str = "strict") -> str:
        """Decode the payload content as a string if cached chunks are available."""
        if self._cached_chunks is not None:
            return b"".join(self._cached_chunks).decode(encoding, errors)
        raise TypeError("Unable to decode - content not cached. Call as_bytes() first.")

    async def as_bytes(self, encoding: str = "utf-8", errors: str = "strict") -> bytes:
        pass


class StreamReaderPayload(AsyncIterablePayload):
    def __init__(self, value: StreamReader, *args: Any, **kwargs: Any) -> None:
        super().__init__(value.iter_any(), *args, **kwargs)


PAYLOAD_REGISTRY = PayloadRegistry()
PAYLOAD_REGISTRY.register(BytesPayload, (bytes, bytearray, memoryview))
PAYLOAD_REGISTRY.register(StringPayload, str)
PAYLOAD_REGISTRY.register(StringIOPayload, io.StringIO)
PAYLOAD_REGISTRY.register(TextIOPayload, io.TextIOBase)
PAYLOAD_REGISTRY.register(BytesIOPayload, io.BytesIO)
PAYLOAD_REGISTRY.register(BufferedReaderPayload, (io.BufferedReader, io.BufferedRandom))
PAYLOAD_REGISTRY.register(IOBasePayload, io.IOBase)
PAYLOAD_REGISTRY.register(StreamReaderPayload, StreamReader)
PAYLOAD_REGISTRY.register(AsyncIterablePayload, AsyncIterable, order=Order.try_last)
