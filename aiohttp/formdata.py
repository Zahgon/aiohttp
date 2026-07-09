import io
from collections import deque
from collections.abc import Iterable
from typing import Any
from urllib.parse import urlencode

from multidict import MultiDict, MultiDictProxy

from . import hdrs, multipart, payload
from .helpers import guess_filename
from .http_writer import _safe_header
from .payload import Payload

__all__ = ("FormData",)


class FormData:

    def __init__(
        self,
        fields: Iterable[Any] = (),
        quote_fields: bool = True,
        charset: str | None = None,
        boundary: str | None = None,
        *,
        default_to_multipart: bool = False,
    ) -> None:
        self._boundary = boundary
        self._writer = multipart.MultipartWriter("form-data", boundary=self._boundary)
        self._fields: list[Any] = []
        self._is_multipart = default_to_multipart
        self._quote_fields = quote_fields
        self._charset = charset

        if isinstance(fields, dict):
            fields = list(fields.items())
        elif not isinstance(fields, (list, tuple)):
            fields = (fields,)
        self.add_fields(*fields)


    def add_field(
        self,
        name: str,
        value: Any,
        *,
        content_type: str | None = None,
        filename: str | None = None,
    ) -> None:
        if isinstance(value, (io.IOBase, bytes, bytearray, memoryview)):
            self._is_multipart = True

        _safe_header(name)
        type_options: MultiDict[str] = MultiDict({"name": name})
        if filename is not None and not isinstance(filename, str):
            raise TypeError("filename must be an instance of str. Got: %s" % filename)
        if filename is None and isinstance(value, io.IOBase):
            filename = guess_filename(value, name)
        if filename is not None:
            _safe_header(filename)
            type_options["filename"] = filename
            self._is_multipart = True

        headers = {}
        if content_type is not None:
            if not isinstance(content_type, str):
                raise TypeError(
                    "content_type must be an instance of str. Got: %s" % content_type
                )
            _safe_header(content_type)
            headers[hdrs.CONTENT_TYPE] = content_type
            self._is_multipart = True

        self._fields.append((type_options, headers, value))

    def add_fields(self, *fields: Any) -> None:
        to_add: deque[Any] = deque(fields)

        while to_add:
            rec = to_add.popleft()

            if isinstance(rec, io.IOBase):
                k = guess_filename(rec, "unknown")
                self.add_field(k, rec)  # type: ignore[arg-type]

            elif isinstance(rec, (MultiDictProxy, MultiDict)):
                to_add.extend(rec.items())

            elif isinstance(rec, (list, tuple)) and len(rec) == 2:
                k, fp = rec
                self.add_field(k, fp)

            else:
                raise TypeError(
                    "Only io.IOBase, multidict and (name, file) "
                    "pairs allowed, use .add_field() for passing "
                    f"more complex parameters, got {rec!r}"
                )


    def _gen_form_data(self) -> multipart.MultipartWriter:
        pass

    def __call__(self) -> Payload:
        if self._is_multipart:
            return self._gen_form_data()
        else:
            return self._gen_form_urlencoded()
