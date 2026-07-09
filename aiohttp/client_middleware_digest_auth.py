
import hashlib
import os
import re
import sys
import time
from collections.abc import Callable
from typing import Final, Literal, TypedDict

from yarl import URL

from . import hdrs
from .client_exceptions import ClientError
from .client_middlewares import ClientHandlerType
from .client_reqrep import ClientRequest, ClientResponse
from .payload import Payload


class DigestAuthChallenge(TypedDict, total=False):
    realm: str
    nonce: str
    qop: str
    algorithm: str
    opaque: str
    domain: str
    stale: str


DigestFunctions: dict[str, Callable[[bytes], "hashlib._Hash"]] = {
    "MD5": hashlib.md5,
    "MD5-SESS": hashlib.md5,
    "SHA": hashlib.sha1,
    "SHA-SESS": hashlib.sha1,
    "SHA256": hashlib.sha256,
    "SHA256-SESS": hashlib.sha256,
    "SHA-256": hashlib.sha256,
    "SHA-256-SESS": hashlib.sha256,
    "SHA512": hashlib.sha512,
    "SHA512-SESS": hashlib.sha512,
    "SHA-512": hashlib.sha512,
    "SHA-512-SESS": hashlib.sha512,
}


_HEADER_PAIRS_PATTERN = re.compile(
    r'(?:^|\s|,\s*)(\w+)(?:\s*=\s*(?:"((?:[^"\\]|\\.)*)"|([^\s,]+)))?'
    if sys.version_info < (3, 11)
    else r'(?:^|\s|,\s*)((?>\w+))(?:\s*=\s*(?:"((?:[^"\\]|\\.)*)"|([^\s,]+)))?'
)


CHALLENGE_FIELDS: Final[
    tuple[
        Literal["realm", "nonce", "qop", "algorithm", "opaque", "domain", "stale"], ...
    ]
] = (
    "realm",
    "nonce",
    "qop",
    "algorithm",
    "opaque",
    "domain",
    "stale",
)

SUPPORTED_ALGORITHMS: Final[tuple[str, ...]] = tuple(sorted(DigestFunctions.keys()))

QUOTED_AUTH_FIELDS: Final[frozenset[str]] = frozenset(
    {"username", "realm", "nonce", "uri", "response", "opaque", "cnonce"}
)


def escape_quotes(value: str) -> str:
    """Escape double quotes for HTTP header values."""
    return value.replace('"', '\\"')


def unescape_quotes(value: str) -> str:
    """Unescape double quotes in HTTP header values."""
    return value.replace('\\"', '"')


def parse_header_pairs(header: str) -> dict[str, str]:
    """
    Parse key-value pairs from the first challenge of a WWW-Authenticate header.

    This function handles the complex format of WWW-Authenticate header values,
    supporting both quoted and unquoted values, proper handling of commas in
    quoted values, and whitespace variations per RFC 7616.

    A single header may carry several challenges
    (https://www.rfc-editor.org/rfc/rfc7235#section-4.1). Parsing
    stops at the next auth-scheme token so a later challenge's parameters cannot
    overwrite the first challenge's values; a leading scheme token is skipped.

    Examples of supported formats:
      - key1="value1", key2=value2
      - key1 = "value1" , key2="value, with, commas"
      - key1=value1,key2="value2"
      - realm="example.com", nonce="12345", qop="auth"

    Args:
        header: The header value string to parse

    Returns:
        Dictionary mapping parameter names to their values
    """
    pairs: dict[str, str] = {}
    for match in _HEADER_PAIRS_PATTERN.finditer(header):
        key = match.group(1)
        quoted_val, unquoted_val = match.group(2), match.group(3)
        if quoted_val is None and unquoted_val is None:
            if pairs:
                break
            continue
        pairs[key] = (
            unescape_quotes(quoted_val) if quoted_val is not None else unquoted_val
        )
    return pairs


class DigestAuthMiddleware:

    def __init__(
        self,
        login: str,
        password: str,
        preemptive: bool = True,
    ) -> None:
        if login is None:
            raise ValueError("None is not allowed as login value")

        if password is None:
            raise ValueError("None is not allowed as password value")

        if ":" in login:
            raise ValueError('A ":" is not allowed in username (RFC 1945#section-11.1)')

        self._login_str: Final[str] = login
        self._login_bytes: Final[bytes] = login.encode("utf-8")
        self._password_bytes: Final[bytes] = password.encode("utf-8")

        self._last_nonce_bytes = b""
        self._nonce_count = 0
        self._challenge: DigestAuthChallenge = {}
        self._preemptive: bool = preemptive
        self._protection_space: list[str] = []
        self._origin: URL | None = None

    async def _encode(self, method: str, url: URL, body: Payload | Literal[b""]) -> str:
        pass

    def _in_protection_space(self, url: URL) -> bool:
        pass

    def _authenticate(self, response: ClientResponse) -> bool:
        pass

    async def __call__(
        self, request: ClientRequest, handler: ClientHandlerType
    ) -> ClientResponse:
        """Run the digest auth middleware."""
        origin = request.url.origin()
        if self._origin is None:
            self._origin = origin
        elif origin != self._origin and not self._in_protection_space(request.url):
            return await handler(request)

        response = None
        for retry_count in range(2):
            if retry_count > 0 or (
                self._preemptive
                and self._challenge
                and self._in_protection_space(request.url)
            ):
                request.headers[hdrs.AUTHORIZATION] = await self._encode(
                    request.method, request.url, request.body
                )

            response = await handler(request)

            if not self._authenticate(response):
                break

        assert response is not None
        return response
