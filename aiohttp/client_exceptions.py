
import asyncio
from collections.abc import Mapping
from typing import TYPE_CHECKING, Union

from .typedefs import StrOrURL

try:
    import ssl

    SSLContext = ssl.SSLContext
except ImportError:  # pragma: no cover
    ssl = SSLContext = None  # type: ignore[assignment]

if TYPE_CHECKING:
    from .client_reqrep import ClientResponse, ConnectionKey, Fingerprint, RequestInfo
    from .http_parser import RawResponseMessage
else:
    RequestInfo = ClientResponse = ConnectionKey = RawResponseMessage = None

__all__ = (
    "ClientError",
    "ClientConnectionError",
    "ClientConnectionResetError",
    "ClientOSError",
    "ClientConnectorError",
    "ClientProxyConnectionError",
    "ClientSSLError",
    "ClientConnectorDNSError",
    "ClientConnectorSSLError",
    "ClientConnectorCertificateError",
    "ConnectionTimeoutError",
    "SocketTimeoutError",
    "ServerConnectionError",
    "ServerTimeoutError",
    "ServerDisconnectedError",
    "ServerFingerprintMismatch",
    "ClientResponseError",
    "ClientHttpProxyError",
    "WSServerHandshakeError",
    "ContentTypeError",
    "ClientPayloadError",
    "InvalidURL",
    "InvalidUrlClientError",
    "RedirectClientError",
    "NonHttpUrlClientError",
    "InvalidUrlRedirectClientError",
    "NonHttpUrlRedirectClientError",
    "WSMessageTypeError",
)


class ClientError(Exception):
    pass


class ClientResponseError(ClientError):

    args: tuple[RequestInfo, tuple[ClientResponse, ...]]

    def __init__(
        self,
        request_info: RequestInfo,
        history: tuple[ClientResponse, ...],
        *,
        status: int | None = None,
        message: str = "",
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self.request_info = request_info
        if status is not None:
            self.status = status
        else:
            self.status = 0
        self.message = message
        self.headers = headers
        self.history = history
        self.args = (request_info, history)

    def __str__(self) -> str:
        return f"{self.status}, message={self.message!r}, url={str(self.request_info.real_url)!r}"

    def __repr__(self) -> str:
        args = f"{self.request_info!r}, {self.history!r}"
        if self.status != 0:
            args += f", status={self.status!r}"
        if self.message != "":
            args += f", message={self.message!r}"
        if self.headers is not None:
            args += f", headers={self.headers!r}"
        return f"{type(self).__name__}({args})"


class ContentTypeError(ClientResponseError):
    pass


class WSServerHandshakeError(ClientResponseError):
    pass


class ClientHttpProxyError(ClientResponseError):
    pass


class TooManyRedirects(ClientResponseError):
    pass


class ClientConnectionError(ClientError):
    pass


class ClientConnectionResetError(ClientConnectionError, ConnectionResetError):
    pass


class ClientOSError(ClientConnectionError, OSError):
    pass


class ClientConnectorError(ClientOSError):

    args: tuple[ConnectionKey, OSError]

    def __init__(self, connection_key: ConnectionKey, os_error: OSError) -> None:
        self._conn_key = connection_key
        self._os_error = os_error
        super().__init__(os_error.errno, os_error.strerror)
        self.args = (connection_key, os_error)





    def __str__(self) -> str:
        return "Cannot connect to host {0.host}:{0.port} ssl:{1} [{2}]".format(
            self, "default" if self.ssl is True else self.ssl, self.strerror
        )

    __reduce__ = BaseException.__reduce__


class ClientConnectorDNSError(ClientConnectorError):
    pass


class ClientProxyConnectionError(ClientConnectorError):
    pass


class UnixClientConnectorError(ClientConnectorError):

    def __init__(
        self, path: str, connection_key: ConnectionKey, os_error: OSError
    ) -> None:
        self._path = path
        super().__init__(connection_key, os_error)


    def __str__(self) -> str:
        return "Cannot connect to unix socket {0.path} ssl:{1} [{2}]".format(
            self, "default" if self.ssl is True else self.ssl, self.strerror
        )


class ServerConnectionError(ClientConnectionError):
    pass


class ServerDisconnectedError(ServerConnectionError):

    args: tuple[RawResponseMessage | str]

    def __init__(self, message: RawResponseMessage | str | None = None) -> None:
        if message is None:
            message = "Server disconnected"

        self.args = (message,)
        self.message = message


class ServerTimeoutError(ServerConnectionError, asyncio.TimeoutError):
    pass


class ConnectionTimeoutError(ServerTimeoutError):
    pass


class SocketTimeoutError(ServerTimeoutError):
    pass


class ServerFingerprintMismatch(ServerConnectionError):

    args: tuple[bytes, bytes, str, int]

    def __init__(self, expected: bytes, got: bytes, host: str, port: int) -> None:
        self.expected = expected
        self.got = got
        self.host = host
        self.port = port
        self.args = (expected, got, host, port)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} expected={self.expected!r} got={self.got!r} host={self.host!r} port={self.port!r}>"


class ClientPayloadError(ClientError):
    pass


class InvalidURL(ClientError, ValueError):


    args: tuple[StrOrURL] | tuple[StrOrURL, str]

    def __init__(self, url: StrOrURL, description: str | None = None) -> None:
        self._url = url
        self._description = description

        if description:
            super().__init__(url, description)
        else:
            super().__init__(url)



    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self}>"

    def __str__(self) -> str:
        if self._description:
            return f"{self._url} - {self._description}"
        return str(self._url)


class InvalidUrlClientError(InvalidURL):
    pass


class RedirectClientError(ClientError):
    pass


class NonHttpUrlClientError(ClientError):
    pass


class InvalidUrlRedirectClientError(InvalidUrlClientError, RedirectClientError):
    pass


class NonHttpUrlRedirectClientError(NonHttpUrlClientError, RedirectClientError):
    pass


class ClientSSLError(ClientConnectorError):
    pass


if ssl is not None:
    cert_errors = (ssl.CertificateError,)
    cert_errors_bases = (
        ClientSSLError,
        ssl.CertificateError,
    )

    ssl_errors = (ssl.SSLError,)
    ssl_error_bases = (ClientSSLError, ssl.SSLError)
else:  # pragma: no cover
    cert_errors = tuple()  # type: ignore[unreachable]
    cert_errors_bases = (
        ClientSSLError,
        ValueError,
    )

    ssl_errors = tuple()
    ssl_error_bases = (ClientSSLError,)


class ClientConnectorSSLError(*ssl_error_bases):  # type: ignore[misc]
    pass


class ClientConnectorCertificateError(*cert_errors_bases):  # type: ignore[misc]

    _conn_key: ConnectionKey
    args: tuple[ConnectionKey, Exception]

    def __init__(
        self,
        connection_key: ConnectionKey,
        certificate_error: Exception,
    ) -> None:
        if isinstance(certificate_error, cert_errors + (OSError,)):
            os_error = certificate_error
        else:
            os_error = OSError()

        super().__init__(connection_key, os_error)
        self._certificate_error = certificate_error
        self.args = (connection_key, certificate_error)





    def __str__(self) -> str:
        return (
            f"Cannot connect to host {self.host}:{self.port} ssl:{self.ssl} "
            f"[{self.certificate_error.__class__.__name__}: "
            f"{self.certificate_error.args}]"
        )


class WSMessageTypeError(TypeError):
    pass
