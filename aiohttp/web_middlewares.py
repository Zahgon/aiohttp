import re
import warnings
from typing import TYPE_CHECKING, TypeVar

from .typedefs import Handler, Middleware
from .web_exceptions import (
    HTTPMethodNotAllowed,
    HTTPMove,
    HTTPNotFound,
    HTTPPermanentRedirect,
)
from .web_request import Request
from .web_response import StreamResponse

__all__ = (
    "middleware",
    "normalize_path_middleware",
)

if TYPE_CHECKING:
    from .web_app import Application

_Func = TypeVar("_Func")


async def _check_request_resolves(request: Request, path: str) -> tuple[bool, Request]:
    alt_request = request.clone(rel_url=path)

    match_info = await request.app.router.resolve(alt_request)
    alt_request._match_info = match_info

    if match_info.http_exception is None:
        return True, alt_request

    return False, request


def middleware(f: _Func) -> _Func:
    warnings.warn(
        "Middleware decorator is deprecated since 4.0 "
        "and its behaviour is default, "
        "you can simply remove this decorator.",
        DeprecationWarning,
        stacklevel=2,
    )
    return f


def normalize_path_middleware(
    *,
    append_slash: bool = True,
    remove_slash: bool = False,
    merge_slashes: bool = True,
    redirect_class: type[HTTPMove] = HTTPPermanentRedirect,
) -> Middleware:
    """Factory for producing a middleware that normalizes the path of a request.

    Normalizing means:
        - Add or remove a trailing slash to the path.
        - Double slashes are replaced by one.

    The middleware returns as soon as it finds a path that resolves
    correctly. The order if both merge and append/remove are enabled is
        1) merge slashes
        2) append/remove slash
        3) both merge slashes and append/remove slash.
    If the path resolves with at least one of those conditions, it will
    redirect to the new path.

    Only one of `append_slash` and `remove_slash` can be enabled. If both
    are `True` the factory will raise an assertion error

    If `append_slash` is `True` the middleware will append a slash when
    needed. If a resource is defined with trailing slash and the request
    comes without it, it will append it automatically.

    If `remove_slash` is `True`, `append_slash` must be `False`. When enabled
    the middleware will remove trailing slashes and redirect if the resource
    is defined

    If merge_slashes is True, merge multiple consecutive slashes in the
    path into one.
    """
    correct_configuration = not (append_slash and remove_slash)
    assert correct_configuration, "Cannot both remove and append slash"


    return impl


def _fix_request_current_app(app: "Application") -> Middleware:

    return impl
