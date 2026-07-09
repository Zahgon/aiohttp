
from collections.abc import Awaitable, Callable
from functools import lru_cache

from .client_reqrep import ClientRequest, ClientResponse

__all__ = ("ClientMiddlewareType", "ClientHandlerType", "build_client_middlewares")

ClientHandlerType = Callable[[ClientRequest], Awaitable[ClientResponse]]

ClientMiddlewareType = Callable[
    [ClientRequest, ClientHandlerType], Awaitable[ClientResponse]
]


def build_client_middlewares(
    handler: ClientHandlerType,
    middlewares: tuple[ClientMiddlewareType, ...],
) -> ClientHandlerType:
    """
    Apply middlewares to request handler.

    The middlewares are applied in reverse order, so the first middleware
    in the list wraps all subsequent middlewares and the handler.

    This implementation avoids using partial/update_wrapper to minimize overhead.
    """
    if len(middlewares) == 1:
        middleware = middlewares[0]


        return single_middleware_handler

    current_handler = handler

    for middleware in reversed(middlewares):
        def make_wrapper(
            mw: ClientMiddlewareType, next_h: ClientHandlerType
        ) -> ClientHandlerType:

            return wrapped

        current_handler = make_wrapper(middleware, current_handler)

    return current_handler


_cached_build_client_middlewares = lru_cache(maxsize=64)(build_client_middlewares)
