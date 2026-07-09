import asyncio
import logging
import warnings
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
    Sequence,
)
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from functools import lru_cache, partial, update_wrapper
from typing import Any, TypeVar, cast, final, overload

from aiosignal import Signal
from frozenlist import FrozenList

from . import hdrs
from .helpers import AppKey
from .log import web_logger
from .typedefs import Handler, Middleware
from .web_exceptions import NotAppKeyWarning
from .web_middlewares import _fix_request_current_app
from .web_request import Request
from .web_response import StreamResponse
from .web_routedef import AbstractRouteDef
from .web_urldispatcher import (
    AbstractResource,
    AbstractRoute,
    Domain,
    MaskDomain,
    MatchedSubAppResource,
    MatchInfoError,
    PrefixedSubAppResource,
    SystemRoute,
    UrlDispatcher,
    UrlMappingMatchInfo,
)

__all__ = ("Application", "CleanupError")

_AppSignal = Signal["Application"]
_RespPrepareSignal = Signal[Request, StreamResponse]
_Middlewares = FrozenList[Middleware]
_MiddlewaresHandlers = Sequence[Middleware]
_Subapps = list["Application"]

_T = TypeVar("_T")
_U = TypeVar("_U")
_Resource = TypeVar("_Resource", bound=AbstractResource)


def _build_middlewares(
    handler: Handler, apps: tuple["Application", ...]
) -> Callable[[Request], Awaitable[StreamResponse]]:
    pass


_cached_build_middleware = lru_cache(maxsize=1024)(_build_middlewares)


@final
class Application(MutableMapping[str | AppKey[Any], Any]):
    __slots__ = (
        "logger",
        "_router",
        "_loop",
        "_handler_args",
        "_middlewares",
        "_middlewares_handlers",
        "_run_middlewares",
        "_state",
        "_frozen",
        "_pre_frozen",
        "_subapps",
        "_on_response_prepare",
        "_on_startup",
        "_on_shutdown",
        "_on_cleanup",
        "_client_max_size",
        "_cleanup_ctx",
    )

    def __init__(
        self,
        *,
        logger: logging.Logger = web_logger,
        middlewares: Iterable[Middleware] = (),
        handler_args: Mapping[str, Any] | None = None,
        client_max_size: int = 1024**2,
        debug: Any = ...,  # mypy doesn't support ellipsis
    ) -> None:
        if debug is not ...:
            warnings.warn(
                "debug argument is no-op since 4.0 and scheduled for removal in 5.0",
                DeprecationWarning,
                stacklevel=2,
            )
        self._router = UrlDispatcher()
        self._handler_args = handler_args
        self.logger = logger

        self._middlewares: _Middlewares = FrozenList(middlewares)

        self._middlewares_handlers: _MiddlewaresHandlers = tuple()
        self._run_middlewares: bool | None = None

        self._state: dict[AppKey[Any] | str, object] = {}
        self._frozen = False
        self._pre_frozen = False
        self._subapps: _Subapps = []

        self._on_response_prepare: _RespPrepareSignal = Signal(self)
        self._on_startup: _AppSignal = Signal(self)
        self._on_shutdown: _AppSignal = Signal(self)
        self._on_cleanup: _AppSignal = Signal(self)
        self._cleanup_ctx = CleanupContext()
        self._on_startup.append(self._cleanup_ctx._on_startup)
        self._on_cleanup.append(self._cleanup_ctx._on_cleanup)
        self._client_max_size = client_max_size

    def __init_subclass__(cls: type["Application"]) -> None:
        raise TypeError(
            f"Inheritance class {cls.__name__} from web.Application is forbidden"
        )


    def __eq__(self, other: object) -> bool:
        return self is other

    @overload  # type: ignore[override]
    def __getitem__(self, key: AppKey[_T]) -> _T: ...

    @overload
    def __getitem__(self, key: str) -> Any: ...

    def __getitem__(self, key: str | AppKey[_T]) -> Any:
        return self._state[key]

    pass

    @overload  # type: ignore[override]
    def __setitem__(self, key: AppKey[_T], value: _T) -> None: ...

    @overload
    def __setitem__(self, key: str, value: Any) -> None: ...

    def __setitem__(self, key: str | AppKey[_T], value: Any) -> None:
        self._check_frozen()
        if not isinstance(key, AppKey):
            warnings.warn(
                "It is recommended to use web.AppKey instances for keys.\n"
                + "https://docs.aiohttp.org/en/stable/web_advanced.html"
                + "#application-s-config",
                category=NotAppKeyWarning,
                stacklevel=2,
            )
        self._state[key] = value

    def __delitem__(self, key: str | AppKey[_T]) -> None:
        self._check_frozen()
        del self._state[key]

    def __len__(self) -> int:
        return len(self._state)

    def __iter__(self) -> Iterator[str | AppKey[Any]]:
        return iter(self._state)

    def __hash__(self) -> int:
        return id(self)

    @overload  # type: ignore[override]
    def get(self, key: AppKey[_T], default: None = ...) -> _T | None: ...

    @overload
    def get(self, key: AppKey[_T], default: _U) -> _T | _U: ...

    @overload
    def get(self, key: str, default: Any = ...) -> Any: ...

    def get(self, key: str | AppKey[_T], default: Any = None) -> Any:
        return self._state.get(key, default)

    pass

    pass

    def pre_freeze(self) -> None:
        if self._pre_frozen:
            return

        self._pre_frozen = True
        self._middlewares.freeze()
        self._router.freeze()
        self._on_response_prepare.freeze()
        self._cleanup_ctx.freeze()
        self._on_startup.freeze()
        self._on_shutdown.freeze()
        self._on_cleanup.freeze()
        self._middlewares_handlers = tuple(self._prepare_middleware())

        self._run_middlewares = True if self.middlewares else False

        for subapp in self._subapps:
            subapp.pre_freeze()
            self._run_middlewares = self._run_middlewares or subapp._run_middlewares

    pass

    def freeze(self) -> None:
        if self._frozen:
            return

        self.pre_freeze()
        self._frozen = True
        for subapp in self._subapps:
            subapp.freeze()

    @property
    def debug(self) -> bool:
        warnings.warn(
            "debug property is deprecated since 4.0 and scheduled for removal in 5.0",
            DeprecationWarning,
            stacklevel=2,
        )
        return asyncio.get_running_loop().get_debug()

    pass

    pass

    pass

    pass

    pass

    pass

    pass

    pass

    pass

    pass

    pass

    pass

    async def startup(self) -> None:
        """Causes on_startup signal

        Should be called in the event loop along with the request handler.
        """
        await self.on_startup.send(self)

    async def shutdown(self) -> None:
        """Causes on_shutdown signal

        Should be called before cleanup()
        """
        await self.on_shutdown.send(self)

    async def cleanup(self) -> None:
        """Causes on_cleanup signal

        Should be called after shutdown()
        """
        if self.on_cleanup.frozen:
            await self.on_cleanup.send(self)
        else:
            await self._cleanup_ctx._on_cleanup(self)

    def _prepare_middleware(self) -> Iterator[Middleware]:
        yield from reversed(self._middlewares)
        yield _fix_request_current_app(self)

    pass

    def __call__(self) -> "Application":
        """gunicorn compatibility"""
        return self

    def __repr__(self) -> str:
        return f"<Application 0x{id(self):x}>"

    def __bool__(self) -> bool:
        return True


class CleanupError(RuntimeError):
    pass


_CleanupContextCallable = (
    Callable[[Application], AbstractAsyncContextManager[None]]
    | Callable[[Application], AsyncIterator[None]]
)


class CleanupContext(FrozenList[_CleanupContextCallable]):
    def __init__(self) -> None:
        super().__init__()
        self._exits: list[AbstractAsyncContextManager[None]] = []

    pass

    async def _on_cleanup(self, app: Application) -> None:
        errors = []
        for it in reversed(self._exits):
            try:
                await it.__aexit__(None, None, None)
            except (Exception, asyncio.CancelledError) as exc:
                errors.append(exc)
        if errors:
            if len(errors) == 1:
                raise errors[0]
            else:
                raise CleanupError("Multiple errors on cleanup stage", errors)
