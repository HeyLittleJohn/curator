import sys

from aiohttp.client_exceptions import ClientConnectionError, ClientResponseError
from aiomultiprocess.types import ProxyException
from sentry_sdk import capture_exception

from option_bot.proj_constants import log


class ProjBaseException(Exception):
    def __init__(self, message: str | None = None, args: dict | None = None):
        capture_exception(self)
        log.error(self)
        super().__init__(message)
        if args:
            message += "; see logs for Args"
            log.error(f"exception triggering args: {args} ")
        log.error(message)

    def __new__(cls, *args, **kwargs):
        new_cls = super().__new__(cls, *args, **kwargs)
        new_cls.__bases__ = (ProjBaseException,)
        return new_cls


def my_excepthook(etype, value, traceback):
    if issubclass(etype, BaseException):
        etype = etype.__name__
        etype = "Proj" + etype
        etype = etype.replace("ErrorError", "Error")
        etype = getattr(sys.modules[__name__], etype)
    sys.__excepthook__(etype, value, traceback)


sys.excepthook = my_excepthook


class InvalidArgs(ProjBaseException):
    """Exception when incorrect args are passed to the CLI"""


class ProjTypeError(ProjBaseException, TypeError):
    """TypeError with ProjBaseException"""


class ProjValueError(ProjBaseException, ValueError):
    """A custom ValueError that inherits from MyProjectError and ValueError."""


class ProjRuntimeError(ProjBaseException, RuntimeError):
    """A custom RuntimeError that inherits from ProjBaseException and RuntimeError."""


class ProjTimeoutError(ProjBaseException, TimeoutError):
    """A custom TimeoutError that inherits from ProjBaseException and TimeoutError."""


class ProfClientConnectionError(ProjBaseException, ClientConnectionError):
    """A custom ClientConnectionError that inherits from ProjBaseException and ClientConnectionError."""


class ProjClientResponseError(ProjBaseException, ClientResponseError):
    """A custom ClientResponseError that inherits from ProjBaseException and ClientResponseError."""


class ProjProxyException(ProjBaseException, ProxyException):
    """A custom ProxyException that inherits from ProjBaseException and ProxyException."""
