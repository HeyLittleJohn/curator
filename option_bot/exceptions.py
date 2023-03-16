from sentry_sdk import capture_exception

from option_bot.proj_constants import log


class BaseException(Exception):
    def __init__(self, message, args: dict | None = None):
        super().__init__(message)
        capture_exception(self)
        if args:
            message += "; see logs for Args"
            log.error(f"exception triggering args: {args} ")
        log.error(message)

    def __new__(cls, *args, **kwargs):
        new_cls = super().__new__(cls, *args, **kwargs)
        new_cls.__bases__ = (BaseException,)
        return new_cls
