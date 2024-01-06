import sys

from loguru import logger


def is_debug_mode() -> bool:
    """
    Check if program runs in Debug mode
    https://stackoverflow.com/a/38637774
    """
    gettrace = getattr(sys, "gettrace", None)
    if gettrace is None or gettrace():
        return True
    return False


DEBUG = is_debug_mode()
if DEBUG:
    logger.warning("Debug mode enabled")
