import sys

from loguru import logger

DEBUG = False

gettrace = getattr(sys, "gettrace", None)

if gettrace is None or gettrace():
    DEBUG = True
    logger.warning("Debug mode enabled")
