import io
import sys

from loguru import logger
from omu.connection import Address

from omuserver.server.omuserver import OmuServer

address = Address(
    host="0.0.0.0",
    port=26423,
    secure=False,
)
server = OmuServer(address)


def set_output_utf8():
    if isinstance(sys.stdout, io.TextIOWrapper):
        sys.stdout.reconfigure(encoding="utf-8")
    if isinstance(sys.stderr, io.TextIOWrapper):
        sys.stderr.reconfigure(encoding="utf-8")


if __name__ == "__main__":
    set_output_utf8()
    logger.info("Starting server...")
    server.run()
