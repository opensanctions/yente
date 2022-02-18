import logging
from uvicorn import Config, Server

from yente.app import app
from yente.logs import configure_logging


if __name__ == "__main__":
    server = Server(
        Config(
            app,
            host="0.0.0.0",
            port=8000,
            proxy_headers=True,
            log_level=logging.INFO,
        ),
    )
    configure_logging()
    server.run()
