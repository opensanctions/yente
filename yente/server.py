from uvicorn import Config, Server

from yente import settings
from yente.app import app
from yente.logs import configure_logging


if __name__ == "__main__":
    server = Server(
        Config(
            app,
            host="0.0.0.0",
            port=settings.PORT,
            proxy_headers=True,
            reload=settings.DEBUG,
            debug=settings.DEBUG,
            log_level=settings.LOG_LEVEL,
        ),
    )
    configure_logging()
    server.run()
