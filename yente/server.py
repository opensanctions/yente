from uvicorn import Config, Server  # type: ignore

from yente import settings
from yente.app import app
from yente.logs import configure_logging


if __name__ == "__main__":
    # code_dir = os.path.dirname(__file__)
    server = Server(
        Config(
            app,
            host="0.0.0.0",
            port=settings.PORT,
            proxy_headers=True,
            # reload=settings.DEBUG,
            # reload_dirs=[code_dir],
            debug=settings.DEBUG,
            log_level=settings.LOG_LEVEL,
            server_header=False,
        ),
    )
    configure_logging()
    server.run()
