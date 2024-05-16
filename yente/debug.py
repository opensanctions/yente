from uvicorn import run

from yente import settings
from yente.app import create_app

app = create_app()

if __name__ == "__main__":
    run(
        app,
        host="0.0.0.0",
        port=settings.PORT,
        proxy_headers=True,
        log_level=settings.LOG_LEVEL,
        server_header=False,
    )
