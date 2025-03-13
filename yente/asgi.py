from yente.app import create_app
from yente.logs import configure_logging


app = create_app()
configure_logging()
