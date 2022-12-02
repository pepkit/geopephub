from sqlmodel import SQLModel, create_engine, Session, select
from models import LogModel
import datetime
from typing import List

import logmuse
import coloredlogs

_LOGGER = logmuse.init_logger("log_uploader")
coloredlogs.install(
    logger=_LOGGER,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] %(message)s",
)


class UploadLogger:
    def __init__(
        self,
        host="localhost",
        port=5432,
        database="pep-db",
        user=None,
        password=None,
        dsn=None,
    ):
        """
        Initialize connection to the pep_db database. You can use The basic connection parameters
        or libpq connection string.
        :param host: database server address e.g., localhost or an IP address.
        :param port: the port number that defaults to 5432 if it is not provided.
        :param database: the name of the database that you want to connect.
        :param user: the username used to authenticate.
        :param password: password used to authenticate.
        :param dsn: libpq connection string using the dsn parameter
        (e.g. "localhost://username:password@pdp_db:5432")
        """
        if not dsn:
            dsn = f"postgresql://{user}:{password}@{host}:{port}/{database}"

        self.engine = create_engine(dsn, echo=False)

        _LOGGER.info("engine was created")

    def create_table(self):
        SQLModel.metadata.create_all(self.engine)
        _LOGGER.info("Table was created")

    def upload_log(self, response: LogModel) -> LogModel:
        """
        Update or upload
        :param response:
        :return: Log Model
        """
        _LOGGER.info("Uploading or updating project logs")
        response.date = datetime.datetime.now()
        with Session(self.engine) as session:
            session.add(response)
            session.commit()
            session.refresh(response)
        _LOGGER.info("Information was uploaded")
        return response

    def get_queued_project(self) -> List[LogModel]:
        """
        Get projects, that have status: "queued"
        :return: list of LogModel
        """
        with Session(self.engine) as session:
            _LOGGER.info("Uploading or updating project logs")
            statement = select(LogModel).where(LogModel.status == "queued")
            results = session.exec(statement)
            heroes = results.all()
            return heroes
