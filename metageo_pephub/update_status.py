from sqlmodel import SQLModel, create_engine, Session, select
from models import StatusModel, StatusModelSQL
import datetime
from typing import List, Union

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

    def upload_log(self, response: Union[StatusModel, StatusModelSQL]) -> StatusModel:
        """
        Update or upload
        :param response:
        :return: Log Model
        """
        _LOGGER.info("Uploading or updating project logs")
        response.date = datetime.datetime.now()
        if isinstance(response, StatusModelSQL):
            sql_response = response
        else:
            sql_response = StatusModelSQL(**response.dict())
        with Session(self.engine) as session:
            session.add(sql_response)
            session.commit()
            session.refresh(sql_response)
        _LOGGER.info("Information was uploaded")
        return sql_response

    def get_queued_project(self, target) -> List[StatusModel]:
        """
        Get projects, that have status: "queued"
        :param target: namespece where project is a target
        :return: list of StatusModel
        """
        with Session(self.engine) as session:
            _LOGGER.info("Uploading or updating project logs")
            statement = select(StatusModelSQL).where(StatusModelSQL.status == "queued").where(StatusModelSQL.target == target)
            results = session.exec(statement)
            heroes = results.all()
            return heroes
