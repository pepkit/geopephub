from sqlmodel import SQLModel, create_engine, Session, select
from models import StatusModel, StatusModelSQL, CycleModel
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


class UploadStatusConnection:
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

    def upload_gse_log(
        self, response: Union[StatusModel, StatusModelSQL]
    ) -> StatusModel:
        """
        Update or upload
        :param response:
        :return: Log Model
        """
        _LOGGER.info("Uploading or updating project logs")
        # response.date = datetime.datetime.now()
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

    def get_queued_project(self, cycle_id: int) -> List[StatusModel]:
        """
        Get projects, that have status: "queued"
        :param cycle_id: cycle id in which project was uploaded
        :return: list of StatusModel
        """
        with Session(self.engine) as session:
            _LOGGER.info("Getting queued projects")
            statement = (
                select(StatusModelSQL)
                .where(StatusModelSQL.status == "queued")
                .where(StatusModelSQL.upload_cycle_id == cycle_id)
            )
            results = session.exec(statement)
            heroes = results.all()
            return heroes

    def get_failed_project(self, cycle_id: int) -> List[StatusModel]:
        """
        Get projects, that have status: "queued"
        :param cycle_id: cycle id in which project was uploaded
        :return: list of StatusModel
        """
        with Session(self.engine) as session:
            _LOGGER.info("Getting queued projects")
            statement = (
                select(StatusModelSQL)
                .where(StatusModelSQL.status != "success")
                .where(StatusModelSQL.upload_cycle_id == cycle_id)
            )
            results = session.exec(statement)
            heroes = results.all()
            return heroes

    def update_upload_cycle(self, cycle_model: CycleModel) -> CycleModel:
        """
        :param cycle_model: pydantic cycle database model with necessary data
        :return: pydantic cycle database model with necessary data that was inserted to db
        """
        _LOGGER.info("Uploading or updating project logs")
        cycle_model.status_date = datetime.datetime.now()
        with Session(self.engine) as session:
            session.add(cycle_model)
            session.commit()
            session.refresh(cycle_model)
        _LOGGER.info("Information was uploaded")
        return cycle_model

    def get_queued_cycle(self, target: str = None) -> List[CycleModel]:
        """
        Get list of queued_cycle
        :param target: target(namespace) of the cycle
        :return: list of queued cycles
        """
        with Session(self.engine) as session:
            _LOGGER.info("Getting queued cycles")
            statement = select(CycleModel).where(CycleModel.status == "queued")
            if target:
                statement = statement.where(CycleModel.target == target)
            results = session.exec(statement)
            queued_cycle = results.all()
            return queued_cycle

    def get_number_samples_success(self, cycle_id: int):
        """
        Get total number of samples that were uploaded successfully
        :param cycle_id: cycle_id
        :return: number of sucesses
        """
        with Session(self.engine) as session:
            _LOGGER.info("Getting number of successes")
            statement = (
                select(StatusModelSQL)
                .where(StatusModelSQL.status == "success")
                .where(StatusModelSQL.upload_cycle_id == cycle_id)
            )
            results = session.exec(statement)
            heroes = results.all()
            return len(heroes)

    def get_number_samples_failures(self, cycle_id: int):
        """
        Get total number of samples that failed to upload
        :param cycle_id: cycle_id
        :return: number of failures
        """
        with Session(self.engine) as session:
            _LOGGER.info("Getting number of failures")
            statement = (
                select(StatusModelSQL)
                .where(StatusModelSQL.status == "failure")
                .where(StatusModelSQL.upload_cycle_id == cycle_id)
            )
            results = session.exec(statement)
            heroes = results.all()
            return len(heroes)

    def get_number_samples_warnings(self, cycle_id: int):
        """
        Get total number of samples that with warnings
        :param cycle_id: cycle_id
        :return: number of failures
        """
        with Session(self.engine) as session:
            _LOGGER.info("Getting number of projects with warning")
            statement = (
                select(StatusModelSQL)
                .where(StatusModelSQL.status == "warning")
                .where(StatusModelSQL.upload_cycle_id == cycle_id)
            )
            results = session.exec(statement)
            heroes = results.all()
            return len(heroes)

    def was_run_successful(
        self,
        start_period: str,
        end_period: str,
        target: str = None,
    ) -> CycleModel:
        """
        Check if run was successful
        :param start_period: start date of cycle [e.g. 2023/02/07]
        :param end_period: end date of cycle [e.g. 2023/02/08]
        :param target: target namespace
        :return: CycleModel
        """
        _LOGGER.info(
            f"checking success of target: {target} [{start_period}-{end_period}]"
        )
        with Session(self.engine) as session:
            _LOGGER.info("Getting queued cycles")
            statement = (
                select(CycleModel)
                .where(CycleModel.start_period == start_period)
                .where(CycleModel.end_period == end_period)
            )
            if target:
                statement = statement.where(CycleModel.target == target)
            results = session.exec(statement)
            queued_cycle = results.all()
            return queued_cycle[0]
