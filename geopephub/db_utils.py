from typing import Optional, List

from sqlalchemy import (
    BigInteger,
    TIMESTAMP,
    select,
    update,
)
from sqlalchemy import inspect
from sqlalchemy.engine import URL, create_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    Session,
)

import datetime

import logging

from geopephub.models import StatusModel, CycleModel
from geopephub.const import (
    STATUS_TABLE_NAME,
    CYCLE_TABLE_NAME,
    POSTGRES_DIALECT,
    __name__,
)

_LOGGER = logging.getLogger(__name__)


# LOG Stages:
# 0 - list of GSEs was fetched
# 1 - start processing particular GSE
# 2 - Geofetcher downloaded project
# 3 - Finished

_LOGGER.info("engine was created")


class Base(DeclarativeBase):
    type_annotation_map = {datetime.datetime: TIMESTAMP(timezone=True)}


class BIGSERIAL(BigInteger):
    pass


@compiles(BIGSERIAL)
def compile_bigserial_pg(type_, compiler, **kw):
    return "BIGSERIAL"


def deliver_date(context):
    return datetime.datetime.now(datetime.timezone.utc)


class CycleModelSA(Base):
    __tablename__ = CYCLE_TABLE_NAME

    id: Mapped[int] = mapped_column(primary_key=True)
    status_date: Mapped[Optional[datetime.datetime]] = mapped_column(
        default=deliver_date, onupdate=deliver_date
    )
    target: Mapped[str]
    status: Mapped[str]
    start_period: Mapped[Optional[str]]
    end_period: Mapped[Optional[str]]
    number_of_projects: Mapped[Optional[int]] = mapped_column(default=0)
    number_of_successes: Mapped[Optional[int]] = mapped_column(default=0)
    number_of_failures: Mapped[Optional[int]] = mapped_column(default=0)

    # project_model_mapping: Mapped[List["ProjectModelSA"]] = relationship(back_populates="cycle_model_mapping")


class ProjectModelSA(Base):
    __tablename__ = STATUS_TABLE_NAME

    id: Mapped[int] = mapped_column(primary_key=True)
    gse: Mapped[str]
    registry_path: Mapped[Optional[str]]
    upload_cycle_id: Mapped[int]
    target: Mapped[str]
    # cycle_model_mapping: Mapped["CycleModelSA"] = relationship(back_populates="project_model_mapping")
    log_stage: Mapped[int]
    status: Mapped[str]
    status_info: Mapped[Optional[str]]
    info: Mapped[Optional[str]]


class BaseEngine:
    """
    A class with base methods, that are used in several classes. e.g. fetch_one or fetch_all
    """

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 5432,
        database: str = "pep-db",
        user: str = None,
        password: str = None,
        drivername: str = POSTGRES_DIALECT,
        dsn: str = None,
        echo: bool = False,
    ):
        """
        Initialize connection to the pep_db database. You can use The basic connection parameters
        or libpq connection string.
        :param host: database server address e.g., localhost or an IP address.
        :param port: the port number that defaults to 5432 if it is not provided.
        :param database: the name of the database that you want to connect.
        :param user: the username used to authenticate.
        :param password: password used to authenticate.
        :param drivername: driver used in
        :param dsn: libpq connection string using the dsn parameter
        (e.g. 'postgresql://user_name:password@host_name:port/db_name')
        """
        if not dsn:
            dsn = URL.create(
                host=host,
                port=port,
                database=database,
                username=user,
                password=password,
                drivername=drivername,
            )

        self._engine = create_engine(dsn, echo=echo)
        self.create_schema(self._engine)

    def create_schema(self, engine=None):
        """
        Create sql schema in the database.

        :param engine: sqlalchemy engine [Default: None]
        :return: None
        """
        if not engine:
            engine = self._engine
        Base.metadata.create_all(engine)
        return None

    def upload_project_log(self, project_status_model: StatusModel) -> StatusModel:
        """
        Update or upload project (gse) status

        :param response:
        :return: Log Model
        """
        _LOGGER.info("Uploading or updating project logs")
        # response.date = datetime.datetime.now()

        if project_status_model.id:
            if self.project_status_exists(project_status_model.id):
                statement = (
                    update(ProjectModelSA)
                    .where(ProjectModelSA.id == project_status_model.id)
                    .values(
                        project_status_model.model_dump(
                            exclude_unset=True, exclude_none=True, exclude={"id"}
                        )
                    )
                )
                with Session(self._engine) as session:
                    session.execute(statement)
                    session.commit()

                return project_status_model
            else:
                raise ValueError(
                    f"Cycle {project_status_model.id} does not exists in the database"
                )

        else:
            new_projects_status = ProjectModelSA(
                **project_status_model.model_dump(
                    exclude_unset=True, exclude_none=True, exclude={"id"}
                )
            )

            with Session(self._engine) as session:
                session.add(new_projects_status)
                session.commit()
                new_cycle_id = new_projects_status.id

            project_status_model.id = new_cycle_id
            _LOGGER.info("Information was uploaded")
            return project_status_model

    # This function should be good now
    def get_queued_project(self, cycle_id: int) -> List[StatusModel]:
        """
        Get projects, that have status: "queued"
        :param cycle_id: cycle id in which project was uploaded
        :return: list of StatusModel
        """
        list_of_status = []
        with Session(self._engine) as session:
            _LOGGER.info("Getting queued projects")
            statement = (
                select(ProjectModelSA)
                .where(ProjectModelSA.status == "queued")
                .where(ProjectModelSA.upload_cycle_id == cycle_id)
            )
            results = session.scalars(statement)
            for result in results:
                list_of_status.append(StatusModel(**self.sa_object_as_dict(result)))

            return list_of_status

    # This function should be good now
    def get_failed_project(self, cycle_id: int) -> List[StatusModel]:
        """
        Get projects, that have status: "queued"
        :param cycle_id: cycle id in which project was uploaded
        :return: list of StatusModel
        """
        list_of_status = []
        with Session(self._engine) as session:
            _LOGGER.info("Getting queued projects")
            statement = (
                select(ProjectModelSA)
                .where(ProjectModelSA.status != "success")
                .where(ProjectModelSA.upload_cycle_id == cycle_id)
            )
            results = session.scalars(statement).all()
            for result in results:
                list_of_status.append(StatusModel(**self.sa_object_as_dict(result)))

            return list_of_status

    # This function should be good now
    def update_upload_cycle(self, cycle_model: CycleModel) -> CycleModel:
        """
        :param cycle_model: pydantic cycle database model with necessary data
        :return: pydantic cycle database model with necessary data that was inserted to db
        """
        _LOGGER.info("Uploading or updating project logs")
        cycle_model.status_date = datetime.datetime.now()

        if cycle_model.id:
            if self.cycle_exists(cycle_model.id):
                cycle_model_dict = cycle_model.model_dump(
                    exclude_unset=True, exclude_none=True, exclude={"id"}
                )
                statement = (
                    update(CycleModelSA)
                    .where(CycleModelSA.id == cycle_model.id)
                    .values(**cycle_model_dict)
                )
                with Session(self._engine) as session:
                    session.execute(statement)
                    session.commit()

                return cycle_model
            else:
                raise ValueError(
                    f"Cycle {cycle_model.id} does not exists in the database"
                )

        else:
            new_cycle = CycleModelSA(
                **cycle_model.model_dump(exclude_unset=True, exclude_none=True)
            )

            with Session(self._engine) as session:
                session.add(new_cycle)
                session.commit()
                new_cycle_id = new_cycle.id

            cycle_model.id = new_cycle_id
            _LOGGER.info("Information was uploaded")
            return cycle_model

    def get_queued_cycle(self, target: str = None) -> List[CycleModel]:
        """
        Get list of queued_cycle
        :param target: target(namespace) of the cycle
        :return: list of queued cycles
        """
        with Session(self._engine) as session:
            _LOGGER.info("Getting queued cycles")
            statement = select(CycleModelSA).where(CycleModelSA.status == "queued")
            if target:
                statement = statement.where(CycleModelSA.target == target)
            results = session.scalars(statement)
            queued_cycles = results.all()
            queued_cycles_py_model = []
            for queued_cycle in queued_cycles:
                queued_cycles_py_model.append(
                    CycleModel(**self.sa_object_as_dict(queued_cycle))
                )
            return queued_cycles_py_model

    def get_number_samples_success(self, cycle_id: int):
        """
        Get total number of samples that were uploaded successfully
        :param cycle_id: cycle_id
        :return: number of successes
        """
        with Session(self._engine) as session:
            _LOGGER.info("Getting number of successes")
            statement = (
                select(ProjectModelSA)
                .where(ProjectModelSA.status == "success")
                .where(ProjectModelSA.upload_cycle_id == cycle_id)
            )
            results = session.execute(statement)
            heroes = results.all()
            return len(heroes)

    def get_number_samples_failures(self, cycle_id: int):
        """
        Get total number of samples that failed to upload
        :param cycle_id: cycle_id
        :return: number of failures
        """
        with Session(self._engine) as session:
            _LOGGER.info("Getting number of failures")

            statement = (
                select(ProjectModelSA)
                .where(ProjectModelSA.status == "failure")
                .where(ProjectModelSA.upload_cycle_id == cycle_id)
            )
            results = session.execute(statement)
            heroes = results.all()
            return len(heroes)

    def get_number_samples_warnings(self, cycle_id: int):
        """
        Get total number of samples that with warnings
        :param cycle_id: cycle_id
        :return: number of failures
        """
        with Session(self._engine) as session:
            _LOGGER.info("Getting number of projects with warning")
            statement = (
                select(ProjectModelSA)
                .where(ProjectModelSA.status == "warning")
                .where(ProjectModelSA.upload_cycle_id == cycle_id)
            )
            results = session.execute(statement)
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
        :return: CycleModelSA
        """
        _LOGGER.info(
            f"checking success of target: {target} [{start_period}-{end_period}]"
        )
        with Session(self._engine) as session:
            _LOGGER.info("Getting queued cycles")
            statement = (
                select(CycleModelSA)
                .where(CycleModelSA.start_period == start_period)
                .where(CycleModelSA.end_period == end_period)
            )
            if target:
                statement = statement.where(CycleModelSA.target == target)
            results = session.scalar(statement)
            if results:
                results_dict = self.sa_object_as_dict(results)
                return CycleModel(**results_dict)
            return None

    def cycle_exists(self, cycle_id):
        with Session(self._engine) as session:
            found_prj = session.execute(
                select(CycleModelSA).where(CycleModelSA.id == cycle_id)
            ).all()
            if len(found_prj) > 0:
                return True
            else:
                return False

    def project_status_exists(self, project_status_id):
        with Session(self._engine) as session:
            found_prj = session.execute(
                select(ProjectModelSA).where(ProjectModelSA.id == project_status_id)
            ).all()
            if len(found_prj) > 0:
                return True
            else:
                return False

    def sa_object_as_dict(self, obj):
        return {c.key: getattr(obj, c.key) for c in inspect(obj).mapper.column_attrs}
