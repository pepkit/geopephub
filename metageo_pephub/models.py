from typing import Optional

from sqlmodel import SQLModel, Field
from pydantic import validator
import datetime

from const import STATUS_TABLE_NAME, STATUS_OPTIONS, CYCLE_TABLE_NAME

# LOG Stages:
# 0 - list of GSEs was fetched
# 1 - start processing particular GSE
# 2 - Geofetcher downloaded project
# 3 - Finished


class CycleModel(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    status_date: Optional[datetime.datetime] = datetime.datetime.now()
    target: str
    status: str
    start_period: Optional[str]
    end_period: Optional[str]
    number_of_projects: Optional[int] = 0
    number_of_successes: Optional[int] = 0
    number_of_failures: Optional[int] = 0

    __tablename__ = CYCLE_TABLE_NAME

    @validator("status")
    def status_checker(cls, value):
        if value not in STATUS_OPTIONS:
            raise ValueError("Incorrect status value")

        return value


class StatusModel(SQLModel, table=False):
    id: Optional[int] = Field(default=None, primary_key=True)
    gse: str
    target: str  # TODO: remove it
    registry_path: Optional[str]
    upload_cycle_id: Optional[int] = Field(
        default=None, foreign_key=f"{CYCLE_TABLE_NAME}.id"
    )
    log_stage: int
    status: str
    status_info: Optional[str]
    info: Optional[str]

    @validator("status")
    def status_checker(cls, value):
        if value not in STATUS_OPTIONS:
            raise ValueError("Incorrect status value")

        return value


class StatusModelSQL(StatusModel, table=True):
    __tablename__ = STATUS_TABLE_NAME
