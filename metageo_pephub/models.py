from typing import Optional

from sqlmodel import SQLModel, Field
from pydantic import validator
import datetime

from const import LOG_TABLE_NAME, STATUS_OPTIONS

# LOG Stages:
# 0 - list of GSEs was fetched
# 1 - start processing particular GSE
# 2 - Geofetcher downloaded project
# 3 - Finished


class StatusModel(SQLModel, table=False):
    id: Optional[int] = Field(default=None, primary_key=True)
    gse: str
    target: str
    registry_path: Optional[str]
    date: Optional[datetime.datetime] = datetime.datetime.now()
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
    __tablename__ = LOG_TABLE_NAME
