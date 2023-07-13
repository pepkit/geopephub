from typing import Optional

from pydantic import BaseModel
from pydantic import validator
import datetime

from const import STATUS_TABLE_NAME, STATUS_OPTIONS, CYCLE_TABLE_NAME

# LOG Stages:
# 0 - list of GSEs was fetched
# 1 - start processing particular GSE
# 2 - Geofetcher downloaded project
# 3 - Finished


class CycleModel(BaseModel):
    id: Optional[int]
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


class StatusModel(BaseModel):
    id: Optional[int]
    gse: str
    target: Optional[str]  # TODO: remove it
    registry_path: Optional[str]
    upload_cycle_id: Optional[int]
    log_stage: int
    status: str
    status_info: Optional[str]
    info: Optional[str]
