from typing import Optional

from sqlmodel import Field, SQLModel, create_engine, select
import datetime


class LogModel(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    gse: str
    registry_path: Optional[str]
    date: Optional[datetime.datetime] = datetime.datetime.now()
    log_stage: int
    status: str
    status_info: Optional[str]
    info: Optional[str]

    __tablename__ = "geo_log"


# LOG Stages:
# 0 - list of GSEs was fetched
# 1 - start processing particular GSE
# 2 - Geofetcher downloaded project
# 3 - Finished

# Statuses:
# queued
# processing
# success
# failure
# warning
