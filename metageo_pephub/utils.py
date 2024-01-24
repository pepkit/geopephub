import time
import signal
import geofetch
from typing import Dict
import peppy
from pepdbagent import PEPDatabaseAgent
import os
from dotenv import load_dotenv
from const import (
    DEFAULT_POSTGRES_USER,
    DEFAULT_POSTGRES_PASSWORD,
    DEFAULT_POSTGRES_HOST,
    DEFAULT_POSTGRES_DB,
    DEFAULT_POSTGRES_PORT,
)
from db_utils import BaseEngine

GSE_LINK = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={}"

load_dotenv()


class FunctionTimeoutError(Exception):
    """
    Time out error when function is running too long
    """

    def __init__(self, reason: str = ""):
        """
        Optionally provide explanation for exceptional condition.
        :param str reason: some context or perhaps just a value that
            could not be interpreted as an accession
        """
        super(FunctionTimeoutError, self).__init__(reason)


# TODO: consider to change it to: https://github.com/pnpnpn/timeout-decorator
def timeout(seconds_before_timeout=60):
    def decorate(f):
        def handler(signum, frame):
            raise FunctionTimeoutError("Geofetch running time is too long. TimeOut.")

        def new_f(*args, **kwargs):
            old = signal.signal(signal.SIGALRM, handler)
            old_time_left = signal.alarm(seconds_before_timeout)
            if (
                0 < old_time_left < seconds_before_timeout
            ):  # never lengthen existing timer
                signal.alarm(old_time_left)
            start_time = time.time()
            try:
                result = f(*args, **kwargs)
            finally:
                if old_time_left > 0:  # deduct f's run time from the saved timer
                    old_time_left -= time.time() - start_time
                signal.signal(signal.SIGALRM, old)
                signal.alarm(old_time_left)
            return result

        return new_f

    return decorate


@timeout(240)
def run_geofetch(
    gse: str, geofetcher_obj: geofetch.Geofetcher = None
) -> Dict[str, peppy.Project]:
    """
    geofetch wrapped in function
    :param gse: Projects GSE
    :param geofetcher_obj: object of Geofetcher class
    :return: dict of peppys
    """
    if not geofetcher_obj:
        geofetcher_obj = geofetch.Geofetcher(
            const_limit_discard=1500,
            attr_limit_truncate=1000,
            const_limit_project=200,
        )
    project_dict = geofetcher_obj.get_projects(gse)
    return project_dict


def add_link_to_description(gse: str, pep: peppy.Project) -> peppy.Project:
    """
    Add geo project link to the project description (Markdown format)

    :param gse: GSE id from GEO (e.g. GSE123456)
    :param pep: peppy project
    :return: peppy project
    """
    new_description = (
        f"Data from [GEO {gse}]({GSE_LINK.format(gse)})\n{pep.description}"
    )
    pep.description = new_description
    pep.name = gse.lower()
    return pep


def get_agent() -> PEPDatabaseAgent:
    """
    Get PEPDatabaseAgent object
    :return: PEPDatabaseAgent
    """
    return PEPDatabaseAgent(
        user=os.environ.get("POSTGRES_USER") or DEFAULT_POSTGRES_USER,
        password=os.environ.get("POSTGRES_PASSWORD") or DEFAULT_POSTGRES_PASSWORD,
        host=os.environ.get("POSTGRES_HOST") or DEFAULT_POSTGRES_HOST,
        database=os.environ.get("POSTGRES_DB") or DEFAULT_POSTGRES_DB,
        port=os.environ.get("POSTGRES_PORT") or DEFAULT_POSTGRES_PORT,
    )


def get_base_db_engine() -> BaseEngine:
    """
    Get BaseEngine object
    :return: BaseEngine
    """
    return BaseEngine(
        user=os.environ.get("POSTGRES_USER") or DEFAULT_POSTGRES_USER,
        password=os.environ.get("POSTGRES_PASSWORD") or DEFAULT_POSTGRES_PASSWORD,
        host=os.environ.get("POSTGRES_HOST") or DEFAULT_POSTGRES_HOST,
        database=os.environ.get("POSTGRES_DB") or DEFAULT_POSTGRES_DB,
        port=os.environ.get("POSTGRES_PORT") or DEFAULT_POSTGRES_PORT,
    )

