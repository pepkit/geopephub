import time
import signal
import geofetch
from typing import Dict
import peppy
from pepdbagent import PEPDatabaseAgent
import os
from dotenv import load_dotenv
from functools import wraps
import logging
import tarfile
import datetime

from geopephub.const import (
    DEFAULT_POSTGRES_USER,
    DEFAULT_POSTGRES_PASSWORD,
    DEFAULT_POSTGRES_HOST,
    DEFAULT_POSTGRES_DB,
    DEFAULT_POSTGRES_PORT,
    __name__,
)
from geopephub.db_utils import BaseEngine

GSE_LINK = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={}"

load_dotenv()

_LOGGER = logging.getLogger(__name__)


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


def calculate_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time

        hours, remainder = divmod(execution_time, 3600)
        minutes, seconds = divmod(remainder, 60)

        print(
            f"Function '{func.__name__}' executed in {int(hours)} hours, {int(minutes)} minutes, and {seconds:.2f} seconds"
        )

        return result

    return wrapper


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


def create_gse_sub_name(name: str) -> str:
    """
    Create gse subfolder name. e.g.
        gse123456 -> gse123nnn
        gse123 -> gsennn
        gse1234-> gse1nnn
        gse1 -> gsennn

    :param name: gse name
    :return: gse subfolder name
    """

    len_name = len(name)

    if len_name <= 6:
        return """gsennn"""
    else:
        # return name[:6] + "n" * (len_name - 6)
        return name[:-3] + "n" * 3


def tar_folder(folder_path: str, tar_name: str) -> str:
    """Tar a folder

    :param folder_path: Folder to tar
    :param tar_name: Name of the tar file
    :return: file name of the tar file
    """
    # tar_type = "w:gz"
    tar_type = "w"
    tar_name = f"{tar_name}.tar"

    with tarfile.open(tar_name, tar_type) as tar:
        tar.add(folder_path, arcname=os.path.basename(folder_path))
    return tar_name


def date_today(separator: str = "_") -> str:
    """
    Get today's date in the format 'YYYY_MM_DD'

    :param separator: separator for the date

    :return: str
    """

    today_date = datetime.datetime.today()
    return today_date.strftime(f"%Y{separator}%m{separator}%d")
