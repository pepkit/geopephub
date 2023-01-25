import geofetch
import pepdbagent
import argparse
import sys
from typing import NoReturn
import datetime
import logmuse
import coloredlogs
from update_status import UploadLogger
from models import StatusModel
from utils import run_geofetch

import peppy

from const import LAST_UPDATE_DATES


_LOGGER = logmuse.init_logger("geo_to_pephub")
coloredlogs.install(
    logger=_LOGGER,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] %(message)s",
)


def metageo_main(
    target: str,
    db: str,
    host: str,
    user: str,
    password: str,
    function: str,
    gse: str = None,
    period: int = 1,
    port: int = 5432,
    tag: str = None,
):
    """
    :param target: Namespace of the projects [bedbase, geo]
    :param tag: Tag of the projects
    :param db: db name of the db
    :param host: host of the db
    :param function: [should be in ["q_insert", "q_upload", "insert_one", "create_status_table"]]
    :param user: Username
    :param password: Password
    :param port: port of the database
    :return: NoReturn
    """
    if function == "q_insert":
        add_to_queue(
            db=db,
            host=host,
            user=user,
            password=password,
            target=target,
            tag=tag,
            period=period
        )
    elif function == "q_upload":
        upload_queued_projects(
            db=db,
            host=host,
            user=user,
            password=password,
            target=target,
            tag=tag,
        )
    elif function == "insert_one":
        ...
    elif function == "create_status_table":
        connection = UploadLogger(
            database=db,
            host=host,
            user=user,
            password=password,
            port=port,
        )
        connection.create_table()
    else:
        raise Exception("Error in function calling. "
                        """Function should be one from the list
                         ["q_insert", "q_upload", "insert_one", "create_status_table"]""")



def add_to_queue(
    db: str,
    host: str,
    user: str,
    password: str,
    target: str,
    tag: str,
    port: int = 5432,
    period: int = LAST_UPDATE_DATES
) -> NoReturn:
    """

    :param target: Namespace of the projects (bedbase, geo)
    :param tag: Tag of the projects
    :param db: db name of the db
    :param host: host of the db
    :param user: Username
    :param password: Password
    :param port: port of the database
    :return: NoReturn
    """
    log_connection = UploadLogger(
        host=host, port=port, database=db, user=user, password=password
    )

    time_now = datetime.datetime.now()

    _LOGGER.info(f"Time now: {time_now}")
    _LOGGER.info(f"geofetch version: {geofetch.__version__}")
    _LOGGER.info(f"pepdbagent version: {pepdbagent.__version__}")
    _LOGGER.info(f"peppy version: {peppy.__version__}")

    if target == "bedbase":
        gse_list = geofetch.Finder(filters="(bed)").get_gse_by_day_count(period)
    elif target == "geo":
        gse_list = geofetch.Finder().get_gse_by_day_count(period)
    else:
        raise Exception(f"Error in target: {target}")

    total_nb = len(gse_list)

    _LOGGER.info(f"Number of projects that will be processed: {total_nb}")

    log_model_dict = {}

    for gse in gse_list:
        model_l = StatusModel(
            target=target,
            gse=gse,
            log_stage=0,
            status="queued",
            registry_path=f"{target}/{gse}:{tag}",
        )
        model_l = log_connection.upload_log(model_l)
        log_model_dict[gse] = model_l
        _LOGGER.info(f"GSE: '{gse}' was added to the queue! Target: {target}")

    _LOGGER.info(f"================== Finished ==================")
    _LOGGER.info(f"\033[32mAfter run report: Added {total_nb} projects\033[0m")


def upload_queued_projects(
    target: str,
    db: str,
    host: str,
    user: str,
    password: str,
    port: int = 5432,
    tag: str = None,
) -> NoReturn:

    # LOG info
    time_now = datetime.datetime.now()
    _LOGGER.info(f"Time now: {time_now}")
    _LOGGER.info(f"geofetch version: {geofetch.__version__}")
    _LOGGER.info(f"pepdbagent version: {pepdbagent.__version__}")
    _LOGGER.info(f"peppy version: {peppy.__version__}")

    agent = pepdbagent.PEPDatabaseAgent(
        host=host, port=port, database=db, user=user, password=password
    )
    log_connection = UploadLogger(
        host=host, port=port, database=db, user=user, password=password
    )
    gse_log_list = log_connection.get_queued_project(target=target)

    log_model_dict = {}
    for gse_log_item in gse_log_list:
        log_model_dict[gse_log_item.gse] = gse_log_item

    _upload_gse_project(agent, log_connection, log_model_dict, target, tag)


def _upload_gse_project(
    agent, log_connection, log_model_dict, target, tag=None,
) -> NoReturn:
    """
    Get, upload to PEPhub and load log to database of GSE project
    :param agent: pepdbagent object connected to db
    :param log_connection: UploadLogger object connected to db
    :param log_model_dict: dictionary with StatusModel (seq table model), where keys are GSEs
    :param target: namespace where project's should be added
    :return: NoReturn
    """
    if target == "bedbase":
        geofetcher_obj = geofetch.Geofetcher(filter="\.(bed|bigBed|narrowPeak|broadPeak)\.",
                                             filter_size="25MB",
                                             data_source="samples",
                                             processed=True,
                                             )
    else:
        geofetcher_obj = geofetch.Geofetcher()
    total_nb = len(log_model_dict.keys())
    process_nb = 0
    _LOGGER.info(f"Number of projects that will be processed: {total_nb}")
    status_dict = {
        "total": total_nb,
        "success": 0,
        "failure": 0,
        "warning": 0,
    }
    for gse in log_model_dict.keys():

        gse_log = log_model_dict[gse]

        gse_log.status = "processing"
        gse_log.log_stage = 1
        log_connection.upload_log(gse_log)

        process_nb += 1
        _LOGGER.info(f"\033[0;33mProcessing GSE: {gse}. {process_nb}/{total_nb}\033[0m")

        try:
            gse_log.status_info = "geofetcher"
            gse_log.log_stage = 2
            project_dict = run_geofetch(gse, geofetcher_obj)
            _LOGGER.info(f"Project has been downloaded using geofetch")
        except Exception as err:
            gse_log.status = "failure"
            gse_log.info = str(err)
            log_connection.upload_log(gse_log)
            status_dict["failure"] += 1
            continue

        if len(list(project_dict.keys())) == 0:
            gse_log.status = "warning"
            gse_log.info = "No data was fetched from GEO, check if project has any data"
            gse_log.status_info = "geofetcher"
            log_connection.upload_log(gse_log)
            status_dict["warning"] += 1
            continue

        for prj_name in project_dict:
            prj_name_list = prj_name.split("_")
            pep_name = prj_name_list[0]
            pep_tag = prj_name_list[1]

            gse_log.registry_path = f"{target}/{pep_name}:{pep_tag}"
            log_connection.upload_log(gse_log)

            _LOGGER.info(
                f"Namespace = {target} ; Project_name = {pep_name} ; Tag = {pep_tag}"
            )
            gse_log.log_stage = 3
            gse_log.status_info = "pepdbagent"
            try:

                agent.project.create(
                    project=project_dict[prj_name],
                    namespace=target,
                    name=pep_name,
                    tag=pep_tag,
                    overwrite=True,
                )
                gse_log.status = "success"
                gse_log.info = ""
                log_connection.upload_log(gse_log)

                status_dict["success"] += 1
            except Exception as err:
                gse_log.status = "failure"
                gse_log.info = str(err)
                log_connection.upload_log(gse_log)

                status_dict["failure"] += 1

    _LOGGER.info(f"================== Finished ==================")
    _LOGGER.info(f"\033[32mAfter run report: {status_dict}\033[0m")