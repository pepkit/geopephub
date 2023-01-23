# 1. Get project that is queued
# 2. Get GSE
# 3. Run uploader
# 4. update log
# 5. do with every


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


_LOGGER = logmuse.init_logger("run_queued")
coloredlogs.install(
    logger=_LOGGER,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] %(message)s",
)


def upload_queued_projects(
    namespace: str,
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
    gse_log_list = log_connection.get_queued_project()

    log_model_dict = {}
    for gse_log_item in gse_log_list:
        log_model_dict[gse_log_item.gse] = gse_log_item

    _upload_gse_project(agent, log_connection, log_model_dict, namespace, tag)


def _upload_gse_project(
    agent, log_connection, log_model_dict, namespace, tag=None,
) -> NoReturn:
    """
    Get, upload to PEPhub and load log to database of GSE project
    :param agent: pepdbagent object connected to db
    :param log_connection: UploadLogger object connected to db
    :param log_model_dict: dictionary with StatusModel (seq table model), where keys are GSEs
    :param namespace: namespace where project's should be added
    :return: NoReturn
    """
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
            gse_log.log_stage = 2
            project_dict = run_geofetch(gse, geofetcher_obj)
            _LOGGER.info(f"Project has been downloaded using geofetch")
        except Exception as err:
            gse_log.status = "failure"
            gse_log.info = str(err)
            gse_log.status_info = "geofetcher"
            log_connection.upload_log(gse_log)
            status_dict["failure"] += 1
            continue

        for prj_name in project_dict:
            prj_name_list = prj_name.split("_")
            pep_name = prj_name_list[0]
            pep_tag = prj_name_list[1]

            gse_log.registry_path = f"{namespace}/{pep_name}:{pep_tag}"
            log_connection.upload_log(gse_log)

            _LOGGER.info(
                f"Namespace = {namespace} ; Project_name = {pep_name} ; Tag = {pep_tag}"
            )
            gse_log.log_stage = 3
            gse_log.status_info = "pepdbagent"
            try:

                agent.project.create(
                    project=project_dict[prj_name],
                    namespace=namespace,
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


def _parse_cmdl(cmdl):
    parser = argparse.ArgumentParser(
        description="Automatic GEO projects uploader to PEPhub",
    )
    parser.add_argument(
        "--namespace",
        required=True,
        help="namespace where project should be uploaded",
    )
    parser.add_argument(
        "--tag",
        required=False,
        default="default",
        help="tag of the project",
    )
    parser.add_argument(
        "--host",
        required=True,
        help="Host of the db",
    )
    parser.add_argument(
        "--db",
        required=True,
        help="db_name of the db",
    )
    parser.add_argument(
        "--user",
        required=True,
        help="Username of the db",
    )
    parser.add_argument(
        "--password",
        required=True,
        help="password of the db",
    )
    return parser.parse_args(cmdl)


def main():
    args = _parse_cmdl(sys.argv[1:])
    args_dict = vars(args)

    upload_queued_projects(
        namespace=args_dict["namespace"],
        tag=args_dict["tag"],
        db=args_dict["db"],
        host=args_dict["host"],
        user=args_dict["user"],
        password=args_dict["password"],
    )


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        print("Pipeline aborted.")
        sys.exit(1)


# upload_queued_projects(
#     namespace="new",
#     tag="def",
#     db="pep-db",
#     host="localhost",
#     user="postgres",
#     password="docker",
# )
