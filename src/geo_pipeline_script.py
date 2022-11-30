import geofetch
import pepdbagent
import argparse
import sys
from typing import NoReturn
import datetime
import logmuse
import coloredlogs
from log_uploader import UploadLogger
from models import LogModel

import peppy


_LOGGER = logmuse.init_logger("geo_to_pephub")
coloredlogs.install(
    logger=_LOGGER,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] %(message)s",
)


def upload_geo_projects(
    namespace: str,
    db: str,
    host: str,
    user: str,
    password: str,
    port: int = 5432,
    tag: str = None,
    overwrite: bool = True,
) -> NoReturn:
    """

    :param namespace: Namespace of the projects
    :param tag: Tag of the projects
    :param db: db name of the db
    :param host: host of the db
    :param user: Username
    :param password: Password
    :param port: port of the database
    :param overwrite: update project in PEPhub if it already exists
    :return: NoReturn
    """

    pep_db_connection = pepdbagent.Connection(
        host=host, port=port, database=db, user=user, password=password
    )
    log_connection = UploadLogger(
        host=host, port=port, database=db, user=user, password=password
    )

    time_now = datetime.datetime.now()

    _LOGGER.info(f"Time now: {time_now}")
    _LOGGER.info(f"geofetch version: {geofetch.__version__}")
    _LOGGER.info(f"pepdbagent version: {pepdbagent.__version__}")
    _LOGGER.info(f"peppy version: {peppy.__version__}")

    # print(host, port, db, user, password)

    gse_list = geofetch.Finder().get_gse_by_day_count(2)
    geofetcher_obj = geofetch.Geofetcher()

    total_nb = len(gse_list)
    process_nb = 0

    _LOGGER.info(f"Number of projects that will be processed: {total_nb}")

    status_dict = {
        "total": total_nb,
        "success": 0,
        "failure": 0,
        "warning": 0,
    }

    log_model_dict = {}

    for gse in gse_list:
        model_l = LogModel(gse=gse, log_stage=0, status="queued")
        model_l = log_connection.upload_log(model_l)
        log_model_dict[gse] = model_l

    for gse in log_model_dict.keys():

        gse_log = log_model_dict[gse]

        gse_log.status = "processing"
        gse_log.log_stage = 1
        log_connection.upload_log(gse_log)

        process_nb += 1
        _LOGGER.info(f"\033[0;33mProcessing GSE: {gse}. {process_nb}/{total_nb}\033[0m")

        try:
            gse_log.log_stage = 2
            project_dict = geofetcher_obj.get_projects(gse)
            _LOGGER.info(f"Project has been downloaded using geofetch")
        except Exception as err:
            gse_log.status = "failure"
            gse_log.info = err
            gse_log.status_info = "geofetcher"
            log_connection.upload_log(gse_log)
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

            upload_return = pep_db_connection.upload_project(
                project=project_dict[prj_name],
                namespace=namespace,
                name=pep_name,
                tag=pep_tag,
                overwrite=overwrite,
            )
            gse_log.log_stage = 3
            gse_log.status = upload_return.status
            gse_log.status_info = upload_return.log_stage
            gse_log.info = upload_return.info
            log_connection.upload_log(gse_log)

            status_dict[upload_return.status] += 1

    _LOGGER.info(f"================== Finished ==================")
    _LOGGER.info(f"\033[32mAfter run report: {status_dict}\033[0m")


# upload_geo_projects(
#     namespace="new",
#     tag="def",
#     db="pep-db",
#     host="localhost",
#     user="postgres",
#     password="docker",
# )


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

    upload_geo_projects(
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
