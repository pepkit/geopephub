import geofetch
import pepdbagent
import argparse
import sys
from typing import NoReturn
import datetime
import logmuse
import coloredlogs
from log_uploader import UploadLogger
from models import StatusModel

import peppy

from const import LAST_UPDATE_DATES


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
    tag: str = "raw",
) -> NoReturn:
    """

    :param namespace: Namespace of the projects
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

    gse_list = geofetch.Finder().get_gse_by_day_count(LAST_UPDATE_DATES)

    total_nb = len(gse_list)

    _LOGGER.info(f"Number of projects that will be processed: {total_nb}")

    log_model_dict = {}

    for gse in gse_list:
        model_l = StatusModel(
            gse=gse,
            log_stage=0,
            status="queued",
            registry_path=f"{namespace}/{gse}:{tag}",
        )
        model_l = log_connection.upload_log(model_l)
        log_model_dict[gse] = model_l
        _LOGGER.info(f"GSE: '{gse}' was added to the queue! ")

    _LOGGER.info(f"================== Finished ==================")
    _LOGGER.info(f"\033[32mAfter run report: Added {total_nb} projects\033[0m")


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


# upload_geo_projects(
#     namespace="new",
#     tag="def",
#     db="pep-db",
#     host="localhost",
#     user="postgres",
#     password="docker",
# )
