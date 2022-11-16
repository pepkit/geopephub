import geofetch
import pepdbagent
import argparse
import sys
from typing import NoReturn, Dict, List
import datetime
import logmuse
import coloredlogs

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
    pep_db_connection = pepdbagent.Connection(
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
    info_list = []

    _LOGGER.info(f"Number of projects that will be processed: {total_nb}")
    for gse in gse_list:
        process_nb += 1
        _LOGGER.info(f"Processing GSE: {gse}. {process_nb}/{total_nb}")

        project_dict = geofetcher_obj.get_projects(gse)
        _LOGGER.info(f"Project has been downloaded using geofetch")

        for prj_name in project_dict:
            prj_name_list = prj_name.split("_")
            pep_name = prj_name_list[0]
            pep_tag = prj_name_list[1]

            _LOGGER.info(
                f"Namespace = {namespace} ; Project_name = {pep_name} ; Tag = {pep_tag}"
            )

            upload_return = pep_db_connection.upload_project(
                project=project_dict[prj_name],
                namespace=namespace,
                name=pep_name,
                tag=pep_tag,
            )

            if isinstance(upload_return, str):
                info_list.append({
                    "gse_acc": gse,
                    "status": "failure",
                })
                print(gse)
            # else:
            #     info_list.append({
            #         "gse_acc": gse,
            #         "status": "success",
            #     })

        # except Exception as err:
        #     print(f"===============================")
        #     print(f"Error whiled downloading: {gse}")
        #     print(f"Error message: {err}")
        #     print(f"===============================")


# def write_log_file(info_list: List[Dict], file: str) -> NoReturn:
#     """
#     Write a log file with information about uploaded files
#     :param info_list: list of information that has to be added to the file
#     :param file: path to the file
#     :return: NoReturn
#
#     """
#     with open(file, 'w+') as f:
#         f.writelines(info_list)


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
