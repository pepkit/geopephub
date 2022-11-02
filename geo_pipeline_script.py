from geofetch import Geofetcher, Finder
import pepdbagent
import argparse
import sys
from typing import NoReturn


def upload_geo_projects(
    namespace: str,
    tag: str,
    db: str,
    host: str,
    user: str,
    password: str,
    port: int = 5432,
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
    # pep_db_connection = pepdbagent.Connection(host=host, port=port, database=db, user=user, password=password)

    print(host, port, db, user, password)

    ff = Finder(retmax=2).get_gse_by_day_count(2)

    for gse in ff:
        print(f"processing GSE: {gse}")
        project_dict = Geofetcher().get_projects(gse)
        print("project has been downloaded")
        for prj_name in project_dict:
            print(f"project_name = {prj_name}")
            # pep_db_connection.upload_project(project=project_dict[prj_name], namespace=namespace, name=prj_name, tag=tag)


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
