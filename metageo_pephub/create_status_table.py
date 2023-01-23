from update_status import UploadLogger
import argparse
import sys


def _parse_cmdl(cmdl):
    parser = argparse.ArgumentParser(
        description="Automatic log table creator",
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
    parser.add_argument(
        "--port",
        required=False,
        default=5432,
        help="port of the db",
    )
    return parser.parse_args(cmdl)


def main():
    args = _parse_cmdl(sys.argv[1:])
    args_dict = vars(args)

    connection = UploadLogger(
        database=args_dict["db"],
        host=args_dict["host"],
        user=args_dict["user"],
        password=args_dict["password"],
        port=args_dict["port"],
    )

    connection.create_table()


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        print("Pipeline aborted.")
        sys.exit(1)
