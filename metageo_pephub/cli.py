from argparse import ArgumentParser


def _parse_cmdl(cmdl):
    parser = ArgumentParser(
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

    parser.add_argument(
        "-f",
        "--function",
        required=True,
        choices=["q_insert", "q_upload", "insert_one", "create_status_table"],
        help="Choose which function should metageo should run",
        type=str,
    )

    parser.add_argument(
        "-g",
        "--gse",
        required=False,
        help="GSE number that has to be inserted [only used when function 'insert_one' was chosen]",
        type=str,
    )

    parser.add_argument(
        "-p",
        "--period",
        required=False,
        default=1,
        help="Period - number of days (time frame) when fetch metadata from GEO [used for q_fetch function]",
        type=int,
    )
    parser.add_argument(
        "--target",
        required=True,
        default="Target of the pipeline. Nampespace, and purpose of pipeline",
        choices=["bedbase", "geo"],
        help="tag of the project",
    )

    parser.add_argument(
        "--tag",
        required=False,
        default="default",
        help="tag of the project",
    )

    return parser.parse_args(cmdl)




