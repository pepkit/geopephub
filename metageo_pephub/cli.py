from argparse import ArgumentParser


def _parse_cmdl(cmdl):
    parser = ArgumentParser(
        description="metageo_pephub pipline runer",
    )
    parser_checker = parser.add_argument_group("checker")
    parser_one_inserter = parser.add_argument_group("inserter_one")
    parser_check_by_date = parser.add_argument_group("check_by_date")

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
        choices=[
            "run_queuer",
            "run_uploader",
            "insert_one",
            "run_checker",
            "check_by_date",
        ],
        help="Choose which function should metageo should run",
        type=str,
    )

    parser_one_inserter.add_argument(
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

    parser_checker.add_argument(
        "--cycle-count",
        required=False,
        default=1,
        type=int,
        help="Cycle that has to be checked if it was successful"
        " before the earliest one. e.g "
        "if we want to check todays cycle (if cycles are happening every day)"
        " you should insert 1."
        "(2) if you want to specify cycle that was happening 3 week before, and every cycle is happening"
        "once a week, you should set 2",
    )

    parser_check_by_date.add_argument(
        "--start-period",
        required=False,
        default=None,
        type=str,
        help="start_period (Earlier in the calender) ['2020/02/25']",
    )
    parser_check_by_date.add_argument(
        "--end-period",
        required=False,
        default=None,
        type=str,
        help="end period (Later in the calender) ['2021/05/27']",
    )

    return parser.parse_args(cmdl)
