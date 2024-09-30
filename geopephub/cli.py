import typer
from pandas.conftest import names

from geopephub.metageo_pephub import (
    add_to_queue,
    upload_queued_projects,
    run_upload_checker,
    check_by_date as check_by_date_function,
    clean_history as clean_history_function,
)
from geopephub.bunch_geo import bunch_geo, auto_run
from geopephub.__version__ import __version__

app = typer.Typer()


def validate_target(value: str):
    valid_target = ["geo", "bedbase"]
    if value.lower() not in valid_target:
        raise typer.BadParameter(
            f"Invalid color '{value}'. Choose from: {', '.join(valid_target)}"
        )
    return value.lower()


@app.command()
def run_queuer(
    target: str = typer.Option(
        ...,
        help="Target of the pipeline. Namespace, and purpose of pipeline. Options: ['geo','bedbase']",
        callback=validate_target,
    ),
    tag: str = typer.Option(
        "default",
        help="Tag of the project, that will be uploaded to the pephub",
    ),
    period: int = typer.Option(
        1,
        help="Period (number of day) (time frame) when fetch metadata from GEO [used for q_fetch function]",
    ),
):
    """
    Queue GEO projects that were uploaded or updated in the last period
    """
    add_to_queue(target=target, tag=tag, period=period)


@app.command()
def run_uploader(
    target: str = typer.Option(
        ...,
        help="Target of the pipeline. Namespace, and purpose of pipeline. Options: ['geo','bedbase']",
        callback=validate_target,
    ),
    tag: str = typer.Option(
        "default",
        help="Tag of the project, that will be uploaded to the pephub",
    ),
):
    """
    Upload projects that were queued, but not uploaded yet.
    """
    upload_queued_projects(
        target=target,
        tag=tag,
    )


@app.command()
def run_checker(
    target: str = typer.Option(
        ...,
        help="Target of the pipeline. Namespace, and purpose of pipeline. Options: ['geo','bedbase']",
        callback=validate_target,
    ),
    tag: str = typer.Option(
        "default",
        help="Tag of the project, that will be uploaded to the pephub",
    ),
    cycle_count: int = typer.Option(
        1,
        help="Cycle that has to be checked if it was successful"
        " before the earliest one. e.g "
        "if we want to check todays cycle (if cycles are happening every day)"
        " you should insert 0."
        "(2) if you want to specify cycle that was happening 3 week before, and every cycle is happening"
        "once a week, you should set 2",
    ),
    period: int = typer.Option(
        1,
        help="length of the period - number of days (time frame) when fetch metadata from GEO [used for q_fetch function]",
    ),
):
    """
    Check if all projects were uploaded successfully in specified period and upload them if not.
    To check if all projects were uploaded successfully 3 periods ago, where one period is 1 day, and cycles are happening every day,
    you should set cycle_count=3, and period_length=1. (geopephub run_checker --cycle-count 3 --period-length 1)

    """
    run_upload_checker(
        target=target,
        period_length=period,
        tag=tag,
        number_of_cycles=cycle_count,
    )


@app.command()
def check_by_date(
    target: str = typer.Option(
        ...,
        help="Target of the pipeline. Namespace, and purpose of pipeline. Options: ['geo','bedbase']",
        callback=validate_target,
    ),
    tag: str = typer.Option(
        "default",
        help="Tag of the project, that will be uploaded to the pephub",
    ),
    start_period: str = typer.Option(
        None,
        help="start_period (Earlier in the calender) ['2020/02/25']",
    ),
    end_period: str = typer.Option(
        None,
        help="end period (Later in the calender) ['2021/05/27']",
    ),
):
    """
    Check if all projects were uploaded successfully in specified period and upload them if not.
    Additionally, you can download projects from huge period of time.
    e.g. if you want to download projects from 2020/02/25 to 2021/05/27, you should set start_period=2020/02/25, and end_period=2021/05/27
    """
    check_by_date_function(
        target=target,
        tag=tag,
        start_period=start_period,
        end_period=end_period,
    )


@app.command()
def download(
    namespace: str = typer.Option(
        ...,
        help="Namespace of the projects that have to be downloaded. ",
    ),
    filter_by: str = typer.Option(
        "last_update_date",
        help="filter_by Options: ['last_update_date', 'submission_date']",
    ),
    start_period: str = typer.Option(
        None,
        help="start_period when project was updated (Earlier in the calendar) ['2020/02/25']"
        "By default set the earliest date in the database",
    ),
    end_period: str = typer.Option(
        None,
        help="end period when project was updated (Later in the calendar) ['2021/05/27']"
        "By default set the current date",
    ),
    limit: int = typer.Option(
        1000,
        help="Number of projects to download",
    ),
    offset: int = typer.Option(
        0,
        help="Offset of the projects to download",
    ),
    destination: str = typer.Option(
        None,
        help="Output directory or s3 bucket. For s3 bucket use s3://bucket_name"
        "By default set current directory",
    ),
    order_by: str = typer.Option(
        default="update_date",
        help="order_by Options: ['name', 'update_date', 'submission_date']",
    ),
    query: str = typer.Option(
        None,
        help="query string. You can find project by keyword, e.g. 'leukemia', default: None"
        "By default all projects will be downloaded",
    ),
    compress: bool = typer.Option(
        True,
        help="zip downloaded projects, default: True",
    ),
    force: bool = typer.Option(
        False,
        help="force rewrite project if it exists, default: False",
    ),
    subfolders: bool = typer.Option(
        True,
        help="Create subfolder hierarchy based on GEO accession number, default: True",
    ),
):
    """
    Download projects from the particular namespace.
    You can filter projects, order them, and download only part of them.
    """

    if namespace.lower() == "geo":
        subfolders = subfolders
    else:
        subfolders = False

    bunch_geo(
        namespace=namespace,
        filter_by=filter_by,
        start_period=start_period,
        end_period=end_period,
        limit=limit,
        offset=offset,
        destination=destination,
        order_by=order_by,
        query=query,
        compress=compress,
        force=force,
        subfolders=subfolders,
    )


@app.command(
    help="Automatically download projects from chosen namespace, tar them and upload to s3 (if needed). Don't forget to set up AWS credentials"
)
def auto_download(
    namespace: str = typer.Option(
        "geo",
        help="Namespace of the projects that have to be downloaded. Default: geo",
    ),
    destination: str = typer.Option(
        ...,
        help="Output directory or s3 bucket. By default set current directory",
    ),
    compress: bool = typer.Option(
        True,
        help="Zip each downloaded project, default: True",
    ),
    tar_all: bool = typer.Option(
        True,
        help="tar all the downloaded projects into a single file",
    ),
    upload_s3: bool = typer.Option(
        True,
        help="upload tar file to s3 bucket",
    ),
    bucket: str = typer.Option(
        "pephub",
        help="S3 bucket name",
    ),
) -> None:
    auto_run(
        namespace=namespace,
        destination=destination,
        compress=compress,
        tar_all=tar_all,
        upload_s3=upload_s3,
        bucket=bucket,
    )


@app.command()
def clean_history(
    days: int = typer.Option(
        90,
        help="Number of days to keep in the history. Default: 90",
    ),
):
    """
    Clean history of the pephub
    """
    clean_history_function(days=days)


def version_callback(value: bool):
    if value:
        typer.echo(f"geopephub version: {__version__}")
        raise typer.Exit()


@app.command()
def bedbase_stats():
    """
    Add bedbase stats to the database (each sample independently to the table)
    """
    from geopephub.bedbase_stats import main

    main()


@app.callback()
def common(
    ctx: typer.Context,
    version: bool = typer.Option(
        None, "--version", "-v", callback=version_callback, help="App version"
    ),
):
    pass
