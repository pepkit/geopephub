import geofetch
import pepdbagent
from typing import NoReturn, Dict
import datetime
import logging

# from update_status import UploadStatusConnection

from sqlalchemy.exc import NoResultFound
from datetime import timedelta

import peppy

from geopephub.const import LAST_UPDATE_DATES
from geopephub.utils import get_agent, get_base_db_engine
from geopephub.models import StatusModel, CycleModel
from geopephub.utils import run_geofetch, add_link_to_description


_LOGGER = logging.getLogger(__name__)


def add_to_queue_by_period(
    target: str,
    tag: str,
    start_period: str,
    end_period: str,
) -> None:
    """

    :param target: Namespace of the projects (bedbase, geo)
    :param tag: Tag of the projects
    :param start_period: start date of cycle [e.g. 2023/02/07]
    :param end_period: end date of cycle [e.g. 2023/02/08]
    :return: NoReturn
    """
    status_db_connection = get_base_db_engine()

    time_now = datetime.datetime.now()

    _LOGGER.info(f"Time now: {time_now}")
    _LOGGER.info(f"geofetch version: {geofetch.__version__}")
    _LOGGER.info(f"pepdbagent version: {pepdbagent.__version__}")
    _LOGGER.info(f"peppy version: {peppy.__version__}")

    today_date_str = end_period
    start_date_str = start_period

    this_cycle = CycleModel(
        target=target,
        status="initial",
        start_period=start_date_str,
        end_period=today_date_str,
    )
    status_db_connection.update_upload_cycle(this_cycle)

    if target == "bedbase":
        # get projects only with this filter
        gse_list = geofetch.Finder(
            filters="((bed) OR narrowPeak) OR broadPeak"
        ).get_gse_by_date(start_date_str, today_date_str)
    elif target == "geo":
        gse_list = geofetch.Finder().get_gse_by_date(start_date_str, today_date_str)
    else:
        this_cycle.status = "failure"
        status_db_connection.update_upload_cycle(this_cycle)
        raise Exception(f"Error in target: {target}")

    this_cycle.number_of_projects = len(gse_list)
    status_db_connection.update_upload_cycle(this_cycle)

    _LOGGER.info(
        f"Number of projects that will be processed: {this_cycle.number_of_projects}"
    )

    log_model_dict = {}

    for gse in gse_list:
        model_l = StatusModel(
            target=target,
            gse=gse,
            log_stage=0,
            status="queued",
            registry_path=f"{target}/{gse}:{tag}",
            upload_cycle_id=this_cycle.id,
        )
        model_l = status_db_connection.upload_project_log(model_l)
        log_model_dict[gse] = model_l

        _LOGGER.info(f"GSE: '{gse}' was added to the queue! Target: {target}")

    this_cycle.status = "queued"
    status_db_connection.update_upload_cycle(this_cycle)

    _LOGGER.info("================== Finished ==================")
    _LOGGER.info(
        f"\033[32mAfter run report: Added {this_cycle.number_of_projects} projects\033[0m"
    )


def add_to_queue(
    target: str,
    tag: str,
    period: int = LAST_UPDATE_DATES,
) -> NoReturn:
    """

    :param target: Namespace of the projects (bedbase, geo)
    :param tag: Tag of the projects
    :param period: number of last days to add to the queue
    :return: NoReturn
    """
    today_date = datetime.datetime.today()
    start_date = today_date - timedelta(days=period)
    end_date_str = today_date.strftime("%Y/%m/%d")
    start_date_str = start_date.strftime("%Y/%m/%d")

    add_to_queue_by_period(
        target=target,
        tag=tag,
        start_period=start_date_str,
        end_period=end_date_str,
    )


def upload_queued_projects(
    target: str,
    tag: str = None,
) -> None:
    # LOG info
    time_now = datetime.datetime.now()
    _LOGGER.info(f"Time now: {time_now}")
    _LOGGER.info(f"geofetch version: {geofetch.__version__}")
    _LOGGER.info(f"pepdbagent version: {pepdbagent.__version__}")
    _LOGGER.info(f"peppy version: {peppy.__version__}")

    agent = get_agent()
    status_db_connection = get_base_db_engine()

    list_of_cycles = status_db_connection.get_queued_cycle(target=target)

    if not list_of_cycles:
        _LOGGER.info("No queued cycles found. Quitting..")

    for this_cycle in list_of_cycles:
        this_cycle.status = "processing"
        this_cycle = status_db_connection.update_upload_cycle(this_cycle)
        gse_log_list = status_db_connection.get_queued_project(cycle_id=this_cycle.id)

        log_model_dict = {}
        for gse_log_item in gse_log_list:
            log_model_dict[gse_log_item.gse] = gse_log_item

        status_dict = _upload_gse_project(
            agent, status_db_connection, log_model_dict, target, tag
        )

        this_cycle.number_of_projects = status_dict.get("total")
        # this_cycle.number_of_successes = status_dict.get("success") + status_dict.get(
        #     "warning"
        # )
        this_cycle.number_of_successes = (
            status_db_connection.get_number_samples_success(this_cycle.id)
            + status_db_connection.get_number_samples_warnings(this_cycle.id)
        )
        # this_cycle.number_of_failures = status_dict.get("failure")
        this_cycle.number_of_failures = (
            status_db_connection.get_number_samples_failures(this_cycle.id)
        )

        this_cycle.status = "success"

        status_db_connection.update_upload_cycle(this_cycle)


def _upload_gse_project(
    agent,
    log_connection,
    log_model_dict,
    target,
    tag=None,
) -> Dict[str, int]:
    """
    Get, upload to PEPhub and load log to database of GSE project
    :param agent: pepdbagent object connected to db
    :param log_connection: UploadStatusConnection object connected to db
    :param log_model_dict: dictionary with StatusModel (seq table model), where keys are GSEs
    :param target: namespace where project's should be added
    :return: NoReturn
    """
    if target == "bedbase":
        geofetcher_obj = geofetch.Geofetcher(
            filter="\.(bed|bigBed|narrowPeak|broadPeak)\.",
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
        log_connection.upload_project_log(gse_log)

        process_nb += 1
        _LOGGER.info(f"\033[0;33mProcessing GSE: {gse}. {process_nb}/{total_nb}\033[0m")

        try:
            gse_log.status_info = "geofetcher"
            gse_log.log_stage = 2
            project_dict = run_geofetch(gse, geofetcher_obj)
            _LOGGER.info("Project has been downloaded using geofetch")
        except Exception as err:
            gse_log.status = "failure"
            gse_log.info = str(err)
            log_connection.upload_project_log(gse_log)
            status_dict["failure"] += 1
            continue

        if len(list(project_dict.keys())) == 0:
            gse_log.status = "warning"
            gse_log.info = "No data was fetched from GEO, check if project has any data"
            gse_log.status_info = "geofetcher"
            log_connection.upload_project_log(gse_log)
            status_dict["warning"] += 1
            continue

        for prj_name in project_dict:
            prj_name_list = prj_name.split("_")
            pep_name = prj_name_list[0]
            pep_tag = prj_name_list[1]

            gse_log.registry_path = f"{target}/{pep_name}:{pep_tag}"
            log_connection.upload_project_log(gse_log)

            _LOGGER.info(
                f"Namespace = {target} ; Project_name = {pep_name} ; Tag = {pep_tag}"
            )
            project_dict[prj_name] = add_link_to_description(
                gse=prj_name_list[0], pep=project_dict[prj_name]
            )
            gse_log.log_stage = 3
            gse_log.status_info = "pepdbagent"
            if target == "bedbase":
                tag = pep_tag
            else:
                tag = "default"
            try:
                agent.project.create(
                    project=project_dict[prj_name],
                    namespace=target,
                    name=pep_name,
                    tag=tag,
                    overwrite=True,
                    description=project_dict[prj_name].description,
                    pep_schema="pep/2.1.0",
                )
                gse_log.status = "success"
                gse_log.info = ""
                log_connection.upload_project_log(gse_log)

                status_dict["success"] += 1
            except Exception as err:
                gse_log.status = "failure"
                gse_log.info = str(err)
                log_connection.upload_project_log(gse_log)

                status_dict["failure"] += 1

    _LOGGER.info("================== Finished ==================")
    _LOGGER.info(f"\033[32mAfter run report: {status_dict}\033[0m")
    return status_dict


def run_upload_checker(
    target: str,
    period_length: int,
    tag: str,
    number_of_cycles: int = 1,
) -> NoReturn:
    """
    Check if previous run (cycle) was successful.

    :param target: Namespace of the projects (bedbase, geo)
    :param period_length: length of the period
    :param tag: tag of the projects
    :param number_of_cycles: what cycle behind should be checked?
    :return: NoReturn
    """

    today_date = datetime.datetime.today() - timedelta(
        days=period_length * number_of_cycles
    )
    start_date = today_date - timedelta(days=period_length)
    start_period = start_date.strftime("%Y/%m/%d")
    end_period = today_date.strftime("%Y/%m/%d")
    check_by_date(
        target=target,
        start_period=start_period,
        end_period=end_period,
        tag=tag,
    )


def check_by_date(
    target: str,
    start_period: str,
    end_period: str,
    tag: str,
) -> NoReturn:
    """
    Check if previous run (cycle) was successful.

    :param target: Namespace of the projects (bedbase, geo)
    :param start_period: start_period (Earlier in the calender) ["2020/02/25"]
    :param end_period: end period (Later in the calender) ["2021/05/27"]
    :param tag: tag of the projects
    :return: NoReturn
    """

    status_db_connection = get_base_db_engine()

    today_date = datetime.datetime.strptime(end_period, "%Y/%m/%d")
    start_date = datetime.datetime.strptime(start_period, "%Y/%m/%d")
    start_period = start_date.strftime("%Y/%m/%d")
    end_period = today_date.strftime("%Y/%m/%d")

    try:
        cycle_info = status_db_connection.was_run_successful(
            target=target, start_period=start_period, end_period=end_period
        )

        if not cycle_info:
            raise NoResultFound

        if cycle_info.status not in ["success", "processing"]:
            raise CycleSuccessException
        else:
            _LOGGER.info(
                f"Cycle {start_period}:{end_period} was successful. (Queuing was successful)"
            )
            _LOGGER.info("Checking sample uploading status...")
            if cycle_info.number_of_projects == cycle_info.number_of_successes:
                _LOGGER.info("All uploads were successful.")
            else:
                list_of_failed_prj = status_db_connection.get_failed_project(
                    cycle_info.id
                )

                log_model_dict = {}
                for gse_log_item in list_of_failed_prj:
                    log_model_dict[gse_log_item.gse] = gse_log_item

                agent = get_agent()

                status_dict = _upload_gse_project(
                    agent, status_db_connection, log_model_dict, target, tag
                )
                cycle_info.number_of_successes = (
                    status_db_connection.get_number_samples_success(cycle_info.id)
                    + status_db_connection.get_number_samples_warnings(cycle_info.id)
                )
                cycle_info.number_of_failures = (
                    status_db_connection.get_number_samples_failures(cycle_info.id)
                )
                cycle_info.status = "success"

                # cycle_info.number_of_failures = status_dict.get("failure")
                status_db_connection.update_upload_cycle(cycle_info)

    except (CycleSuccessException, NoResultFound, IndexError):
        _LOGGER.warning("Result not found, Uploading!")
        # return False
        add_to_queue_by_period(
            target=target,
            start_period=start_period,
            end_period=end_period,
            tag=tag,
        )
        upload_queued_projects(
            target=target,
            tag=tag,
        )


def clean_history(days: int = 90) -> None:
    """
    Delete histrory of project changes in pephub database

    :param days: number of days to keep in history
    :return: NoReturn
    """

    agent = get_agent()
    agent.project.clean_history(days=days)


class CycleSuccessException(Exception):
    """Exception, when cycle has status: Failure."""

    def __init__(self, reason: str = ""):
        """
        Optionally provide explanation for exceptional condition.

        :param reason: some additional information
        """
        super(Exception, self).__init__(reason)
