# This script aims to provide a simple way to get a list of all the PEPs in a
# PEPhub from GEO namespace, download them and zip into a single file.
import pepdbagent
import pephubclient
from pephubclient.helpers import save_pep, MessageHandler
from pephubclient.files_manager import FilesManager
from pephubclient.exceptions import PEPExistsError
from pepdbagent.models import RegistryPath
import logging

from ubiquerg import parse_registry_path
import os
from typing import List
import tempfile
import boto3
from botocore.exceptions import ClientError

from geopephub.utils import get_agent, calculate_time, create_gse_sub_name

_LOGGER = logging.getLogger(__name__)


@calculate_time
def bunch_geo(
    namespace: str = "geo",
    filter_by: str = "update_date",
    start_period: str = None,
    end_period: str = None,
    limit: int = 10000,
    offset: int = 0,
    destination: str = None,
    order_by: str = "last_update_date",
    query: str = None,
    compress: bool = True,
    force: bool = False,
    subfolders: bool = True,
) -> None:
    """
    Get a list of all the PEPs in a PEPhub from GEO namespace, download them and zip into a single file.

    :param namespace: namespace ['geo']
    :param filter_by: filter_by Options: ["last_update_date", "submission_date"]
    :param start_period: start_period when project was updated (Earlier in the calendar) ['2020/02/25']
    :param end_period: end period when project was updated (Later in the calendar) ['2021/05/27']
    :param limit: Number of projects to download
    :param offset: Offset of the projects to download
    :param destination: Output directory or s3 bucket. For s3 bucket use s3://bucket_name
    :param order_by: order_by Options: ["name", "update_date", "submission_date"]
    :param query: query string, e.g. "leukemia", default: None
    :param compress: zip downloaded projects, default: True
    :param force: force rewrite project if it exists, default: False
    :param subfolders: create subfolder for each pep based on GEO accession number

    :return: None
    """

    _LOGGER.info(f"pepdbagent version: {pepdbagent.__version__}")
    _LOGGER.info(f"{pephubclient.__app_name__} version: {pephubclient.__version__}")

    if not destination:
        destination = os.getcwd()
    s3 = parse_registry_path(destination)

    agent = get_agent()

    projects_list = agent.annotation.get_projects_list(
        search_str=query,
        namespace=namespace,
        limit=limit,
        offset=offset,
        order_by=order_by,
        filter_by=filter_by,
        filter_start_date=start_period,
        filter_end_date=end_period,
    )

    if s3:
        process_to_s3(
            projects_list=projects_list, destination=s3.get("item"), agent=agent
        )

    else:
        os.makedirs(destination, exist_ok=True)
        new_destination = destination

        total_number = len(projects_list)
        _LOGGER.info(f"Number of projects to be downloaded: {total_number}")
        for processing_number, geo in enumerate(projects_list):
            _LOGGER.info(
                f"\033[0;33m ############################################# \033[0m"
            )
            _LOGGER.info(
                f"\033[0;33mProcessing project: {geo.namespace}/{geo.name}:{geo.tag}. {processing_number+1}/{total_number}\033[0m"
            )
            project = agent.project.get(
                namespace=geo.namespace, name=geo.name, tag=geo.tag, raw=True
            )
            if subfolders:
                new_destination = os.path.join(
                    destination, create_gse_sub_name(geo.name)
                )
                os.makedirs(new_destination, exist_ok=True)

            try:
                save_pep(
                    project,
                    project_path=new_destination,
                    force=force,
                    zip=compress,
                )
            except PEPExistsError:
                _LOGGER.warning(f"Project {geo.name} already exists. skipping..")


def process_to_s3(
    projects_list: List[RegistryPath],
    destination: str,
    agent: pepdbagent.PEPDatabaseAgent = None,
) -> None:
    """
    Download projects from PEPhub and upload them to s3 bucket

    :param projects_list: project to upload
    :param destination: s3 bucket. For s3 bucket use s3://bucket_name
    :param agent: PEPDatabaseAgent object, default: None
    :return: None
    """
    if not agent:
        agent = get_agent()

    with tempfile.TemporaryDirectory() as temp_dir:
        total_number = len(projects_list) + 1
        _LOGGER.info(f"Number of projects that will be downloaded: {total_number}")
        for processing_number, geo in enumerate(projects_list):
            _LOGGER.info(
                f"\033[0;33m ############################################# \033[0m"
            )
            _LOGGER.info(
                f"\033[0;33mProcessing project: {geo.namespace}/{geo.name}:{geo.tag}. {processing_number}/{total_number}\033[0m"
            )
            project = agent.project.get(
                namespace=geo.namespace, name=geo.name, tag=geo.tag, raw=True
            )
            save_pep(
                project,
                project_path=temp_dir,
                force=True,
                zip=True,
            )
            for file in os.listdir(temp_dir):
                file_path = os.path.join(temp_dir, file)
                if os.path.isfile(file_path):
                    upload_to_s3_file(file_path, destination)
                    FilesManager.delete_file_if_exists(file_path)


def upload_to_s3_file(file_name: str, bucket: str, object_name: str = None):
    """Upload a file to an S3 bucket
    # copied from: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    s3_client = boto3.client("s3")

    try:
        s3_client.upload_file(file_name, bucket, object_name)
        MessageHandler.print_success(
            f"Project was uploaded successfully to s3://{bucket}/{object_name}"
        )
    except ClientError as e:
        print(e)
        return False

    return True
