# This script aims to provide a simple way to get a list of all the PEPs in a
# PEPhub from GEO namespace, download them and zip into a single file.
import datetime

import pepdbagent
from pepdbagent.models import TarNamespaceModel
import pephubclient
from pephubclient.helpers import save_pep, MessageHandler
from pephubclient.files_manager import FilesManager
from pephubclient.exceptions import PEPExistsError
from pepdbagent.models import RegistryPath
import logging

from ubiquerg import parse_registry_path
import os
from typing import List, Union
import tempfile
import boto3
from botocore.exceptions import ClientError

from geopephub.utils import (
    get_agent,
    calculate_time,
    create_gse_sub_name,
    tar_folder,
    date_today,
)

_LOGGER = logging.getLogger(__name__)


@calculate_time
def bunch_geo(
    namespace: str = "geo",
    filter_by: str = "last_update_date",
    start_period: str = None,
    end_period: str = None,
    limit: int = 1000000,
    offset: int = 0,
    destination: str = None,
    order_by: str = "update_date",
    query: str = None,
    compress: bool = True,
    force: bool = False,
    subfolders: bool = True,
    tar_all: bool = True,
) -> Union[str, None]:
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
    :param tar_all: tar all the downloaded projects into a single file

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

    if s3 and s3.get("protocol") == "s3":
        process_to_s3(
            projects_list=projects_list, destination=s3.get("item"), agent=agent
        )

    else:
        os.makedirs(destination, exist_ok=True)

        pep_folder = os.path.join(destination, "peps")
        tar_folder_name = os.path.join(destination, "tars")

        os.makedirs(pep_folder, exist_ok=True)
        os.makedirs(tar_folder_name, exist_ok=True)

        new_destination = pep_folder

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
                    pep_folder, create_gse_sub_name(namespace)
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

        if tar_all:

            tar_name = os.path.join(tar_folder_name, f"{namespace}_{date_today()}")
            tar_name = tar_folder(pep_folder, tar_name)
            _LOGGER.info(f"Projects were tarred into {tar_name}")
            return tar_name
        return None


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


def upload_to_s3_file(file_name: str, bucket: str = "pephub", object_name: str = None):
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


@calculate_time
def auto_run(
    destination: str = None,
    namespace: str = "geo",
    compress: bool = True,
    tar_all: bool = True,
    upload_s3: bool = True,
    bucket: str = "pephub",
) -> None:
    """
    Automatically download project:
     1. Check what projects were uploaded since last uploading time.
     2. Download them.
     3. Tar them into a single file.
     4. Upload to s3 bucket.
     5. Save metadata to pephub

    :param destination: Output directory.
    :param namespace: namespace ['geo']
    :param compress: zip downloaded projects, default: True
    :param tar_all: tar all the downloaded projects into a single file
    :param upload_s3: upload to s3 bucket
    :param bucket: s3 bucket name
    """

    agent = get_agent()
    destination = os.path.join(destination, namespace)

    uploaded_tars = agent.namespace.get_tar_info(namespace=namespace)
    if uploaded_tars.count == 0:
        last_uploaded_period_date = datetime.datetime(2000, 1, 1)
    else:
        last_uploaded_period_date = uploaded_tars.results[0].creation_date
    last_uploaded_period_str = last_uploaded_period_date.strftime("%Y/%m/%d")

    today_string_str = date_today(separator="/")

    tar_name = bunch_geo(
        namespace=namespace,
        start_period=last_uploaded_period_str,
        end_period=today_string_str,
        destination=destination,
        compress=compress,
        tar_all=tar_all,
        limit=1000000,
        subfolders=True if namespace in ["geo", "bedbase"] else False,
    )

    base_name = os.path.join(namespace, os.path.basename(tar_name))
    if upload_s3:
        upload_to_s3_file(
            file_name=os.path.abspath(tar_name),
            bucket=bucket,
            object_name=base_name,
        )
    file_size = os.stat(os.path.abspath(tar_name)).st_size
    number_of_project = agent.annotation.get(namespace=namespace, limit=2).count
    agent.namespace.upload_tar_info(
        TarNamespaceModel(
            namespace=namespace,
            file_path=base_name,
            number_of_projects=number_of_project,
            file_size=file_size,
        )
    )
    _LOGGER.info(f"Metadata was uploaded successfully. Exiting...")
