# Full-namespace archive production (v2).
#
# v1 lives in bunch_geo.py and is intentionally left alone: it assumes a
# destination directory that survives between runs, which is how the manual
# Rivanna flow documented in the README works. This module assumes the
# opposite -- nothing persists except the tar in S3 -- so it rehydrates the
# previous archive, applies the delta, and republishes.

import concurrent.futures
import logging
import os
import shutil
import tarfile
from typing import Optional, Tuple

import pepdbagent
from pepdbagent.models import TarNamespaceModel
from pephubclient.helpers import save_pep

from geopephub.utils import date_today, get_agent, tar_folder

_LOGGER = logging.getLogger(__name__)

ARCHIVE_BUCKET = "pephub"
PEPS_DIRNAME = "peps"

# Sentinel start date for a cold start, matching v1's auto_run.
EPOCH_START = "2000/01/01"


def gse_shard(name: str) -> str:
    """
    Shard directory for a project NAME.

    gse100000 -> gse100nnn
    gse12345678 -> gse12345nnn
    gse123 -> gsennn

    utils.create_gse_sub_name does the same arithmetic but bunch_geo calls it
    with the *namespace*, so every name is <= 6 chars and everything collapses
    into one gsennn directory. This function exists to make the argument
    unambiguous: it is always the project name.

    :param name: project name, e.g. 'gse100000'
    :return: shard directory name
    """
    if len(name) <= 6:
        return "gsennn"
    return name[:-3] + "nnn"


def normalize_archive_key(file_path: str, bucket: str = ARCHIVE_BUCKET) -> str:
    """
    Recover the S3 object key from a stored archive file_path.

    pepdbagent returns the raw column value ('geo/geo_2025_04_28.tar'), but the
    pephub HTTP API rewrites it to an absolute CDN URL. Accept either so this
    works against whichever one is handed to it.

    :param file_path: value from TarNamespaceModel.file_path
    :param bucket: bucket name to strip when the path is absolute
    :return: object key relative to the bucket root
    """
    key = file_path
    if key.startswith("http://") or key.startswith("https://"):
        key = key.split("://", 1)[1]
        key = key.split("/", 1)[1] if "/" in key else ""
    key = key.lstrip("/")
    prefix = f"{bucket}/"
    if key.startswith(prefix):
        key = key[len(prefix) :]
    return key


def count_projects(peps_dir: str) -> int:
    """
    Count PEPs actually present in the archive tree.

    v1 registers agent.annotation.get(...).count -- the live namespace count --
    which makes the number useless for spotting a short dump. This counts what
    will really be in the tar.

    :param peps_dir: the peps/ directory
    :return: number of .zip files, recursively
    """
    total = 0
    for _, _, files in os.walk(peps_dir):
        total += sum(1 for f in files if f.endswith(".zip"))
    return total


def resolve_shards(peps_dir: str) -> int:
    """
    Move PEPs that landed in the wrong shard into the right one.

    Repairs the flat peps/gsennn/ pile left by the create_gse_sub_name(namespace)
    regression. Idempotent: a second call over a clean tree moves nothing.
    Projects whose name is short enough to legitimately shard to 'gsennn' stay
    where they are.

    :param peps_dir: the peps/ directory
    :return: number of files relocated
    """
    moved = 0
    for shard in sorted(os.listdir(peps_dir)):
        shard_path = os.path.join(peps_dir, shard)
        if not os.path.isdir(shard_path):
            continue
        for filename in sorted(os.listdir(shard_path)):
            if not filename.endswith(".zip"):
                continue
            correct = gse_shard(filename[: -len(".zip")])
            if correct == shard:
                continue
            target_dir = os.path.join(peps_dir, correct)
            os.makedirs(target_dir, exist_ok=True)
            shutil.move(
                os.path.join(shard_path, filename),
                os.path.join(target_dir, filename),
            )
            moved += 1
    if moved:
        _LOGGER.info(f"Relocated {moved} projects into correct shard directories")
    return moved


def extract_archive(tar_path: str, destination: str) -> str:
    """
    Extract a published archive, returning the peps/ directory inside it.

    v1 tars with arcname=basename(folder), so every member is rooted at 'peps/'.
    Anything else means the tar was not produced by this pipeline and the run
    should stop rather than build on top of an unknown layout.

    :param tar_path: local path to the downloaded tar
    :param destination: directory to extract into
    :return: path to the extracted peps/ directory
    """
    os.makedirs(destination, exist_ok=True)
    dest_real = os.path.realpath(destination)

    # Python 3.14 makes 'data' the default extraction filter; opt in early where
    # it exists so behavior does not shift under us.
    extract_kwargs = {"filter": "data"} if hasattr(tarfile, "data_filter") else {}

    with tarfile.open(tar_path, "r") as tar:
        for member in tar:
            root = member.name.split("/", 1)[0]
            if root != PEPS_DIRNAME:
                raise ValueError(
                    f"Unexpected archive layout: member '{member.name}' is not "
                    f"rooted at '{PEPS_DIRNAME}/'. Refusing to extract."
                )
            target = os.path.realpath(os.path.join(dest_real, member.name))
            if not target.startswith(dest_real + os.sep):
                raise ValueError(f"Archive member escapes destination: {member.name}")
            tar.extract(member, path=dest_real, **extract_kwargs)

    return os.path.join(destination, PEPS_DIRNAME)


def download_previous_archive(
    agent: pepdbagent.PEPDatabaseAgent,
    namespace: str,
    destination: str,
    bucket: str = ARCHIVE_BUCKET,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Fetch the most recent published archive for a namespace.

    :param agent: PEPDatabaseAgent
    :param namespace: namespace to look up
    :param destination: directory to download into
    :param bucket: S3 bucket holding the archives
    :return: (local tar path, start period as 'YYYY/MM/DD'), both None on cold start
    """
    import boto3

    info = agent.namespace.get_tar_info(namespace=namespace)
    if not info.count:
        _LOGGER.info(f"No archive registered for '{namespace}'; starting from scratch")
        return None, None

    latest = info.results[0]
    key = normalize_archive_key(latest.file_path, bucket=bucket)
    local_path = os.path.join(destination, os.path.basename(key))

    _LOGGER.info(f"Downloading previous archive s3://{bucket}/{key}")
    os.makedirs(destination, exist_ok=True)
    boto3.client("s3").download_file(bucket, key, local_path)

    start_period = latest.creation_date.strftime("%Y/%m/%d")
    _LOGGER.info(
        f"Previous archive: {key} ({latest.number_of_projects} projects, "
        f"created {start_period})"
    )
    return local_path, start_period


def sync_delta(
    agent: pepdbagent.PEPDatabaseAgent,
    namespace: str,
    peps_dir: str,
    start_period: str,
    end_period: str,
    workers: int = 4,
) -> dict:
    """
    Download every project updated in the window and write it into the tree.

    Filtering on last_update_date returns new *and* revised projects, so this
    single pass covers both -- provided save_pep is allowed to overwrite. v1
    passes force=False, so revisions raise PEPExistsError and get swallowed by a
    "skipping" warning; that is why every project revised since Sept 2024 is
    stale in the published archive. force=True here is the fix, and
    PEPExistsError is deliberately not caught: if it fires, something is wrong
    and the run should say so.

    :param agent: PEPDatabaseAgent
    :param namespace: namespace to sync
    :param peps_dir: the peps/ directory to write into
    :param start_period: earlier bound, 'YYYY/MM/DD'
    :param end_period: later bound, 'YYYY/MM/DD'
    :param workers: concurrent fetches; each opens its own session off the
        shared engine, so keep this under the SQLAlchemy pool size
    :return: {'attempted': int, 'written': int, 'failed': [(name, reason), ...]}
    """
    projects = agent.annotation.get_projects_list(
        namespace=namespace,
        limit=1000000,
        order_by="update_date",
        filter_by="last_update_date",
        filter_start_date=start_period,
        filter_end_date=end_period,
    )
    attempted = len(projects)
    _LOGGER.info(
        f"{attempted} projects updated between {start_period} and {end_period}"
    )

    failed = []
    written = 0

    def _fetch(registry_path) -> None:
        project = agent.project.get(
            namespace=registry_path.namespace,
            name=registry_path.name,
            tag=registry_path.tag,
            raw=True,
        )
        target = os.path.join(peps_dir, gse_shard(registry_path.name))
        os.makedirs(target, exist_ok=True)
        save_pep(project, project_path=target, force=True, zip=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch, p): p for p in projects}
        for done, future in enumerate(concurrent.futures.as_completed(futures), 1):
            registry_path = futures[future]
            try:
                future.result()
                written += 1
            except Exception as e:
                _LOGGER.warning(f"Failed: {registry_path.name}: {e}")
                failed.append((registry_path.name, str(e)))
            if done % 1000 == 0:
                _LOGGER.info(f"  {done}/{attempted} processed")

    _LOGGER.info(f"Delta complete: {written} written, {len(failed)} failed")
    return {"attempted": attempted, "written": written, "failed": failed}


def build_archive(
    namespace: str = "geo",
    workdir: str = None,
    start_period: str = None,
    workers: int = 4,
    register: bool = True,
    bucket: str = ARCHIVE_BUCKET,
    fail_threshold: float = 0.01,
) -> Optional[str]:
    """
    Produce a dated full-namespace archive without relying on persistent state.

    Downloads the last published archive, applies the update delta, repairs
    shard layout, re-tars, and (optionally) publishes and registers the result.

    :param namespace: namespace to archive
    :param workdir: scratch directory; needs roughly 5GB free for geo
    :param start_period: override the delta start ('YYYY/MM/DD'). Defaults to the
        previous archive's creation date. Widen it to re-pull projects that
        earlier runs skipped.
    :param workers: concurrent project fetches
    :param register: upload to S3 and write the archive row
    :param bucket: S3 bucket
    :param fail_threshold: abort without publishing if this fraction of fetches fail
    :return: path to the new tar, or None if nothing was produced
    """
    if not workdir:
        workdir = os.getcwd()
    os.makedirs(workdir, exist_ok=True)

    agent = get_agent()

    previous_tar, previous_period = download_previous_archive(
        agent, namespace, workdir, bucket=bucket
    )

    if previous_tar:
        peps_dir = extract_archive(previous_tar, workdir)
        os.remove(previous_tar)
        _LOGGER.info(f"Rehydrated {count_projects(peps_dir)} projects from previous archive")
    else:
        peps_dir = os.path.join(workdir, PEPS_DIRNAME)
        os.makedirs(peps_dir, exist_ok=True)

    resolve_shards(peps_dir)

    start = start_period or previous_period or EPOCH_START
    stats = sync_delta(
        agent,
        namespace=namespace,
        peps_dir=peps_dir,
        start_period=start,
        end_period=date_today(separator="/"),
        workers=workers,
    )

    if stats["attempted"]:
        failure_rate = len(stats["failed"]) / stats["attempted"]
        if failure_rate > fail_threshold:
            raise RuntimeError(
                f"{len(stats['failed'])}/{stats['attempted']} fetches failed "
                f"({failure_rate:.1%} > {fail_threshold:.1%}). Refusing to publish "
                f"a partial archive."
            )

    tar_path = tar_folder(peps_dir, os.path.join(workdir, f"{namespace}_{date_today()}"))
    actual = count_projects(peps_dir)
    live = agent.annotation.get(namespace=namespace, limit=1).count
    _LOGGER.info(f"Archived {actual} projects (live namespace count: {live})")
    if actual != live:
        _LOGGER.warning(
            f"Archive holds {actual} projects but the namespace reports {live}. "
            f"Deleted projects are never removed by a date-based delta; a digest "
            f"manifest would be needed to reconcile this."
        )

    if not register:
        _LOGGER.info(f"register=False; archive left at {tar_path}")
        return tar_path

    from geopephub.bunch_geo import upload_to_s3_file

    object_name = f"{namespace}/{os.path.basename(tar_path)}"
    upload_to_s3_file(file_name=os.path.abspath(tar_path), bucket=bucket, object_name=object_name)
    agent.namespace.upload_tar_info(
        TarNamespaceModel(
            namespace=namespace,
            file_path=object_name,
            number_of_projects=actual,
            file_size=os.stat(os.path.abspath(tar_path)).st_size,
        )
    )
    _LOGGER.info(f"Published and registered {object_name}")
    return tar_path
