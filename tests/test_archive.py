"""Offline tests for the v2 archive builder. No database, no network."""

import os
import tarfile

import pytest

from geopephub.archive import (
    count_projects,
    extract_archive,
    gse_shard,
    normalize_archive_key,
    resolve_shards,
)
from geopephub.utils import create_gse_sub_name, tar_folder


def make_pep(peps_dir, shard, name):
    """Drop a stand-in PEP zip into a shard directory."""
    shard_dir = os.path.join(peps_dir, shard)
    os.makedirs(shard_dir, exist_ok=True)
    path = os.path.join(shard_dir, f"{name}.zip")
    with open(path, "w") as f:
        f.write(name)
    return path


class TestGseShard:
    @pytest.mark.parametrize(
        "name,expected",
        [
            ("gse100000", "gse100nnn"),
            ("gse100999", "gse100nnn"),
            ("gse271386", "gse271nnn"),
            ("gse12345678", "gse12345nnn"),
            ("gse1234567", "gse1234nnn"),
            ("gse123456", "gse123nnn"),
            ("gse1234", "gse1nnn"),
            ("gse123", "gsennn"),
            ("gse1", "gsennn"),
        ],
    )
    def test_shards(self, name, expected):
        assert gse_shard(name) == expected

    def test_shard_is_stable(self):
        """Re-sharding an already-correct name must not move it."""
        for name in ["gse100000", "gse12345678", "gse123"]:
            assert gse_shard(name) == gse_shard(name)

    def test_namespace_argument_is_the_v1_bug(self):
        """
        Regression guard for commit 7cf93035.

        v1 calls create_gse_sub_name(namespace); 'geo' is under the length
        threshold so every project collapses into one directory. Pin that
        behavior so it is obvious why v2 shards on the project name instead.
        """
        assert create_gse_sub_name("geo") == "gsennn"
        assert create_gse_sub_name("bedbase") != "gsennn"
        assert gse_shard("gse100000") == "gse100nnn"


class TestNormalizeArchiveKey:
    @pytest.mark.parametrize(
        "stored,expected",
        [
            # what pepdbagent returns (raw column value)
            ("geo/geo_2025_04_28.tar", "geo/geo_2025_04_28.tar"),
            # what the pephub HTTP API returns
            (
                "https://cloud2.databio.org/pephub/geo/geo_2025_04_28.tar",
                "geo/geo_2025_04_28.tar",
            ),
            ("http://cloud2.databio.org/pephub/geo/geo_2024_10_01.tar", "geo/geo_2024_10_01.tar"),
            ("/geo/geo_2025_04_28.tar", "geo/geo_2025_04_28.tar"),
            ("bedbase/bedbase_2025_01_01.tar", "bedbase/bedbase_2025_01_01.tar"),
        ],
    )
    def test_normalize(self, stored, expected):
        assert normalize_archive_key(stored) == expected


class TestResolveShards:
    def test_relocates_flat_pile(self, tmp_path):
        """The gsennn pile left by the v1 regression gets redistributed."""
        peps = str(tmp_path / "peps")
        for name in ["gse100000", "gse100001", "gse271386", "gse12345678"]:
            make_pep(peps, "gsennn", name)

        moved = resolve_shards(peps)

        assert moved == 4
        assert os.path.exists(os.path.join(peps, "gse100nnn", "gse100000.zip"))
        assert os.path.exists(os.path.join(peps, "gse100nnn", "gse100001.zip"))
        assert os.path.exists(os.path.join(peps, "gse271nnn", "gse271386.zip"))
        assert os.path.exists(os.path.join(peps, "gse12345nnn", "gse12345678.zip"))

    def test_leaves_correct_shards_alone(self, tmp_path):
        peps = str(tmp_path / "peps")
        make_pep(peps, "gse100nnn", "gse100000")
        make_pep(peps, "gse271nnn", "gse271386")

        assert resolve_shards(peps) == 0

    def test_short_names_stay_in_gsennn(self, tmp_path):
        """Names of 6 chars or fewer legitimately shard to gsennn."""
        peps = str(tmp_path / "peps")
        make_pep(peps, "gsennn", "gse123")
        make_pep(peps, "gsennn", "gse1")

        assert resolve_shards(peps) == 0
        assert os.path.exists(os.path.join(peps, "gsennn", "gse123.zip"))
        assert os.path.exists(os.path.join(peps, "gsennn", "gse1.zip"))

    def test_idempotent(self, tmp_path):
        peps = str(tmp_path / "peps")
        for name in ["gse100000", "gse271386"]:
            make_pep(peps, "gsennn", name)

        assert resolve_shards(peps) == 2
        assert resolve_shards(peps) == 0

    def test_preserves_project_count(self, tmp_path):
        peps = str(tmp_path / "peps")
        names = ["gse100000", "gse100001", "gse271386", "gse123", "gse12345678"]
        for name in names:
            make_pep(peps, "gsennn", name)

        before = count_projects(peps)
        resolve_shards(peps)
        assert count_projects(peps) == before == len(names)

    def test_mixed_tree(self, tmp_path):
        """A real archive: correct shards from Sept 2024 plus a later pile."""
        peps = str(tmp_path / "peps")
        make_pep(peps, "gse100nnn", "gse100000")
        make_pep(peps, "gse271nnn", "gse271386")
        make_pep(peps, "gsennn", "gse250000")
        make_pep(peps, "gsennn", "gse260000")

        assert resolve_shards(peps) == 2
        assert count_projects(peps) == 4
        assert os.path.exists(os.path.join(peps, "gse250nnn", "gse250000.zip"))
        assert os.path.exists(os.path.join(peps, "gse100nnn", "gse100000.zip"))


class TestCountProjects:
    def test_counts_recursively(self, tmp_path):
        peps = str(tmp_path / "peps")
        make_pep(peps, "gse100nnn", "gse100000")
        make_pep(peps, "gse100nnn", "gse100001")
        make_pep(peps, "gse271nnn", "gse271386")

        assert count_projects(peps) == 3

    def test_ignores_non_zip(self, tmp_path):
        peps = str(tmp_path / "peps")
        make_pep(peps, "gse100nnn", "gse100000")
        with open(os.path.join(peps, "gse100nnn", "README.md"), "w") as f:
            f.write("not a pep")

        assert count_projects(peps) == 1

    def test_empty_tree(self, tmp_path):
        peps = str(tmp_path / "peps")
        os.makedirs(peps)
        assert count_projects(peps) == 0


class TestArchiveRoundTrip:
    def test_tar_then_extract(self, tmp_path):
        """tar_folder (v1) output must be readable by extract_archive (v2)."""
        source = tmp_path / "source"
        peps = str(source / "peps")
        names = ["gse100000", "gse100001", "gse271386"]
        for name in names:
            make_pep(peps, gse_shard(name), name)

        tar_path = tar_folder(peps, str(tmp_path / "geo_2026_07_31"))
        assert tar_path.endswith(".tar")

        extracted_root = tmp_path / "extracted"
        peps_out = extract_archive(tar_path, str(extracted_root))

        assert os.path.basename(peps_out) == "peps"
        assert count_projects(peps_out) == len(names)
        for name in names:
            assert os.path.exists(os.path.join(peps_out, gse_shard(name), f"{name}.zip"))

    def test_round_trip_is_stable_across_generations(self, tmp_path):
        """Extract, add a project, re-tar, extract again -- the update cycle."""
        peps = str(tmp_path / "gen1" / "peps")
        make_pep(peps, "gse100nnn", "gse100000")
        tar1 = tar_folder(peps, str(tmp_path / "gen1_archive"))

        gen2 = tmp_path / "gen2"
        peps2 = extract_archive(tar1, str(gen2))
        make_pep(peps2, "gse271nnn", "gse271386")
        tar2 = tar_folder(peps2, str(tmp_path / "gen2_archive"))

        gen3 = tmp_path / "gen3"
        peps3 = extract_archive(tar2, str(gen3))

        assert count_projects(peps3) == 2
        assert os.path.exists(os.path.join(peps3, "gse100nnn", "gse100000.zip"))
        assert os.path.exists(os.path.join(peps3, "gse271nnn", "gse271386.zip"))

    def test_rejects_unexpected_layout(self, tmp_path):
        """A tar not rooted at peps/ must be refused, not silently built upon."""
        stray = tmp_path / "elsewhere"
        stray.mkdir()
        (stray / "gse100000.zip").write_text("x")

        tar_path = tar_folder(str(stray), str(tmp_path / "bad"))

        with pytest.raises(ValueError, match="not rooted at"):
            extract_archive(tar_path, str(tmp_path / "out"))

    def test_rejects_path_traversal(self, tmp_path):
        tar_path = str(tmp_path / "evil.tar")
        payload = tmp_path / "payload"
        payload.write_text("x")
        with tarfile.open(tar_path, "w") as tar:
            tar.add(str(payload), arcname="peps/../../escaped.zip")

        with pytest.raises(ValueError):
            extract_archive(tar_path, str(tmp_path / "out"))
