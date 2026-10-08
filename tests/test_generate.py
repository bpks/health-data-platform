import csv
import json
from pathlib import Path

import pytest

from hdp.generate import Volumes, generate_day1, generate_day2


def _rows(path: Path):
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


@pytest.fixture()
def day1(tmp_path):
    manifest = generate_day1(tmp_path, "2025-06-01", seed=7, volumes=Volumes.small())
    return tmp_path, manifest


def test_same_seed_gives_identical_files(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    generate_day1(a, "2025-06-01", seed=7, volumes=Volumes.small())
    generate_day1(b, "2025-06-01", seed=7, volumes=Volumes.small())
    for name in ["applications.csv", "enrolments.csv", "participants.csv", "studies.csv", "sites.csv"]:
        assert (a / "batch_date=2025-06-01" / name).read_bytes() == (b / "batch_date=2025-06-01" / name).read_bytes()


def test_different_seeds_differ(tmp_path):
    generate_day1(tmp_path / "a", "2025-06-01", seed=1, volumes=Volumes.small())
    generate_day1(tmp_path / "b", "2025-06-01", seed=2, volumes=Volumes.small())
    assert (tmp_path / "a/batch_date=2025-06-01/participants.csv").read_bytes() != (
        tmp_path / "b/batch_date=2025-06-01/participants.csv"
    ).read_bytes()


def test_manifest_matches_what_was_written(day1):
    landing, manifest = day1
    for entity, n in manifest["rows_written"].items():
        assert len(_rows(landing / "batch_date=2025-06-01" / f"{entity}.csv")) == n
    on_disk = json.loads((landing / "batch_date=2025-06-01/_generation_manifest.json").read_text())
    assert on_disk["expected_quarantined"] == manifest["expected_quarantined"]
    # something was broken on purpose, otherwise the data quality tests would prove nothing
    assert sum(manifest["expected_quarantined"].values()) > 0


def test_generated_participants_are_synthetic_and_unique(day1):
    landing, _ = day1
    refs = [r["participant_ref"] for r in _rows(landing / "batch_date=2025-06-01/participants.csv")]
    well_formed = [r for r in refs if len(r) == 10]
    # duplicates are injected on purpose (~1%), nothing more than that
    assert len(set(well_formed)) >= len(well_formed) * 0.97


def test_day2_requires_an_earlier_batch(tmp_path):
    with pytest.raises(FileNotFoundError):
        generate_day2(tmp_path, "2025-06-02")


def test_day2_is_an_incremental_extract_with_a_stale_record_and_renamed_sites(day1):
    landing, day1_manifest = day1
    m2 = generate_day2(landing, "2025-06-02", seed=8)
    d2 = landing / "batch_date=2025-06-02"
    apps = _rows(d2 / "applications.csv")
    assert len(apps) < day1_manifest["rows_written"]["applications"] / 2  # deltas, not a full reload

    stale = [r for r in apps if r["updated_at"].startswith("2000-01-01")]
    assert len(stale) == 1 and stale[0]["application_id"] == m2["stale_application"]["id"]

    # every progressed application was open on day 1 and now has a decision date
    day1_status = {
        r["application_id"]: r["status"].strip().lower()
        for r in _rows(landing / "batch_date=2025-06-01/applications.csv")
    }
    by_id = {r["application_id"]: r for r in apps}
    for app_id, status in m2["progressed_applications"].items():
        assert day1_status[app_id] in {"submitted", "in review"}
        assert by_id[app_id]["status"] == status and by_id[app_id]["decision_date"]

    sites_d1 = {r["site_id"]: r["site_name"] for r in _rows(landing / "batch_date=2025-06-01/sites.csv")}
    sites_d2 = {r["site_id"]: r["site_name"] for r in _rows(d2 / "sites.csv")}
    assert set(sites_d1) == set(sites_d2)  # full snapshot
    assert {s for s in sites_d1 if sites_d1[s] != sites_d2[s]} == set(m2["renamed_sites"])
