"""Synthetic landing-zone data generator.

Everything is randomly generated from a seed: no real people, organisations or studies.
Postcodes use real outward-code prefixes with random inward codes, so they look realistic
but do not identify anyone.

Two kinds of batch are produced, mimicking a daily extract:

* day 1: a full historical load
* day 2: an incremental extract: changed and new application / study / enrolment /
  participant rows, plus a full snapshot of sites (a few of them renamed), plus a
  deliberately stale record that must NOT overwrite newer data

A small share of rows is deliberately broken (see ``bad_rate``) so the data quality
rules have something to catch. Two sidecar files next to each batch record what was
injected and which keys are valid; the tests use them to check the pipeline's counts.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

from hdp.model import ETHNICITIES, RAW_COLUMNS, REGIONS, RISK_LEVELS, STUDY_TYPES, TRUST_TYPES

PLACE_PREFIX = ["Ash", "Brook", "Cedar", "Dale", "Elm", "Fern", "Glen", "Heath", "Iron", "Juniper", "Kestrel", "Lark"]
PLACE_SUFFIX = ["field", "ford", "gate", "mere", "ton", "vale", "wick", "worth"]
SPONSOR_WORDS = ["Meridian", "Northlight", "Quillon", "Ravenscar", "Solace", "Tamarind", "Umber", "Verity"]
SPONSOR_KINDS = ["Therapeutics", "Research Ltd", "Biosciences", "Health Partners", "Diagnostics"]
OUTWARD_CODES = [
    "MK9",
    "MK40",
    "LU1",
    "NN1",
    "OX1",
    "CB2",
    "B15",
    "M1",
    "LS6",
    "S10",
    "NE1",
    "BS8",
    "EX4",
    "SW1A",
    "E1",
    "G12",
]
FIRST_STUDY_START = date(2019, 1, 1)
FIRST_SUBMISSION = date(2023, 1, 1)

# Kinds of deliberately broken rows. The "error" kinds must end up in quarantine; the other
# kinds are repaired by cleaning, de-duplicated, or only raise a warning.
APPLICATION_ERRORS = [
    "null_study_id",
    "invalid_status",
    "future_submitted",
    "decision_before_submission",
    "unknown_site",
]
ENROLMENT_ERRORS = ["unknown_study", "unknown_site", "unknown_participant", "future_enrolled_date"]
PARTICIPANT_ERRORS = ["invalid_dob", "implausible_age", "invalid_ref"]
ERROR_KINDS = {"applications": APPLICATION_ERRORS, "enrolments": ENROLMENT_ERRORS, "participants": PARTICIPANT_ERRORS}


@dataclass(frozen=True)
class Volumes:
    sites: int = 120
    studies: int = 2000
    applications: int = 20000
    participants: int = 60000
    enrolments: int = 150000

    @classmethod
    def small(cls) -> Volumes:
        return cls(sites=12, studies=60, applications=600, participants=900, enrolments=1500)


class _Tally:
    """Records what was injected and which keys are valid, per entity."""

    def __init__(self) -> None:
        self.injected: dict[str, Counter] = {e: Counter() for e in RAW_COLUMNS}
        self.valid_keys: dict[str, set[str]] = {e: set() for e in RAW_COLUMNS}
        self.written: Counter = Counter()

    def expected_quarantined(self) -> dict[str, int]:
        return {e: sum(self.injected[e][k] for k in kinds) for e, kinds in ERROR_KINDS.items()}


def _place(rng: random.Random) -> str:
    return rng.choice(PLACE_PREFIX) + rng.choice(PLACE_SUFFIX)


def _write_csv(path: Path, entity: str, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(RAW_COLUMNS[entity])
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _batch_dir(landing: Path, batch_date: str) -> Path:
    return landing / f"batch_date={batch_date}"


def _ts(rng: random.Random, day: date) -> datetime:
    return datetime(day.year, day.month, day.day, rng.randrange(7, 19), rng.randrange(60), rng.randrange(60))


def _postcode(rng: random.Random) -> str:
    letters = "ABDEFGHJLNPQRSTUWXYZ"
    return f"{rng.choice(OUTWARD_CODES)} {rng.randrange(10)}{rng.choice(letters)}{rng.choice(letters)}"


def _dob(rng: random.Random) -> str:
    return (date(1935, 1, 1) + timedelta(days=rng.randrange(365 * 71))).isoformat()


def _unique_by(rows: list[dict[str, str]], key: str) -> list[dict[str, str]]:
    seen, out = set(), []
    for r in rows:
        if r[key] not in seen:
            seen.add(r[key])
            out.append(r)
    return out


def generate_day1(
    landing: Path,
    batch_date: str,
    seed: int = 42,
    volumes: Volumes = Volumes(),  # noqa: B008  (frozen dataclass: the shared default is immutable)
    bad_rate: float = 0.015,
) -> dict:
    rng = random.Random(seed)
    batch = date.fromisoformat(batch_date)
    tally = _Tally()

    # --- sites and studies: clean reference data (studies get cosmetic formatting noise only) ---
    sites = [
        [f"SITE{i + 1:03d}", f"{_place(rng)} Health Partnership", rng.choice(REGIONS), rng.choice(TRUST_TYPES)]
        for i in range(volumes.sites)
    ]
    tally.valid_keys["sites"] = {r[0] for r in sites}
    site_ids = [r[0] for r in sites]

    studies = []
    for i in range(volumes.studies):
        start = FIRST_STUDY_START + timedelta(days=rng.randrange(1500))
        studies.append(
            [
                f"S{i + 1:05d}",
                f"Synthetic study {i + 1:05d}",
                rng.choice(STUDY_TYPES),
                f"{rng.choice(SPONSOR_WORDS)} {rng.choice(SPONSOR_KINDS)}",
                rng.choice(RISK_LEVELS),
                start.isoformat(),
            ]
        )
    tally.valid_keys["studies"] = {r[0] for r in studies}
    study_ids = [r[0] for r in studies]
    for r in studies[:: max(1, len(studies) // 20)]:  # cosmetic noise that cleaning must repair, not reject
        r[0], r[4] = f" {r[0].lower()} ", r[4].upper()
        tally.injected["studies"]["dirty_formatting"] += 1

    # --- participants ---
    refs = rng.sample(range(10**9, 10**10), volumes.participants)
    participants: list[list[str]] = []
    good_refs: list[str] = []
    for ref in refs:
        row = [str(ref), _dob(rng), rng.choice(["F", "M", "Other", "Unknown"]), rng.choice(ETHNICITIES), _postcode(rng)]
        kind = None
        if rng.random() < bad_rate:
            kind = rng.choice(PARTICIPANT_ERRORS + ["malformed_postcode"])
            tally.injected["participants"][kind] += 1
            if kind == "invalid_dob":
                row[1] = rng.choice(["1990-02-31", "not recorded", "31/12/1980"])
            elif kind == "implausible_age":
                row[1] = "1850-01-01"
            elif kind == "invalid_ref":
                row[0] = str(ref)[:9]
            else:  # malformed_postcode is only a warning: the row still loads
                row[4] = "ZZ99"
        elif rng.random() < 0.05:
            row[2] = rng.choice(["female ", "male", " f"])
            tally.injected["participants"]["dirty_formatting"] += 1
        if kind in (None, "malformed_postcode"):
            good_refs.append(str(ref))
        participants.append(row)
    good_ref_set = set(good_refs)
    tally.valid_keys["participants"] = set(good_ref_set)
    for row in rng.sample(participants, max(1, int(volumes.participants * 0.01))):
        participants.append(list(row))
        tally.injected["participants"]["exact_duplicate"] += 1

    # --- applications ---
    applications: list[list[str]] = []
    span = (batch - FIRST_SUBMISSION).days - 1
    for i in range(volumes.applications):
        app_id = f"APP{i + 1:06d}"
        submitted = FIRST_SUBMISSION + timedelta(days=rng.randrange(span))
        age_days = (batch - submitted).days
        finished_p = 0.97 if age_days > 120 else 0.5 if age_days > 30 else 0.1
        decision = ""
        if rng.random() < finished_p:
            status = rng.choices(["Approved", "Rejected", "Withdrawn"], weights=[72, 12, 16])[0]
            decided_on = submitted + timedelta(days=min(age_days, int(rng.triangular(10, 150, 55))))
            if status != "Withdrawn":
                decision = decided_on.isoformat()
            updated = _ts(rng, decided_on)
        else:
            status = "Submitted" if age_days < 10 else "In Review"
            updated = _ts(rng, submitted + timedelta(days=rng.randrange(max(1, min(age_days, 20)))))
        row = [
            app_id,
            rng.choice(study_ids),
            rng.choice(site_ids),
            submitted.isoformat(),
            status,
            decision,
            updated.isoformat(),
        ]
        if rng.random() < bad_rate:
            kind = rng.choice(APPLICATION_ERRORS)
            tally.injected["applications"][kind] += 1
            if kind == "null_study_id":
                row[1] = ""
            elif kind == "invalid_status":
                row[4] = "Aproved"
            elif kind == "future_submitted":
                row[3] = (batch + timedelta(days=400)).isoformat()
            elif kind == "decision_before_submission":
                row[3], row[5], row[4] = (submitted + timedelta(days=30)).isoformat(), submitted.isoformat(), "Approved"
            else:
                row[2] = "SITE999"
        else:
            tally.valid_keys["applications"].add(app_id)
            if rng.random() < 0.04:
                row[4] = f" {row[4].lower()} "
                tally.injected["applications"]["dirty_formatting"] += 1
        applications.append(row)
    for row in rng.sample(applications, max(1, int(volumes.applications * 0.01))):
        if row[0] in tally.valid_keys["applications"]:
            applications.append(list(row))
            tally.injected["applications"]["exact_duplicate"] += 1

    # --- enrolments (reference good participants, studies and sites unless deliberately broken) ---
    enrolments: list[list[str]] = []
    for i in range(volumes.enrolments):
        enr_id = f"ENR{i + 1:07d}"
        enrolled = FIRST_SUBMISSION + timedelta(days=rng.randrange(span))
        status = rng.choices(["Enrolled", "Completed", "Withdrawn"], weights=[55, 35, 10])[0]
        row = [
            enr_id,
            rng.choice(good_refs),
            rng.choice(study_ids),
            rng.choice(site_ids),
            enrolled.isoformat(),
            status,
            _ts(rng, enrolled).isoformat(),
        ]
        if rng.random() < bad_rate:
            kind = rng.choice(ENROLMENT_ERRORS)
            tally.injected["enrolments"][kind] += 1
            if kind == "unknown_study":
                row[2] = "S99999"
            elif kind == "unknown_site":
                row[3] = "SITE999"
            elif kind == "unknown_participant":
                ghost = str(rng.randrange(10**9, 10**10))
                while ghost in good_ref_set:
                    ghost = str(rng.randrange(10**9, 10**10))
                row[1] = ghost
            else:
                row[4] = (batch + timedelta(days=200)).isoformat()
        else:
            tally.valid_keys["enrolments"].add(enr_id)
        enrolments.append(row)
    for row in rng.sample(enrolments, max(1, int(volumes.enrolments * 0.005))):
        if row[0] in tally.valid_keys["enrolments"]:
            enrolments.append(list(row))
            tally.injected["enrolments"]["exact_duplicate"] += 1

    out = _batch_dir(landing, batch_date)
    for entity, rows in [
        ("sites", sites),
        ("studies", studies),
        ("participants", participants),
        ("applications", applications),
        ("enrolments", enrolments),
    ]:
        _write_csv(out / f"{entity}.csv", entity, rows)
        tally.written[entity] = len(rows)
    return _write_sidecars(out, batch_date, seed, tally, {"kind": "full_load"}, previous_valid=None)


def generate_day2(landing: Path, batch_date: str, seed: int = 43, bad_rate: float = 0.015) -> dict:
    """Incremental extract built from the most recent earlier batch found in `landing`."""
    rng = random.Random(seed)
    batch = date.fromisoformat(batch_date)
    previous = sorted(p.name.split("=")[1] for p in landing.glob("batch_date=*") if p.name.split("=")[1] < batch_date)
    if not previous:
        raise FileNotFoundError("generate_day2 needs an earlier batch in the landing folder; run day 1 first")
    prev = _batch_dir(landing, previous[-1])
    prev_valid = {k: set(v) for k, v in json.loads((prev / "_valid_keys.json").read_text()).items()}
    out = _batch_dir(landing, batch_date)
    tally = _Tally()
    extra: dict = {"kind": "incremental", "based_on": previous[-1]}

    # sites: full snapshot, a few renamed (these become type 2 history in dim_site)
    sites = _read_csv(prev / "sites.csv")
    renamed = rng.sample(range(len(sites)), min(5, len(sites)))
    for i in renamed:
        sites[i]["site_name"] = sites[i]["site_name"].replace("Health Partnership", "Integrated Care Partnership")
    extra["renamed_sites"] = sorted(sites[i]["site_id"] for i in renamed)
    site_ids = [r["site_id"] for r in sites]
    _write_csv(out / "sites.csv", "sites", [[r[c] for c in RAW_COLUMNS["sites"]] for r in sites])
    tally.valid_keys["sites"] = set(site_ids)

    # studies: a handful of retitled studies plus new ones (delta rows only)
    studies_prev = _unique_by(
        [
            {**r, "study_id": r["study_id"].strip().upper(), "risk_level": r["risk_level"].title()}
            for r in _read_csv(prev / "studies.csv")
        ],
        "study_id",
    )
    amended = rng.sample(studies_prev, min(10, len(studies_prev)))
    for r in amended:
        r["title"] += " (amended)"
    new_studies = []
    for i in range(min(20, max(2, len(studies_prev) // 100))):
        sid = f"S{len(studies_prev) + i + 1:05d}"
        new_studies.append(
            {
                "study_id": sid,
                "title": f"Synthetic study {sid[1:]}",
                "study_type": rng.choice(STUDY_TYPES),
                "sponsor": f"{rng.choice(SPONSOR_WORDS)} {rng.choice(SPONSOR_KINDS)}",
                "risk_level": rng.choice(RISK_LEVELS),
                "start_date": batch.isoformat(),
            }
        )
    _write_csv(out / "studies.csv", "studies", [[r[c] for c in RAW_COLUMNS["studies"]] for r in amended + new_studies])
    study_ids = [r["study_id"] for r in studies_prev + new_studies]
    tally.valid_keys["studies"] = {r["study_id"] for r in new_studies}
    extra["amended_studies"] = sorted(r["study_id"] for r in amended)
    extra["new_studies"] = len(new_studies)

    # applications: progress some open ones to a decision, one stale record, plus new applications
    apps_prev = _unique_by(_read_csv(prev / "applications.csv"), "application_id")
    open_apps = [
        r
        for r in apps_prev
        if r["application_id"] in prev_valid["applications"]
        and r["status"].strip().lower() in {"submitted", "in review"}
    ]
    approved = [
        r
        for r in apps_prev
        if r["application_id"] in prev_valid["applications"] and r["status"].strip().lower() == "approved"
    ]
    progressed = rng.sample(open_apps, min(len(open_apps), max(3, int(len(apps_prev) * 0.03))))
    app_rows, progressed_ids = [], {}
    for r in progressed:
        submitted = date.fromisoformat(r["submitted_date"])
        decided_on = max(submitted, batch - timedelta(days=rng.randrange(2)))
        status = rng.choices(["Approved", "Rejected"], weights=[85, 15])[0]
        app_rows.append(
            [
                r["application_id"],
                r["study_id"].strip().upper(),
                r["lead_site_id"].strip().upper(),
                r["submitted_date"],
                status,
                decided_on.isoformat(),
                _ts(rng, batch).isoformat(),
            ]
        )
        progressed_ids[r["application_id"]] = status
    stale = rng.choice(approved)  # re-sent with an OLDER timestamp and a different status: must be ignored
    app_rows.append(
        [
            stale["application_id"],
            stale["study_id"].strip().upper(),
            stale["lead_site_id"].strip().upper(),
            stale["submitted_date"],
            "In Review",
            "",
            "2000-01-01T00:00:00",
        ]
    )
    extra["progressed_applications"] = progressed_ids
    extra["stale_application"] = {"id": stale["application_id"], "expected_status": "Approved"}

    first_new = len(apps_prev) + 1
    n_new = max(3, int(len(apps_prev) * 0.02))
    for i in range(n_new):
        app_id = f"APP{first_new + i:06d}"
        submitted = batch - timedelta(days=rng.randrange(0, 3))
        row = [
            app_id,
            rng.choice(study_ids),
            rng.choice(site_ids),
            submitted.isoformat(),
            "Submitted",
            "",
            _ts(rng, submitted).isoformat(),
        ]
        if i == 0:  # always include one broken new row so day 2 exercises quarantine too
            tally.injected["applications"]["invalid_status"] += 1
            row[4] = "Aproved"
        else:
            tally.valid_keys["applications"].add(app_id)
        app_rows.append(row)
    extra["new_applications"] = n_new - 1
    _write_csv(out / "applications.csv", "applications", app_rows)

    # participants: a few new people
    prev_ref_count = len(prev_valid["participants"])
    new_refs: list[str] = []
    while len(new_refs) < max(2, prev_ref_count // 100):
        ref = str(rng.randrange(10**9, 10**10))
        if ref not in prev_valid["participants"] and ref not in new_refs:
            new_refs.append(ref)
    _write_csv(
        out / "participants.csv",
        "participants",
        [
            [ref, _dob(rng), rng.choice(["F", "M", "Other", "Unknown"]), rng.choice(ETHNICITIES), _postcode(rng)]
            for ref in new_refs
        ],
    )
    tally.valid_keys["participants"] = set(new_refs)
    extra["new_participants"] = len(new_refs)

    # enrolments: status changes on valid rows, plus new enrolments for new and existing participants
    enr_prev = _unique_by(_read_csv(prev / "enrolments.csv"), "enrolment_id")
    movable = [r for r in enr_prev if r["enrolment_id"] in prev_valid["enrolments"] and r["status"] == "Enrolled"]
    movers = rng.sample(movable, min(len(movable), max(3, int(len(enr_prev) * 0.03))))
    e_rows, moved_ids = [], {}
    for r in movers:
        new_status = rng.choice(["Completed", "Withdrawn"])
        e_rows.append(
            [
                r["enrolment_id"],
                r["participant_ref"],
                r["study_id"],
                r["site_id"],
                r["enrolled_date"],
                new_status,
                _ts(rng, batch).isoformat(),
            ]
        )
        moved_ids[r["enrolment_id"]] = new_status
    pool = new_refs + rng.sample(sorted(prev_valid["participants"]), min(50, len(prev_valid["participants"])))
    n_new_e = max(3, int(len(enr_prev) * 0.02))
    for i in range(n_new_e):
        enr_id = f"ENR{len(enr_prev) + i + 1:07d}"
        e_rows.append(
            [
                enr_id,
                rng.choice(pool),
                rng.choice(study_ids),
                rng.choice(site_ids),
                batch.isoformat(),
                "Enrolled",
                _ts(rng, batch).isoformat(),
            ]
        )
        tally.valid_keys["enrolments"].add(enr_id)
    _write_csv(out / "enrolments.csv", "enrolments", e_rows)
    extra["changed_enrolments"] = moved_ids
    extra["new_enrolments"] = n_new_e

    tally.written.update(
        {
            "sites": len(sites),
            "studies": len(amended) + len(new_studies),
            "applications": len(app_rows),
            "participants": len(new_refs),
            "enrolments": len(e_rows),
        }
    )
    return _write_sidecars(out, batch_date, seed, tally, extra, previous_valid=prev_valid)


def _write_sidecars(
    out: Path, batch_date: str, seed: int, tally: _Tally, extra: dict, previous_valid: dict[str, set[str]] | None
) -> dict:
    manifest = {
        "batch_date": batch_date,
        "seed": seed,
        "rows_written": dict(tally.written),
        "injected": {e: dict(c) for e, c in tally.injected.items() if c},
        "expected_quarantined": tally.expected_quarantined(),
        **extra,
    }
    cumulative = {e: set(v) for e, v in tally.valid_keys.items()}
    for e, keys in (previous_valid or {}).items():
        cumulative[e] = cumulative.get(e, set()) | keys
    manifest["valid_keys_total"] = {e: len(v) for e, v in cumulative.items()}
    (out / "_generation_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    (out / "_valid_keys.json").write_text(json.dumps({e: sorted(v) for e, v in cumulative.items()}), encoding="utf-8")
    return manifest


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Generate synthetic landing data (all random, no real data).")
    p.add_argument("--landing", default="data/lake/landing")
    p.add_argument("--day", choices=["1", "2"], default="1", help="1 = full historical load, 2 = incremental extract")
    p.add_argument("--batch-date", default=None, help="default: 2025-06-01 for day 1, 2025-06-02 for day 2")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--small", action="store_true", help="tiny volumes, for demos and tests")
    args = p.parse_args(argv)

    landing = Path(args.landing)
    if args.day == "1":
        volumes = Volumes.small() if args.small else Volumes()
        manifest = generate_day1(landing, args.batch_date or "2025-06-01", args.seed or 42, volumes)
        manifest["volumes"] = asdict(volumes)
    else:
        manifest = generate_day2(landing, args.batch_date or "2025-06-02", args.seed or 43)
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
