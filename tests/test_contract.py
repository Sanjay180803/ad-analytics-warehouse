"""Data contract: schema generation, producer validation, and the compatibility gate."""
import copy
import os
import sys
from datetime import datetime, timezone

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "include", "streaming"))

from contract import _from_dict, breaking_changes, check_evolution, load_contract  # noqa: E402
import producer  # noqa: E402

C = load_contract()
ANCHOR = datetime(2025, 1, 1, tzinfo=timezone.utc)
RAW = {"timestamp": "3600", "uid": "12345", "campaign": "70003", "conversion": "0", "conversion_timestamp": "-1",
       "conversion_id": "-1", "attribution": "0", "click": "1", "click_pos": "0", "click_nb": "1", "cost": "0.00123",
       "cpo": "-1", "time_since_last_click": "-1", **{f"cat{i}": f"c{i}" for i in range(1, 10)}}


def evolve(mutate, version=None, topic=None):
    raw = copy.deepcopy(C.raw)
    mutate(raw)
    if version:
        raw["version"] = version
    if topic:
        raw["transport"]["topic"] = topic
    return _from_dict(raw)


def test_contract_basics():
    assert C.name == "ad_impression_event" and C.major == 1
    assert C.topic.endswith(".v1"), "major version belongs in the topic name"
    assert {"impression_id", "event_ts", "user_id", "campaign_id", "cost"} <= set(C.required)
    assert C.field("user_id").pii == "user_id"


def test_producer_event_is_valid_and_matches_batch_semantics():
    ev = producer.to_event(RAW, ANCHOR, C.version)
    assert C.validate(ev) == []
    assert ev["event_ts"].startswith("2025-01-01T01:00:00")
    assert ev["conversion_id"] is None and ev["cpo"] is None      # -1 sentinels -> null, like the batch job
    assert len(ev["impression_id"]) == 64


@pytest.mark.parametrize("fault,expect_schema_error", [
    ("missing_required", True), ("wrong_type", True), ("negative_cost", True), ("bad_version", True),
    # these two are consumer-side quality rules, not schema rules
    ("unknown_campaign", False), ("future_event", False),
])
def test_faults_vs_producer_validation(fault, expect_schema_error):
    import json
    ev = producer.to_event(RAW, ANCHOR, C.version)
    bad = json.loads(producer._inject(ev, fault))
    assert bool(C.validate(bad)) == expect_schema_error


def test_unknown_fields_are_allowed_forward_compatible():
    ev = producer.to_event(RAW, ANCHOR, C.version) | {"new_field_from_v1_1": "x"}
    assert C.validate(ev) == []


def test_raw_columns_match_batch_job():
    sys.path.insert(0, os.path.join(ROOT, "include", "spark"))
    from common import RAW_COLUMNS
    assert producer.RAW_COLUMNS == RAW_COLUMNS


def test_spark_schema_types():
    from pyspark.sql import types as T
    s = C.spark_schema()
    types = {f.name: f.dataType for f in s.fields}
    assert types["campaign_id"] == T.LongType() and types["event_ts"] == T.TimestampType()
    assert types["cost"] == T.DecimalType(18, 8) and "_corrupt_record" in types


# ---------------------------------------------------------------- compatibility
def test_unchanged_contract_is_compatible():
    assert check_evolution(C, C)[0]


def test_adding_optional_field_with_minor_bump_is_allowed():
    new = evolve(lambda r: r["fields"].append({"name": "placement", "type": "string", "required": False}), "1.1.0")
    ok, msgs = check_evolution(C, new)
    assert ok and not breaking_changes(C, new)


def test_schema_change_without_version_bump_is_blocked():
    new = evolve(lambda r: r["fields"].append({"name": "placement", "type": "string", "required": False}))
    assert not check_evolution(C, new)[0]


@pytest.mark.parametrize("mutate,needle", [
    (lambda r: r["fields"].pop(next(i for i, f in enumerate(r["fields"]) if f["name"] == "cat9")), "field removed"),
    (lambda r: r["fields"].append({"name": "region", "type": "string", "required": True}), "new required field"),
    (lambda r: next(f for f in r["fields"] if f["name"] == "campaign_id").update(type="string"), "type changed"),
    (lambda r: next(f for f in r["fields"] if f["name"] == "click_pos").update(required=True), "became required"),
    (lambda r: next(f for f in r["fields"] if f["name"] == "user_id").update(required=False), "became optional"),
    (lambda r: next(f for f in r["fields"] if f["name"] == "click_nb").update(constraints={"minimum": 1}), "tightened"),
    (lambda r: r["transport"].update(key="user_id"), "partition key changed"),
])
def test_breaking_changes_are_blocked_without_major_bump(mutate, needle):
    new = evolve(mutate, "1.1.0")
    ok, msgs = check_evolution(C, new)
    assert not ok and any(needle in m for m in msgs), msgs


def test_type_widening_is_compatible():
    new = evolve(lambda r: next(f for f in r["fields"] if f["name"] == "click_pos").update(type="long"), "1.1.0")
    assert check_evolution(C, new)[0]


def test_major_bump_needs_a_new_topic():
    drop = lambda r: r["fields"].pop(next(i for i, f in enumerate(r["fields"]) if f["name"] == "cat9"))  # noqa: E731
    assert not check_evolution(C, evolve(drop, "2.0.0"))[0]
    ok, msgs = check_evolution(C, evolve(drop, "2.0.0", topic="ad.impressions.v2"))
    assert ok and any("new topic" in m for m in msgs)
