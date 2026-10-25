"""Load the data contract and derive everything from it.

    contract = load_contract()
    contract.json_schema()   -> JSON Schema for producer-side validation
    contract.spark_schema()  -> StructType for consumer-side parsing
    contract.validate(event) -> list of violations (producer side)
    breaking_changes(old, new) -> why `new` would break consumers of `old`

Keeping both sides generated from one YAML file is the point: producer and
consumer can't drift apart, and a schema change is a reviewed diff to one file.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

CONTRACT_DIR = Path(__file__).resolve().parent / "contracts"
DEFAULT_CONTRACT = CONTRACT_DIR / "ad_impression_event.yaml"

# Allowed type widenings (old -> new) that don't break readers.
WIDENINGS = {("int", "long"), ("float", "double"), ("int", "double"), ("long", "double")}


@dataclass
class Field:
    name: str
    type: str
    required: bool
    constraints: dict = field(default_factory=dict)
    description: str = ""
    pii: str | None = None


@dataclass
class Contract:
    name: str
    version: str
    topic: str
    key: str
    fields: list[Field]
    raw: dict

    @property
    def major(self) -> int:
        return int(self.version.split(".")[0])

    @property
    def required(self) -> list[str]:
        return [f.name for f in self.fields if f.required]

    def field(self, name: str) -> Field:
        return next(f for f in self.fields if f.name == name)

    # ---------------------------------------------------------------- producer
    def json_schema(self) -> dict:
        props = {}
        for f in self.fields:
            s = _json_type(f.type)
            if "minimum" in f.constraints:
                s["minimum"] = f.constraints["minimum"]
            if "pattern" in f.constraints:
                s["pattern"] = f.constraints["pattern"]
            if not f.required:
                s = {"anyOf": [s, {"type": "null"}]}
            if f.description:
                s["description"] = f.description
            props[f.name] = s
        return {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": f"urn:contract:{self.name}:{self.version}",
            "title": self.name, "type": "object",
            "properties": props, "required": self.required,
            "additionalProperties": True,      # forward compatible: unknown fields allowed
        }

    def validate(self, event: dict) -> list[str]:
        """Producer-side validation. Returns a list of violations (empty = valid)."""
        from jsonschema import Draft202012Validator, FormatChecker

        v = Draft202012Validator(self.json_schema(), format_checker=FormatChecker())
        return sorted(f"{'/'.join(map(str, e.absolute_path)) or '<root>'}: {e.message}" for e in v.iter_errors(event))

    # ---------------------------------------------------------------- consumer
    def spark_schema(self, corrupt_column: str | None = "_corrupt_record"):
        from pyspark.sql import types as T

        cols = [T.StructField(f.name, _spark_type(f.type), True) for f in self.fields]
        if corrupt_column:
            cols.append(T.StructField(corrupt_column, T.StringType(), True))
        return T.StructType(cols)


def load_contract(path: str | Path = DEFAULT_CONTRACT) -> Contract:
    raw = yaml.safe_load(Path(path).read_text())
    return _from_dict(raw)


def _from_dict(raw: dict) -> Contract:
    fields = [Field(name=f["name"], type=f["type"], required=bool(f.get("required", False)),
                    constraints=f.get("constraints", {}) or {}, description=f.get("description", ""),
                    pii=f.get("pii")) for f in raw["fields"]]
    return Contract(name=raw["contract"], version=str(raw["version"]), topic=raw["transport"]["topic"],
                    key=raw["transport"]["key"], fields=fields, raw=raw)


def _json_type(t: str) -> dict:
    if t == "string":
        return {"type": "string"}
    if t == "timestamp":
        return {"type": "string", "format": "date-time"}
    if t in ("int", "long"):
        return {"type": "integer"}
    if t in ("float", "double") or t.startswith("decimal"):
        return {"type": "number"}
    if t == "boolean":
        return {"type": "boolean"}
    raise ValueError(f"unsupported contract type: {t}")


def _spark_type(t: str):
    from pyspark.sql import types as T

    m = re.fullmatch(r"decimal\((\d+),(\d+)\)", t)
    if m:
        return T.DecimalType(int(m.group(1)), int(m.group(2)))
    return {"string": T.StringType(), "timestamp": T.TimestampType(), "int": T.IntegerType(),
            "long": T.LongType(), "float": T.FloatType(), "double": T.DoubleType(),
            "boolean": T.BooleanType()}[t]


# --------------------------------------------------------------------------- #
# Compatibility gate
# --------------------------------------------------------------------------- #
def _tightened(old: dict, new: dict) -> list[str]:
    out = []
    if "minimum" in new and ("minimum" not in old or new["minimum"] > old["minimum"]):
        out.append(f"minimum tightened to {new['minimum']}")
    if "pattern" in new and new.get("pattern") != old.get("pattern"):
        out.append("pattern changed")
    return out


def breaking_changes(old: Contract, new: Contract) -> list[str]:
    """Changes in `new` that would break consumers built against `old`."""
    problems = []
    old_f = {f.name: f for f in old.fields}
    new_f = {f.name: f for f in new.fields}
    for name, of in old_f.items():
        if name not in new_f:
            problems.append(f"field removed: {name}")
            continue
        nf = new_f[name]
        if of.type != nf.type and (of.type, nf.type) not in WIDENINGS:
            problems.append(f"type changed: {name} {of.type} -> {nf.type}")
        if not of.required and nf.required:
            problems.append(f"optional field became required: {name}")
        if of.required and not nf.required:
            problems.append(f"required field became optional: {name} (consumers may rely on it being present)")
        problems += [f"constraint on {name}: {t}" for t in _tightened(of.constraints, nf.constraints)]
    for name, nf in new_f.items():
        if name not in old_f and nf.required:
            problems.append(f"new required field: {name} (events from older producers would be rejected)")
    if old.key != new.key:
        problems.append(f"partition key changed: {old.key} -> {new.key} (breaks per-key ordering)")
    return problems


def check_evolution(old: Contract, new: Contract) -> tuple[bool, list[str]]:
    """Is `new` an acceptable successor of `old`? Returns (ok, messages)."""
    msgs = []
    ov, nv = [tuple(map(int, c.version.split("."))) for c in (old, new)]
    breaking = breaking_changes(old, new)
    changed = old.raw.get("fields") != new.raw.get("fields") or old.key != new.key
    if breaking:
        msgs += [f"BREAKING: {b}" for b in breaking]
        if nv[0] <= ov[0]:
            msgs.append(f"breaking changes need a major version bump (got {old.version} -> {new.version})")
            return False, msgs
        if new.topic == old.topic:
            msgs.append(f"a new major version must use a new topic (still {new.topic})")
            return False, msgs
        msgs.append("allowed: major version bump with a new topic; run both topics until consumers migrate")
        return True, msgs
    if changed and nv <= ov:
        msgs.append(f"schema changed but version did not increase ({old.version} -> {new.version})")
        return False, msgs
    msgs.append("compatible" + (" (no schema change)" if not changed else f": {old.version} -> {new.version}"))
    return True, msgs
