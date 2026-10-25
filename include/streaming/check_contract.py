"""CI gate: is the contract on this branch a safe successor of the one on main?

    python include/streaming/check_contract.py OLD.yaml NEW.yaml

Exits 1 on a breaking change without a major version bump and a new topic,
or on a schema change without any version bump.
"""
import sys

from contract import check_evolution, load_contract


def main() -> int:
    old, new = load_contract(sys.argv[1]), load_contract(sys.argv[2])
    ok, msgs = check_evolution(old, new)
    for m in msgs:
        print(("  " if ok else "::error::") + m)
    print(f"{old.name}: {old.version} -> {new.version}: {'OK' if ok else 'BLOCKED'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
