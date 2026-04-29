"""
Test suite for the Spec Parser.

Tests three layers:
1. Happy path — all 4 real specs validate clean
2. Error detection — synthetic broken specs caught correctly
3. Cross-field consistency — chain_invokes / memory namespace checks
"""
from __future__ import annotations
import sys
import json
import os
from pathlib import Path

# Add parser to path
sys.path.insert(0, str(Path(__file__).parent.parent / "parser"))
from spec_parser import parse_spec, validate_spec, load_spec, SpecParseError

SPECS_DIR = Path(__file__).parent.parent / "specs"
PASS_COUNT = 0
FAIL_COUNT = 0


def test(name: str, condition: bool, detail: str = ""):
    """Simple test runner — track pass/fail."""
    global PASS_COUNT, FAIL_COUNT
    if condition:
        PASS_COUNT += 1
        print(f"  ✅ {name}")
    else:
        FAIL_COUNT += 1
        print(f"  ❌ {name}")
        if detail:
            print(f"     {detail}")


# ============================================================
# LAYER 1 — HAPPY PATH (4 REAL SPECS)
# ============================================================

print("\n" + "=" * 70)
print("LAYER 1 — HAPPY PATH (4 REAL SPECS)")
print("=" * 70)

real_specs = [
    "agent-01-free-website-audit",
    "agent-22-video-engine",
    "agent-26-pipeline-monitor",
    "agent-28-results-amplifier",
]

for name in real_specs:
    path = SPECS_DIR / f"{name}.spec.json"
    spec, errors = parse_spec(path)
    test(
        f"{name} parses clean",
        spec is not None and len(errors) == 0,
        f"errors: {errors[:3] if errors else ''}"
    )


# ============================================================
# LAYER 2 — ERROR DETECTION (SYNTHETIC BROKEN SPECS)
# ============================================================

print("\n" + "=" * 70)
print("LAYER 2 — ERROR DETECTION (SYNTHETIC BROKEN SPECS)")
print("=" * 70)

# Load a known-good spec as template, mutate to break it
def _mutate(field_path: list[str], value):
    """Load Agent-01 spec, mutate one field, return broken version."""
    with open(SPECS_DIR / "agent-01-free-website-audit.spec.json") as f:
        spec = json.load(f)
    
    target = spec
    for p in field_path[:-1]:
        target = target[p]
    target[field_path[-1]] = value
    return spec


# Test: missing required field
spec = _mutate(["agent_id"], None)
del spec["agent_id"]
errors = validate_spec(spec)
test(
    "missing agent_id detected",
    any("agent_id" in e for e in errors),
    f"errors: {errors}"
)


# Test: wrong spec_version
spec = _mutate(["spec_version"], "0.9")
errors = validate_spec(spec)
test(
    "wrong spec_version detected",
    any("spec_version" in e for e in errors),
    f"errors: {errors}"
)


# Test: bad agent_name (kebab-case)
spec = _mutate(["agent_name"], "free-website-audit")
errors = validate_spec(spec)
test(
    "kebab-case agent_name rejected",
    any("agent_name" in e and "camelCase" in e for e in errors),
    f"errors: {errors}"
)


# Test: bad agent_name (UPPERCASE)
spec = _mutate(["agent_name"], "FreeWebsiteAudit")
errors = validate_spec(spec)
test(
    "PascalCase agent_name rejected",
    any("agent_name" in e for e in errors),
    f"errors: {errors}"
)


# Test: bad tenant_id format
spec = _mutate(["tenant_id"], "tenant_001")  # underscore not hyphen
errors = validate_spec(spec)
test(
    "wrong tenant_id format rejected",
    any("tenant_id" in e for e in errors),
    f"errors: {errors}"
)


# Test: invalid phase
spec = _mutate(["phase"], "phase3")
errors = validate_spec(spec)
test(
    "invalid phase rejected",
    any("phase" in e for e in errors),
    f"errors: {errors}"
)


# Test: invalid trigger type
spec = _mutate(["trigger", "type"], "websocket")
errors = validate_spec(spec)
test(
    "invalid trigger type rejected",
    any("trigger.type" in e for e in errors),
    f"errors: {errors}"
)


# Test: cron without schedule
spec = _mutate(["trigger", "type"], "cron")
spec["trigger"].pop("schedule", None)
errors = validate_spec(spec)
test(
    "cron without schedule rejected",
    any("schedule" in e for e in errors),
    f"errors: {errors}"
)


# Test: bad path format (no leading /)
spec = _mutate(["paths", 0, "path"], "audit/website")
errors = validate_spec(spec)
test(
    "path without leading / rejected",
    any("path" in e and "start with /" in e for e in errors),
    f"errors: {errors}"
)


# Test: bad handler name
spec = _mutate(["paths", 0, "handler"], "doAudit")  # not handle_*
errors = validate_spec(spec)
test(
    "handler not matching handle_* rejected",
    any("handler" in e for e in errors),
    f"errors: {errors}"
)


# Test: SSM with hyphen
with open(SPECS_DIR / "agent-01-free-website-audit.spec.json") as f:
    spec = json.load(f)
spec["ssm_params"]["required"] = ["GHL-API-KEY"]
errors = validate_spec(spec)
test(
    "SSM name with hyphen rejected",
    any("UPPER_SNAKE_CASE" in e for e in errors),
    f"errors: {errors}"
)


# Test: invalid LLM complexity tier
with open(SPECS_DIR / "agent-28-results-amplifier.spec.json") as f:
    spec = json.load(f)
spec["llm_tasks"][0]["complexity"] = "MEDIUM"
errors = validate_spec(spec)
test(
    "invalid complexity tier rejected",
    any("complexity" in e for e in errors),
    f"errors: {errors}"
)


# Test: invalid API name
spec = _mutate(["apis"], ["ghl", "fake_api"])
errors = validate_spec(spec)
test(
    "unknown API rejected",
    any("apis" in e for e in errors),
    f"errors: {errors}"
)


# Test: extra top-level field
spec = _mutate(["claude"], {"uses_claude": True})  # old block
errors = validate_spec(spec)
test(
    "extra top-level field 'claude' rejected",
    any("claude" in e for e in errors),
    f"errors: {errors}"
)


# ============================================================
# LAYER 3 — CROSS-FIELD CONSISTENCY
# ============================================================

print("\n" + "=" * 70)
print("LAYER 3 — CROSS-FIELD CONSISTENCY")
print("=" * 70)


# Test: chain_invoke ssm_arn_key not in ssm_params
with open(SPECS_DIR / "agent-28-results-amplifier.spec.json") as f:
    spec = json.load(f)
# Remove the ssm_arn_key from ssm_params (it's normally in optional)
spec["ssm_params"]["optional"] = [
    k for k in spec["ssm_params"].get("optional", [])
    if k != "VIDEO_ENGINE_RUNTIME_ARN"
]
errors = validate_spec(spec)
test(
    "chain_invoke ssm_arn_key missing from ssm_params detected",
    any("VIDEO_ENGINE_RUNTIME_ARN" in e for e in errors),
    f"errors: {errors}"
)


# Test: memory.namespace_key not in any path's inputs
with open(SPECS_DIR / "agent-01-free-website-audit.spec.json") as f:
    spec = json.load(f)
# namespace_key is "email" — change it to a non-existent field
spec["memory"]["namespace_key"] = "nonexistent_field"
errors = validate_spec(spec)
test(
    "memory.namespace_key missing from path inputs detected",
    any("nonexistent_field" in e for e in errors),
    f"errors: {errors}"
)


# Test: memory enabled without strategy
with open(SPECS_DIR / "agent-01-free-website-audit.spec.json") as f:
    spec = json.load(f)
spec["memory"].pop("strategy")
errors = validate_spec(spec)
test(
    "memory enabled without strategy detected",
    any("strategy" in e for e in errors),
    f"errors: {errors}"
)


# ============================================================
# LAYER 4 — FILE LOADING ERRORS
# ============================================================

print("\n" + "=" * 70)
print("LAYER 4 — FILE LOADING ERRORS")
print("=" * 70)


# Test: nonexistent file
try:
    load_spec("/tmp/does_not_exist.json")
    test("nonexistent file raises error", False)
except SpecParseError:
    test("nonexistent file raises error", True)


# Test: invalid JSON
import tempfile
with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
    f.write("{not valid json,,,")
    tmp = f.name
try:
    load_spec(tmp)
    test("invalid JSON raises error", False)
except SpecParseError as e:
    test("invalid JSON raises error", True)
finally:
    os.unlink(tmp)


# Test: not an object
with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
    f.write('["this", "is", "an", "array"]')
    tmp = f.name
try:
    load_spec(tmp)
    test("non-object JSON raises error", False)
except SpecParseError:
    test("non-object JSON raises error", True)
finally:
    os.unlink(tmp)


# ============================================================
# SUMMARY
# ============================================================

print("\n" + "=" * 70)
print("TEST SUMMARY")
print("=" * 70)
print(f"  Passed : {PASS_COUNT}")
print(f"  Failed : {FAIL_COUNT}")
print(f"  Total  : {PASS_COUNT + FAIL_COUNT}")
print()

if FAIL_COUNT == 0:
    print("🎉 ALL TESTS PASS — Parser is production-ready")
    sys.exit(0)
else:
    print(f"❌ {FAIL_COUNT} test(s) failed — review output above")
    sys.exit(1)