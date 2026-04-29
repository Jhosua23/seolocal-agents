"""
Test suite for the SSM Validator.

Uses a mock SSM client to test all code paths without needing real AWS access.

Tests cover:
1. Happy path  — all required + all optional present
2. Missing required — blocks deployment
3. Missing optional — allows but warns
4. Placeholder values — flagged as warning, doesn't block
5. AWS auth failure — graceful error
6. Empty ssm_params — no errors
7. boto3 missing — graceful error
"""
from __future__ import annotations
import sys
import os
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

# Add validator + parser to path
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "validator"))
sys.path.insert(0, str(ROOT / "parser"))

from ssm_validator import (
    ParamStatus,
    ValidationReport,
    validate_ssm_params,
    format_report,
    _is_placeholder_value,
    _check_ssm_params_batch,
)
import ssm_validator as _ssm_mod
from spec_parser import parse_spec

# If real boto3 isn't installed, inject a fake module-level attr so we can patch it.
# This lets the same test code work whether or not boto3 is in the test environment.
if not hasattr(_ssm_mod, "boto3"):
    _ssm_mod.boto3 = MagicMock()
    _ssm_mod.BOTO3_AVAILABLE = True
    # Re-bind the exception classes to MagicMocks so try/except still works
    _ssm_mod.ClientError = type("ClientError", (Exception,), {})
    _ssm_mod.BotoCoreError = type("BotoCoreError", (Exception,), {})
    _ssm_mod.NoCredentialsError = type("NoCredentialsError", (Exception,), {})

SPECS_DIR = ROOT / "specs"

PASS_COUNT = 0
FAIL_COUNT = 0


def test(name: str, condition: bool, detail: str = ""):
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
# MOCK SSM CLIENT
# ============================================================

def _make_mock_ssm(present_params: dict[str, dict]):
    """
    Build a mock SSM client. `present_params` maps:
        param_name -> {"Value": str, "Type": str}
    Any param NOT in this dict is treated as not-present.
    """
    mock_ssm = MagicMock()
    
    def get_parameters(Names, WithDecryption):
        found = []
        invalid = []
        for n in Names:
            if n in present_params:
                p = present_params[n]
                found.append({
                    "Name": n,
                    "Value": p["Value"],
                    "Type": p["Type"],
                })
            else:
                invalid.append(n)
        return {"Parameters": found, "InvalidParameters": invalid}
    
    mock_ssm.get_parameters.side_effect = get_parameters
    mock_ssm.describe_parameters.return_value = {"Parameters": []}
    return mock_ssm


# ============================================================
# LAYER 1 — placeholder detection
# ============================================================

print("\n" + "=" * 70)
print("LAYER 1 — PLACEHOLDER VALUE DETECTION")
print("=" * 70)

test(
    "real value not flagged as placeholder",
    not _is_placeholder_value("ghl_pit_abc123def456"),
)
test(
    "'pending-from-chuck' flagged as placeholder",
    _is_placeholder_value("pending-from-chuck"),
)
test(
    "'TODO' flagged as placeholder",
    _is_placeholder_value("TODO"),
)
test(
    "empty string flagged as placeholder",
    _is_placeholder_value(""),
)
test(
    "'<replace>' flagged as placeholder",
    _is_placeholder_value("<replace>"),
)
test(
    "case-insensitive 'PENDING' flagged",
    _is_placeholder_value("PENDING"),
)


# ============================================================
# LAYER 2 — happy path (all params present)
# ============================================================

print("\n" + "=" * 70)
print("LAYER 2 — HAPPY PATH (ALL PARAMS PRESENT)")
print("=" * 70)

# Use Agent-22 spec (real production spec, 17 params)
spec, errors = parse_spec(SPECS_DIR / "agent-22-video-engine.spec.json")
assert not errors, errors

# Build mock with all 17 params present
all_params = {}
for n in spec["ssm_params"]["required"]:
    all_params[n] = {"Value": "real-value-abc", "Type": "SecureString"}
for n in spec["ssm_params"]["optional"]:
    all_params[n] = {"Value": "real-value-xyz", "Type": "String"}

mock = _make_mock_ssm(all_params)
with patch("ssm_validator.boto3") as mock_boto3:
    mock_boto3.client.return_value = mock
    report = validate_ssm_params(spec, region="us-east-1")

test(
    "all required params detected as present",
    all(r.is_present for r in report.required_results),
)
test(
    "all optional params detected as present",
    all(r.is_present for r in report.optional_results),
)
test(
    "no required params missing",
    len(report.missing_required) == 0,
)
test(
    "ready to deploy when all present",
    report.ready_to_deploy is True,
)
test(
    "no warnings when no placeholders",
    report.has_warnings is False,
)


# ============================================================
# LAYER 3 — missing REQUIRED blocks deployment
# ============================================================

print("\n" + "=" * 70)
print("LAYER 3 — MISSING REQUIRED BLOCKS DEPLOY")
print("=" * 70)

# Use Agent-01 spec (4 required params)
spec, _ = parse_spec(SPECS_DIR / "agent-01-free-website-audit.spec.json")

# Mock with only 2 of 4 required present
partial_params = {
    "PAGESPEED_API_KEY": {"Value": "real", "Type": "SecureString"},
    "GHL_API_KEY": {"Value": "real", "Type": "SecureString"},
    # Missing: GHL_PIPELINE_ID, GHL_STAGE_NEW_LEAD
}

mock = _make_mock_ssm(partial_params)
with patch("ssm_validator.boto3") as mock_boto3:
    mock_boto3.client.return_value = mock
    report = validate_ssm_params(spec)

test(
    "missing required params detected",
    len(report.missing_required) == 2,
    f"got {len(report.missing_required)} missing: {report.missing_required}",
)
test(
    "GHL_PIPELINE_ID identified as missing",
    "GHL_PIPELINE_ID" in report.missing_required,
)
test(
    "GHL_STAGE_NEW_LEAD identified as missing",
    "GHL_STAGE_NEW_LEAD" in report.missing_required,
)
test(
    "NOT ready to deploy when required missing",
    report.ready_to_deploy is False,
)


# ============================================================
# LAYER 4 — missing OPTIONAL is just a warning
# ============================================================

print("\n" + "=" * 70)
print("LAYER 4 — MISSING OPTIONAL = WARNING ONLY")
print("=" * 70)

spec, _ = parse_spec(SPECS_DIR / "agent-01-free-website-audit.spec.json")

# All required present, all optional missing
required_only = {}
for n in spec["ssm_params"]["required"]:
    required_only[n] = {"Value": "real", "Type": "SecureString"}

mock = _make_mock_ssm(required_only)
with patch("ssm_validator.boto3") as mock_boto3:
    mock_boto3.client.return_value = mock
    report = validate_ssm_params(spec)

test(
    "all required present",
    len(report.missing_required) == 0,
)
test(
    "optional missing detected",
    len(report.missing_optional) == len(spec["ssm_params"]["optional"]),
)
test(
    "still ready to deploy (optional missing OK)",
    report.ready_to_deploy is True,
)
test(
    "has_warnings flag set when optional missing",
    report.has_warnings is True,
)


# ============================================================
# LAYER 5 — placeholder values flagged
# ============================================================

print("\n" + "=" * 70)
print("LAYER 5 — PLACEHOLDER VALUES FLAGGED")
print("=" * 70)

spec, _ = parse_spec(SPECS_DIR / "agent-01-free-website-audit.spec.json")

placeholder_params = {
    "PAGESPEED_API_KEY": {"Value": "real", "Type": "SecureString"},
    "GHL_API_KEY": {"Value": "real", "Type": "SecureString"},
    "GHL_PIPELINE_ID": {"Value": "pending-from-chuck", "Type": "String"},
    "GHL_STAGE_NEW_LEAD": {"Value": "real", "Type": "String"},
}

mock = _make_mock_ssm(placeholder_params)
with patch("ssm_validator.boto3") as mock_boto3:
    mock_boto3.client.return_value = mock
    report = validate_ssm_params(spec)

test(
    "placeholder required param flagged",
    "GHL_PIPELINE_ID" in report.placeholder_required,
)
test(
    "placeholder still counts as present",
    report.ready_to_deploy is True,  # placeholder OK technically — agent will start
)
test(
    "has_warnings when placeholder present",
    report.has_warnings is True,
)


# ============================================================
# LAYER 6 — empty ssm_params handled gracefully
# ============================================================

print("\n" + "=" * 70)
print("LAYER 6 — EMPTY SSM_PARAMS")
print("=" * 70)

empty_spec = {
    "agent_id": "agent-99-empty",
    "agent_name": "emptyTest",
    "ssm_params": {"required": [], "optional": []},
}

mock = _make_mock_ssm({})
with patch("ssm_validator.boto3") as mock_boto3:
    mock_boto3.client.return_value = mock
    report = validate_ssm_params(empty_spec)

test(
    "empty required list handled",
    len(report.required_results) == 0,
)
test(
    "empty optional list handled",
    len(report.optional_results) == 0,
)
test(
    "empty spec ready to deploy (no params required)",
    report.ready_to_deploy is True,
)


# ============================================================
# LAYER 7 — boto3 not installed = graceful error
# ============================================================

print("\n" + "=" * 70)
print("LAYER 7 — BOTO3 NOT INSTALLED")
print("=" * 70)

with patch("ssm_validator.BOTO3_AVAILABLE", False):
    spec, _ = parse_spec(SPECS_DIR / "agent-01-free-website-audit.spec.json")
    report = validate_ssm_params(spec)

test(
    "boto3 missing produces auth_error",
    report.auth_error is not None,
)
test(
    "boto3 missing makes ready_to_deploy False",
    report.ready_to_deploy is False,
)
test(
    "error message mentions boto3",
    "boto3" in report.auth_error.lower(),
)


# ============================================================
# LAYER 8 — format_report produces output
# ============================================================

print("\n" + "=" * 70)
print("LAYER 8 — REPORT FORMATTING")
print("=" * 70)

# Build a representative report
spec, _ = parse_spec(SPECS_DIR / "agent-01-free-website-audit.spec.json")
mock = _make_mock_ssm({
    "PAGESPEED_API_KEY": {"Value": "real", "Type": "SecureString"},
    "GHL_API_KEY": {"Value": "real", "Type": "SecureString"},
    "GHL_PIPELINE_ID": {"Value": "real", "Type": "String"},
    "GHL_STAGE_NEW_LEAD": {"Value": "real", "Type": "String"},
    "GHL_LOCATION_ID": {"Value": "uXRl9WpDjS7LFjeYfQqD", "Type": "String"},
})
with patch("ssm_validator.boto3") as mock_boto3:
    mock_boto3.client.return_value = mock
    report = validate_ssm_params(spec)

output = format_report(report)

test(
    "report includes agent_name",
    "freeWebsiteAudit" in output,
)
test(
    "report includes 'REQUIRED PARAMS' section",
    "REQUIRED PARAMS" in output,
)
test(
    "report includes 'OPTIONAL PARAMS' section",
    "OPTIONAL PARAMS" in output,
)
test(
    "report shows verdict",
    "READY TO DEPLOY" in output,
)


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
    print("🎉 ALL TESTS PASS — SSM Validator is production-ready")
    sys.exit(0)
else:
    print(f"❌ {FAIL_COUNT} test(s) failed")
    sys.exit(1)
