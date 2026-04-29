"""
Agent Factory — Component 4: SSM Validator
============================================

Reads a parsed spec, queries AWS SSM Parameter Store, and confirms every
required parameter exists before agent deployment. Optional params are
warned but don't block.

This is the LAST gate before code generation/deployment. Catching missing
SSM params here saves 30 minutes of "agent deployed but crashes at cold start
because GHL_API_KEY isn't set" debug pain.

Usage:
    # As CLI
    python ssm_validator.py specs/agent-01-free-website-audit.spec.json
    
    # As library
    from ssm_validator import validate_ssm_params
    
    spec, errors = parse_spec(path)
    if errors: ...
    
    report = validate_ssm_params(spec, region="us-east-1")
    if not report.ready_to_deploy:
        print("BLOCKED:", report.missing_required)


How it interprets ssm_params:

    Two naming conventions are supported:
    
    1. Flat names (preferred for new agents):
       GHL_API_KEY, PAGESPEED_API_KEY, etc.
       Looked up directly at SSM root.
    
    2. Path-style names (existing agents):
       /seolocal/agent13/CLAUDE_MODEL_ID
       Looked up at exact path.

Author: Jhosua, April 29, 2026
"""

from __future__ import annotations
import json
import sys
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Add parser to import path so we can call parse_spec()
_PARSER_PATH = Path(__file__).parent.parent / "parser"
if str(_PARSER_PATH) not in sys.path:
    sys.path.insert(0, str(_PARSER_PATH))

# boto3 is REQUIRED — fail fast if missing
try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError, NoCredentialsError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False


# ============================================================
# REPORT DATA STRUCTURES
# ============================================================

@dataclass
class ParamStatus:
    """Result of checking a single SSM parameter."""
    name: str
    is_present: bool
    is_required: bool
    param_type: str | None = None      # String | StringList | SecureString
    error: str | None = None            # If lookup itself failed (auth, network)
    is_placeholder: bool = False        # Value contains "pending" or similar
    
    @property
    def status_label(self) -> str:
        if self.error:
            return "ERROR"
        if not self.is_present:
            return "MISSING"
        if self.is_placeholder:
            return "PLACEHOLDER"
        return "SET"


@dataclass
class ValidationReport:
    """Full report from validating all SSM params for one agent."""
    agent_id: str
    agent_name: str
    region: str
    required_results: list[ParamStatus] = field(default_factory=list)
    optional_results: list[ParamStatus] = field(default_factory=list)
    auth_error: str | None = None      # If we can't even talk to AWS
    
    @property
    def missing_required(self) -> list[str]:
        return [r.name for r in self.required_results if not r.is_present]
    
    @property
    def missing_optional(self) -> list[str]:
        return [r.name for r in self.optional_results if not r.is_present]
    
    @property
    def placeholder_required(self) -> list[str]:
        return [r.name for r in self.required_results if r.is_placeholder]
    
    @property
    def ready_to_deploy(self) -> bool:
        """True if all required params are present (placeholders allowed)."""
        if self.auth_error:
            return False
        return len(self.missing_required) == 0
    
    @property
    def has_warnings(self) -> bool:
        return (
            len(self.missing_optional) > 0
            or len(self.placeholder_required) > 0
        )


# ============================================================
# SSM CLIENT — CACHED, CHUNKED LOOKUPS
# ============================================================

# Words that suggest a param has placeholder-not-real value
_PLACEHOLDER_INDICATORS = (
    "pending-from-chuck",
    "pending-from-",
    "TODO",
    "PLACEHOLDER",
    "pending",
    "<replace>",
    "<insert>",
)


def _is_placeholder_value(value: str) -> bool:
    """Heuristic: does this SSM value look like a placeholder vs real data?"""
    if not value:
        return True
    lower = value.lower()
    return any(p.lower() in lower for p in _PLACEHOLDER_INDICATORS)


def _check_ssm_params_batch(
    param_names: list[str],
    ssm_client,
) -> dict[str, ParamStatus]:
    """
    Look up multiple SSM params in a single API call (10 at a time max).
    Returns a dict mapping name -> ParamStatus.
    """
    results: dict[str, ParamStatus] = {}
    if not param_names:
        return results
    
    # SSM get_parameters supports up to 10 names per call
    BATCH_SIZE = 10
    
    for i in range(0, len(param_names), BATCH_SIZE):
        batch = param_names[i:i + BATCH_SIZE]
        
        try:
            response = ssm_client.get_parameters(
                Names=batch,
                WithDecryption=True,
            )
        except (ClientError, BotoCoreError) as e:
            # If batch fails entirely, mark all in batch as error
            err_msg = f"AWS API error: {type(e).__name__}: {str(e)[:100]}"
            for name in batch:
                results[name] = ParamStatus(
                    name=name,
                    is_present=False,
                    is_required=False,
                    error=err_msg,
                )
            continue
        
        # Found params
        for p in response.get("Parameters", []):
            name = p["Name"]
            value = p.get("Value", "")
            results[name] = ParamStatus(
                name=name,
                is_present=True,
                is_required=False,  # Caller fills this in
                param_type=p.get("Type"),
                is_placeholder=_is_placeholder_value(value),
            )
        
        # Not-found params (in InvalidParameters list)
        for name in response.get("InvalidParameters", []):
            results[name] = ParamStatus(
                name=name,
                is_present=False,
                is_required=False,
            )
    
    return results


# ============================================================
# CORE VALIDATION
# ============================================================

def validate_ssm_params(
    spec: dict[str, Any],
    region: str = "us-east-1",
) -> ValidationReport:
    """
    Validate all SSM params declared in spec exist in AWS.
    
    Args:
        spec: Already-parsed spec dict (from spec_parser.parse_spec)
        region: AWS region to query
    
    Returns:
        ValidationReport with per-param results.
    """
    report = ValidationReport(
        agent_id=spec.get("agent_id", "unknown"),
        agent_name=spec.get("agent_name", "unknown"),
        region=region,
    )
    
    if not BOTO3_AVAILABLE:
        report.auth_error = (
            "boto3 not installed. Install with: pip install boto3"
        )
        return report
    
    # Build SSM client with region
    try:
        ssm = boto3.client("ssm", region_name=region)
    except NoCredentialsError:
        report.auth_error = (
            "AWS credentials not configured. Run 'aws configure' or set "
            "AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY env vars."
        )
        return report
    except Exception as e:
        report.auth_error = f"Failed to create SSM client: {type(e).__name__}: {e}"
        return report
    
    # Sanity check: can we talk to AWS at all?
    try:
        # Cheap test — lists at most 1 param
        ssm.describe_parameters(MaxResults=1)
    except NoCredentialsError:
        report.auth_error = (
            "AWS credentials not found. Run 'aws configure' or set "
            "AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY."
        )
        return report
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        if code == "AccessDeniedException":
            report.auth_error = (
                f"AWS credentials valid but missing IAM permission "
                f"ssm:DescribeParameters / ssm:GetParameters. "
                f"Need ssm:Get* on {region}."
            )
        else:
            report.auth_error = f"AWS error: {code}: {e}"
        return report
    except BotoCoreError as e:
        report.auth_error = f"AWS network/config error: {e}"
        return report
    
    # Read declared params from spec
    ssm_params = spec.get("ssm_params", {})
    required_names = ssm_params.get("required", []) or []
    optional_names = ssm_params.get("optional", []) or []
    
    # Look up REQUIRED
    if required_names:
        required_results = _check_ssm_params_batch(required_names, ssm)
        for name in required_names:
            status = required_results.get(name) or ParamStatus(
                name=name,
                is_present=False,
                is_required=True,
            )
            status.is_required = True
            report.required_results.append(status)
    
    # Look up OPTIONAL
    if optional_names:
        optional_results = _check_ssm_params_batch(optional_names, ssm)
        for name in optional_names:
            status = optional_results.get(name) or ParamStatus(
                name=name,
                is_present=False,
                is_required=False,
            )
            status.is_required = False
            report.optional_results.append(status)
    
    return report


# ============================================================
# SPEC PARSING WRAPPER
# ============================================================

def validate_ssm_for_spec_file(
    spec_path: str | Path,
    region: str = "us-east-1",
) -> tuple[ValidationReport | None, list[str]]:
    """
    Combined: parse spec file + validate SSM. Top-level entrypoint.
    
    Returns:
        (report, []) if spec parses
        (None, [errors...]) if spec fails to parse
    """
    try:
        from spec_parser import parse_spec
    except ImportError as e:
        return None, [f"Could not import spec_parser: {e}"]
    
    spec, errors = parse_spec(spec_path)
    if errors:
        return None, errors
    
    report = validate_ssm_params(spec, region=region)
    return report, []


# ============================================================
# REPORTING — pretty CLI output
# ============================================================

def format_report(report: ValidationReport) -> str:
    """Format a ValidationReport for human reading."""
    lines = []
    lines.append("=" * 72)
    lines.append(f"  SSM VALIDATION — {report.agent_name}")
    lines.append(f"  agent_id : {report.agent_id}")
    lines.append(f"  region   : {report.region}")
    lines.append("=" * 72)
    
    if report.auth_error:
        lines.append("")
        lines.append("❌ CANNOT VALIDATE — AWS auth/access error:")
        lines.append(f"   {report.auth_error}")
        lines.append("")
        return "\n".join(lines)
    
    # Required section
    lines.append("")
    lines.append(f"REQUIRED PARAMS ({len(report.required_results)}):")
    if not report.required_results:
        lines.append("  (none declared)")
    for r in report.required_results:
        if r.is_present and not r.is_placeholder:
            symbol = "✅"
        elif r.is_present and r.is_placeholder:
            symbol = "⚠️ "
        elif r.error:
            symbol = "❌"
        else:
            symbol = "❌"
        
        type_str = (r.param_type or "").ljust(13)
        status_str = r.status_label.ljust(13)
        line = f"  {symbol} {r.name.ljust(40)} {type_str} {status_str}"
        if r.error:
            line += f"  ({r.error})"
        lines.append(line)
    
    # Optional section
    lines.append("")
    lines.append(f"OPTIONAL PARAMS ({len(report.optional_results)}):")
    if not report.optional_results:
        lines.append("  (none declared)")
    for r in report.optional_results:
        if r.is_present and not r.is_placeholder:
            symbol = "✅"
        elif r.is_present and r.is_placeholder:
            symbol = "⚠️ "
        elif r.error:
            symbol = "❌"
        else:
            symbol = "⚪"  # Missing optional — informational
        
        type_str = (r.param_type or "").ljust(13)
        status_str = r.status_label.ljust(13)
        line = f"  {symbol} {r.name.ljust(40)} {type_str} {status_str}"
        if r.error:
            line += f"  ({r.error})"
        lines.append(line)
    
    # Verdict
    lines.append("")
    lines.append("-" * 72)
    if report.ready_to_deploy:
        if report.has_warnings:
            lines.append("⚠️  READY TO DEPLOY — with warnings")
            if report.missing_optional:
                lines.append(f"   {len(report.missing_optional)} optional param(s) missing — agent runs in degraded mode")
            if report.placeholder_required:
                lines.append(f"   {len(report.placeholder_required)} required param(s) have placeholder values — replace before production")
        else:
            lines.append("✅ READY TO DEPLOY")
            r_present = sum(1 for r in report.required_results if r.is_present)
            r_total = len(report.required_results)
            lines.append(f"   {r_present}/{r_total} required params present")
            if report.optional_results:
                o_present = sum(1 for r in report.optional_results if r.is_present)
                o_total = len(report.optional_results)
                lines.append(f"   {o_present}/{o_total} optional params present")
    else:
        lines.append("❌ NOT READY TO DEPLOY")
        lines.append(f"   {len(report.missing_required)} required param(s) MISSING:")
        for name in report.missing_required:
            lines.append(f"     - {name}")
        lines.append("")
        lines.append("   Add these to SSM before deploying:")
        for name in report.missing_required:
            lines.append(f"     aws ssm put-parameter --name {name} --value <VALUE> \\")
            lines.append(f"       --type SecureString --region {report.region}")
    
    lines.append("=" * 72)
    return "\n".join(lines)


# ============================================================
# CLI
# ============================================================

def main():
    """
    CLI: python ssm_validator.py <spec_file> [region]
    
    Exit codes:
        0  ready to deploy (clean or with warnings)
        1  NOT ready to deploy (required params missing)
        2  spec parse error or AWS auth error
    """
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python ssm_validator.py <spec_file> [region]", file=sys.stderr)
        print("  region defaults to us-east-1", file=sys.stderr)
        sys.exit(2)
    
    spec_path = sys.argv[1]
    region = sys.argv[2] if len(sys.argv) >= 3 else "us-east-1"
    
    report, errors = validate_ssm_for_spec_file(spec_path, region=region)
    
    if errors:
        print(f"❌ SPEC PARSE ERRORS in {spec_path}:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        sys.exit(2)
    
    print(format_report(report))
    
    if report.auth_error:
        sys.exit(2)
    if not report.ready_to_deploy:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
