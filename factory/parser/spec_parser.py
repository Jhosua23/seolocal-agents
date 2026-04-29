"""
Agent Factory — Component 2: Spec Parser
==========================================

Loads JSON specs, validates against schema, runs cross-field checks,
returns either a normalized SpecDict or a list of validation errors.

This is the gate between human-authored specs and the code generator.
A spec that passes here is guaranteed safe to feed to the generator.

Usage:
    from factory.parser import parse_spec

    spec, errors = parse_spec("specs/agent-01-free-website-audit.spec.json")
    if errors:
        for e in errors:
            print(f"  ❌ {e}")
        sys.exit(1)
    
    # spec is now a normalized dict ready for the generator
    generator.emit(spec)


Hard rules:
    1. spec_version MUST be "1.0"
    2. agent_id MUST match agent-{kebab-case}
    3. agent_name MUST match camelCase pattern
    4. Every chain_invokes target MUST reference a valid agent_id pattern
    5. Every chain_invokes ssm_arn_key MUST appear in ssm_params (required or optional)
    6. Every memory.namespace_key MUST reference an input field name in at least one path
    7. Every llm_tasks.complexity MUST be COMPLEX | STANDARD | SIMPLE
    8. SSM param names MUST be UPPER_SNAKE_CASE (Critical Rule #5)
    9. agent_name MUST be camelCase no spaces no hyphens
   10. Every path MUST start with /

Author: Jhosua, April 29, 2026
"""

from __future__ import annotations
import json
import re
import os
from pathlib import Path
from typing import Any


# ============================================================
# CONSTANTS
# ============================================================

SUPPORTED_SPEC_VERSION = "1.0"

VALID_TRIGGER_TYPES = {"http_webhook", "cron", "event_bus", "manual"}

VALID_COMPLEXITY_TIERS = {"COMPLEX", "STANDARD", "SIMPLE"}

VALID_PHASES = {"phase1", "phase2", "meta-phase1", "meta-phase2"}

VALID_APIS = {
    "ghl", "dataforseo", "anthropic", "google_places", "pagespeed",
    "local_falcon", "recurly", "heygen", "slack", "sendgrid",
    "s3", "rds", "dynamodb", "ses",
}

VALID_MEMORY_STRATEGIES = {"episodic", "semantic", "both"}

VALID_INPUT_TYPES = {"string", "integer", "float", "boolean", "list", "dict"}

# Regex patterns from schema
PATTERN_AGENT_ID = re.compile(r"^agent-[a-z0-9-]+$")
PATTERN_AGENT_NAME = re.compile(r"^[a-z][a-zA-Z0-9]*$")
PATTERN_TENANT_ID = re.compile(r"^tenant-[0-9]{3}$")
PATTERN_PATH = re.compile(r"^/[a-z0-9/-]+$")
PATTERN_HANDLER = re.compile(r"^handle_[a-z_]+$")
PATTERN_INPUT_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
PATTERN_SSM_NAME = re.compile(r"^[A-Z][A-Z0-9_]*$")
PATTERN_TASK_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
PATTERN_NAMESPACE_KEY = re.compile(r"^[a-z][a-z0-9_]*$")


# ============================================================
# EXCEPTIONS
# ============================================================

class SpecParseError(Exception):
    """Raised when a spec file can't be loaded or parsed as JSON."""
    pass


# ============================================================
# CORE PARSING
# ============================================================

def load_spec(path: str | Path) -> dict[str, Any]:
    """
    Load JSON spec file from disk.
    
    Raises:
        SpecParseError: if file doesn't exist or isn't valid JSON
    """
    path = Path(path)
    if not path.exists():
        raise SpecParseError(f"Spec file not found: {path}")
    if not path.is_file():
        raise SpecParseError(f"Path is not a file: {path}")
    
    try:
        with open(path, encoding="utf-8") as f:
            spec = json.load(f)
    except json.JSONDecodeError as e:
        raise SpecParseError(
            f"Invalid JSON in {path}: line {e.lineno}, column {e.colno}: {e.msg}"
        )
    
    if not isinstance(spec, dict):
        raise SpecParseError(f"Spec must be a JSON object, got {type(spec).__name__}")
    
    return spec


def validate_spec(spec: dict[str, Any]) -> list[str]:
    """
    Run all validation checks on a spec dict.
    
    Returns:
        Empty list if spec is valid.
        List of error strings if spec has problems.
    """
    errors: list[str] = []
    
    # Layer 1 — required top-level fields
    errors.extend(_check_required_fields(spec))
    if errors:
        # If basic structure is broken, don't bother with deeper checks
        return errors
    
    # Layer 2 — field-level format validation
    errors.extend(_check_spec_version(spec))
    errors.extend(_check_agent_id(spec))
    errors.extend(_check_agent_name(spec))
    errors.extend(_check_tenant_id(spec))
    errors.extend(_check_phase(spec))
    errors.extend(_check_description(spec))
    errors.extend(_check_trigger(spec))
    errors.extend(_check_paths(spec))
    errors.extend(_check_ssm_params(spec))
    errors.extend(_check_apis(spec))
    errors.extend(_check_adapters(spec))
    errors.extend(_check_memory(spec))
    errors.extend(_check_chain_invokes(spec))
    errors.extend(_check_lambda_config(spec))
    errors.extend(_check_llm_tasks(spec))
    errors.extend(_check_metadata(spec))
    
    # Layer 3 — cross-field consistency checks
    errors.extend(_check_chain_invoke_ssm_consistency(spec))
    errors.extend(_check_memory_namespace_consistency(spec))
    errors.extend(_check_no_unknown_top_level_fields(spec))
    
    return errors


def parse_spec(path: str | Path) -> tuple[dict[str, Any] | None, list[str]]:
    """
    Load and validate a spec file. The main entrypoint.
    
    Returns:
        (spec_dict, []) on success
        (None, [errors...]) on failure
    """
    try:
        spec = load_spec(path)
    except SpecParseError as e:
        return None, [str(e)]
    
    errors = validate_spec(spec)
    if errors:
        return None, errors
    
    return spec, []


# ============================================================
# LAYER 1 — REQUIRED FIELDS
# ============================================================

REQUIRED_TOP_LEVEL = [
    "spec_version", "agent_id", "agent_name", "tenant_id",
    "phase", "description", "trigger", "paths", "ssm_params",
]


def _check_required_fields(spec: dict[str, Any]) -> list[str]:
    errors = []
    for field in REQUIRED_TOP_LEVEL:
        if field not in spec:
            errors.append(f"Missing required top-level field: '{field}'")
    return errors


# ============================================================
# LAYER 2 — FIELD-LEVEL CHECKS
# ============================================================

def _check_spec_version(spec: dict[str, Any]) -> list[str]:
    v = spec.get("spec_version")
    if v != SUPPORTED_SPEC_VERSION:
        return [f"spec_version must be '{SUPPORTED_SPEC_VERSION}', got '{v}'"]
    return []


def _check_agent_id(spec: dict[str, Any]) -> list[str]:
    agent_id = spec.get("agent_id", "")
    if not isinstance(agent_id, str):
        return [f"agent_id must be a string, got {type(agent_id).__name__}"]
    if not PATTERN_AGENT_ID.match(agent_id):
        return [f"agent_id '{agent_id}' must match pattern 'agent-<kebab-case>' (e.g., agent-01-free-website-audit)"]
    return []


def _check_agent_name(spec: dict[str, Any]) -> list[str]:
    name = spec.get("agent_name", "")
    if not isinstance(name, str):
        return [f"agent_name must be a string, got {type(name).__name__}"]
    if not (3 <= len(name) <= 50):
        return [f"agent_name must be 3-50 chars, got {len(name)} chars: '{name}'"]
    if not PATTERN_AGENT_NAME.match(name):
        return [
            f"agent_name '{name}' must be camelCase, start with lowercase letter, "
            f"no spaces, no hyphens (e.g., freeWebsiteAudit, pipelineMonitor)"
        ]
    return []


def _check_tenant_id(spec: dict[str, Any]) -> list[str]:
    tid = spec.get("tenant_id", "")
    if not PATTERN_TENANT_ID.match(tid):
        return [f"tenant_id '{tid}' must match pattern 'tenant-NNN' (e.g., tenant-001)"]
    return []


def _check_phase(spec: dict[str, Any]) -> list[str]:
    phase = spec.get("phase", "")
    if phase not in VALID_PHASES:
        return [f"phase '{phase}' must be one of {sorted(VALID_PHASES)}"]
    return []


def _check_description(spec: dict[str, Any]) -> list[str]:
    desc = spec.get("description", "")
    if not isinstance(desc, str):
        return [f"description must be a string"]
    if not (30 <= len(desc) <= 500):
        return [f"description must be 30-500 chars, got {len(desc)} chars"]
    return []


def _check_trigger(spec: dict[str, Any]) -> list[str]:
    errors = []
    trigger = spec.get("trigger", {})
    if not isinstance(trigger, dict):
        return [f"trigger must be an object"]
    
    t_type = trigger.get("type")
    if not t_type:
        errors.append("trigger.type is required")
    elif t_type not in VALID_TRIGGER_TYPES:
        errors.append(f"trigger.type '{t_type}' must be one of {sorted(VALID_TRIGGER_TYPES)}")
    
    # Conditional requirements
    if t_type == "cron" and not trigger.get("schedule"):
        errors.append("trigger.schedule is required when trigger.type is 'cron'")
    if t_type == "event_bus" and not trigger.get("event_pattern"):
        errors.append("trigger.event_pattern is required when trigger.type is 'event_bus'")
    
    return errors


def _check_paths(spec: dict[str, Any]) -> list[str]:
    errors = []
    paths = spec.get("paths", [])
    if not isinstance(paths, list):
        return [f"paths must be an array"]
    if len(paths) < 1:
        return [f"paths must have at least 1 entry"]
    
    seen_paths = set()
    seen_handlers = set()
    
    for i, p in enumerate(paths):
        prefix = f"paths[{i}]"
        if not isinstance(p, dict):
            errors.append(f"{prefix} must be an object")
            continue
        
        # Required: path, handler, description
        path = p.get("path", "")
        handler = p.get("handler", "")
        description = p.get("description", "")
        
        if not path:
            errors.append(f"{prefix}.path is required")
        elif not PATTERN_PATH.match(path):
            errors.append(f"{prefix}.path '{path}' must start with / and contain only [a-z0-9/-]")
        elif path in seen_paths:
            errors.append(f"{prefix}.path '{path}' is duplicated")
        else:
            seen_paths.add(path)
        
        if not handler:
            errors.append(f"{prefix}.handler is required")
        elif not PATTERN_HANDLER.match(handler):
            errors.append(f"{prefix}.handler '{handler}' must match 'handle_<lowercase_name>'")
        elif handler in seen_handlers:
            errors.append(f"{prefix}.handler '{handler}' is duplicated")
        else:
            seen_handlers.add(handler)
        
        if not description:
            errors.append(f"{prefix}.description is required")
        
        # Validate inputs
        for j, inp in enumerate(p.get("inputs", []) or []):
            iprefix = f"{prefix}.inputs[{j}]"
            if not isinstance(inp, dict):
                errors.append(f"{iprefix} must be an object")
                continue
            if not inp.get("name"):
                errors.append(f"{iprefix}.name is required")
            elif not PATTERN_INPUT_NAME.match(inp["name"]):
                errors.append(
                    f"{iprefix}.name '{inp['name']}' must be lowercase_snake_case"
                )
            if not inp.get("type"):
                errors.append(f"{iprefix}.type is required")
            elif inp["type"] not in VALID_INPUT_TYPES:
                errors.append(
                    f"{iprefix}.type '{inp['type']}' must be one of {sorted(VALID_INPUT_TYPES)}"
                )
            if "required" not in inp:
                errors.append(f"{iprefix}.required is required (boolean)")
        
        # Validate outputs (less strict — mostly informational)
        for j, out in enumerate(p.get("outputs", []) or []):
            oprefix = f"{prefix}.outputs[{j}]"
            if not isinstance(out, dict):
                errors.append(f"{oprefix} must be an object")
                continue
            if not out.get("name"):
                errors.append(f"{oprefix}.name is required")
            if not out.get("type"):
                errors.append(f"{oprefix}.type is required")
    
    return errors


def _check_ssm_params(spec: dict[str, Any]) -> list[str]:
    errors = []
    ssm = spec.get("ssm_params", {})
    if not isinstance(ssm, dict):
        return [f"ssm_params must be an object"]
    
    if "required" not in ssm:
        errors.append("ssm_params.required is required (can be empty array)")
        return errors
    
    for kind in ("required", "optional"):
        params = ssm.get(kind, [])
        if not isinstance(params, list):
            errors.append(f"ssm_params.{kind} must be an array")
            continue
        for i, p in enumerate(params):
            if not isinstance(p, str):
                errors.append(f"ssm_params.{kind}[{i}] must be a string")
                continue
            # NOTE: Path-style names like /seolocal/agent13/CLAUDE_MODEL_ID
            # are also valid in production. Schema enforces flat for new agents.
            if not (PATTERN_SSM_NAME.match(p) or p.startswith("/")):
                errors.append(
                    f"ssm_params.{kind}[{i}] '{p}' must be UPPER_SNAKE_CASE "
                    f"or a path starting with /"
                )
    
    return errors


def _check_apis(spec: dict[str, Any]) -> list[str]:
    errors = []
    apis = spec.get("apis", [])
    if not isinstance(apis, list):
        return [f"apis must be an array"]
    for i, a in enumerate(apis):
        if a not in VALID_APIS:
            errors.append(f"apis[{i}] '{a}' must be one of {sorted(VALID_APIS)}")
    return errors


def _check_adapters(spec: dict[str, Any]) -> list[str]:
    errors = []
    adapters = spec.get("adapters", {})
    if not isinstance(adapters, dict):
        return [f"adapters must be an object"]
    if "crm" in adapters and not isinstance(adapters["crm"], bool):
        errors.append("adapters.crm must be boolean")
    if "billing" in adapters and not isinstance(adapters["billing"], bool):
        errors.append("adapters.billing must be boolean")
    return errors


def _check_memory(spec: dict[str, Any]) -> list[str]:
    errors = []
    memory = spec.get("memory", {})
    if not isinstance(memory, dict):
        return [f"memory must be an object"]
    
    enabled = memory.get("enabled")
    if "enabled" in memory and not isinstance(enabled, bool):
        errors.append("memory.enabled must be boolean")
    
    if enabled is True:
        strategy = memory.get("strategy")
        if not strategy:
            errors.append("memory.strategy is required when memory.enabled=true")
        elif strategy not in VALID_MEMORY_STRATEGIES:
            errors.append(f"memory.strategy '{strategy}' must be one of {sorted(VALID_MEMORY_STRATEGIES)}")
        
        ns_key = memory.get("namespace_key")
        if not ns_key:
            errors.append("memory.namespace_key is required when memory.enabled=true")
        elif not PATTERN_NAMESPACE_KEY.match(ns_key):
            errors.append(f"memory.namespace_key '{ns_key}' must be lowercase_snake_case")
    
    return errors


def _check_chain_invokes(spec: dict[str, Any]) -> list[str]:
    errors = []
    chains = spec.get("chain_invokes", [])
    if not isinstance(chains, list):
        return [f"chain_invokes must be an array"]
    
    for i, c in enumerate(chains):
        prefix = f"chain_invokes[{i}]"
        if not isinstance(c, dict):
            errors.append(f"{prefix} must be an object")
            continue
        
        target = c.get("target_agent_id")
        if not target:
            errors.append(f"{prefix}.target_agent_id is required")
        elif not PATTERN_AGENT_ID.match(target):
            errors.append(f"{prefix}.target_agent_id '{target}' must match pattern 'agent-<kebab-case>'")
        
        if not c.get("trigger_condition"):
            errors.append(f"{prefix}.trigger_condition is required")
        
        ssm_arn = c.get("ssm_arn_key")
        if ssm_arn and not PATTERN_SSM_NAME.match(ssm_arn):
            errors.append(f"{prefix}.ssm_arn_key '{ssm_arn}' must be UPPER_SNAKE_CASE")
    
    return errors


def _check_lambda_config(spec: dict[str, Any]) -> list[str]:
    errors = []
    lc = spec.get("lambda_config", {})
    if not isinstance(lc, dict):
        return [f"lambda_config must be an object"]
    
    mem = lc.get("memory_mb")
    if mem is not None:
        if not isinstance(mem, int) or not (128 <= mem <= 10240):
            errors.append(f"lambda_config.memory_mb must be int 128-10240, got {mem}")
    
    timeout = lc.get("timeout_seconds")
    if timeout is not None:
        if not isinstance(timeout, int) or not (10 <= timeout <= 900):
            errors.append(f"lambda_config.timeout_seconds must be int 10-900, got {timeout}")
    
    pkgs = lc.get("extra_packages", [])
    if not isinstance(pkgs, list):
        errors.append(f"lambda_config.extra_packages must be an array")
    
    return errors


def _check_llm_tasks(spec: dict[str, Any]) -> list[str]:
    errors = []
    tasks = spec.get("llm_tasks", [])
    if not isinstance(tasks, list):
        return [f"llm_tasks must be an array"]
    
    seen_names = set()
    for i, t in enumerate(tasks):
        prefix = f"llm_tasks[{i}]"
        if not isinstance(t, dict):
            errors.append(f"{prefix} must be an object")
            continue
        
        name = t.get("task_name")
        if not name:
            errors.append(f"{prefix}.task_name is required")
        elif not PATTERN_TASK_NAME.match(name):
            errors.append(f"{prefix}.task_name '{name}' must be lowercase_snake_case")
        elif name in seen_names:
            errors.append(f"{prefix}.task_name '{name}' is duplicated")
        else:
            seen_names.add(name)
        
        complexity = t.get("complexity")
        if not complexity:
            errors.append(f"{prefix}.complexity is required")
        elif complexity not in VALID_COMPLEXITY_TIERS:
            errors.append(
                f"{prefix}.complexity '{complexity}' must be one of {sorted(VALID_COMPLEXITY_TIERS)}"
            )
    
    return errors


def _check_metadata(spec: dict[str, Any]) -> list[str]:
    errors = []
    md = spec.get("metadata", {})
    if not isinstance(md, dict):
        return [f"metadata must be an object"]
    # Metadata is informational — no strict validation
    return errors


# ============================================================
# LAYER 3 — CROSS-FIELD CONSISTENCY
# ============================================================

ALLOWED_TOP_LEVEL = {
    "spec_version", "agent_id", "agent_name", "tenant_id", "phase",
    "description", "trigger", "paths", "ssm_params", "apis", "adapters",
    "memory", "chain_invokes", "lambda_config", "llm_tasks", "metadata",
}


def _check_no_unknown_top_level_fields(spec: dict[str, Any]) -> list[str]:
    errors = []
    extra = set(spec.keys()) - ALLOWED_TOP_LEVEL
    if extra:
        errors.append(
            f"Unknown top-level fields not allowed: {sorted(extra)}. "
            f"Allowed: {sorted(ALLOWED_TOP_LEVEL)}"
        )
    return errors


def _check_chain_invoke_ssm_consistency(spec: dict[str, Any]) -> list[str]:
    """
    For every chain_invoke that declares an ssm_arn_key,
    that key MUST also appear in ssm_params (required or optional).
    Otherwise the generated agent will fail to read the target ARN.
    """
    errors = []
    chains = spec.get("chain_invokes", [])
    ssm = spec.get("ssm_params", {})
    
    declared = set()
    declared.update(ssm.get("required", []) or [])
    declared.update(ssm.get("optional", []) or [])
    
    for i, c in enumerate(chains):
        if not isinstance(c, dict):
            continue
        ssm_arn = c.get("ssm_arn_key")
        if ssm_arn and ssm_arn not in declared:
            errors.append(
                f"chain_invokes[{i}].ssm_arn_key '{ssm_arn}' is not declared in "
                f"ssm_params.required or ssm_params.optional. "
                f"The generator can't emit a valid ARN read without this."
            )
    return errors


def _check_memory_namespace_consistency(spec: dict[str, Any]) -> list[str]:
    """
    If memory is enabled and namespace_key references an input field,
    that input field MUST exist in at least one path's inputs.
    Otherwise the generator can't resolve the namespace at runtime.
    """
    errors = []
    memory = spec.get("memory", {})
    if not memory.get("enabled"):
        return errors
    
    ns_key = memory.get("namespace_key")
    if not ns_key:
        return errors
    
    # Find this field in any path's inputs
    found = False
    for p in spec.get("paths", []):
        if not isinstance(p, dict):
            continue
        for inp in p.get("inputs", []) or []:
            if isinstance(inp, dict) and inp.get("name") == ns_key:
                found = True
                break
        if found:
            break
    
    if not found:
        errors.append(
            f"memory.namespace_key '{ns_key}' is not declared as an input "
            f"in any path. The generator needs this field at runtime to "
            f"build the memory namespace."
        )
    return errors


# ============================================================
# CLI ENTRYPOINT
# ============================================================

def main():
    """
    CLI: python spec_parser.py <spec_file>
    
    Returns:
        Exit 0 if spec is valid
        Exit 1 if spec has errors (errors printed to stderr)
        Exit 2 if spec file can't be loaded
    """
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python spec_parser.py <spec_file>", file=sys.stderr)
        sys.exit(1)
    
    spec_path = sys.argv[1]
    spec, errors = parse_spec(spec_path)
    
    if errors:
        print(f"❌ INVALID SPEC: {spec_path}", file=sys.stderr)
        print(f"Found {len(errors)} error(s):", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        sys.exit(1)
    
    print(f"✅ VALID SPEC: {spec_path}")
    print(f"   agent_id    : {spec['agent_id']}")
    print(f"   agent_name  : {spec['agent_name']}")
    print(f"   tenant_id   : {spec['tenant_id']}")
    print(f"   phase       : {spec['phase']}")
    print(f"   trigger     : {spec['trigger']['type']}")
    print(f"   paths       : {len(spec.get('paths', []))}")
    print(f"   ssm req     : {len(spec.get('ssm_params', {}).get('required', []))}")
    print(f"   ssm opt     : {len(spec.get('ssm_params', {}).get('optional', []))}")
    print(f"   apis        : {len(spec.get('apis', []))}")
    print(f"   memory      : {spec.get('memory', {}).get('enabled', False)}")
    print(f"   chain calls : {len(spec.get('chain_invokes', []))}")
    print(f"   llm tasks   : {len(spec.get('llm_tasks', []))}")
    sys.exit(0)


if __name__ == "__main__":
    main()
