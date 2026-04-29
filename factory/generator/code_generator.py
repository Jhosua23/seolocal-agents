"""
Agent Factory — Component 3: Code Generator
=============================================

Takes a parsed spec (already validated by spec_parser.py) and emits a
complete, deployable main.py file using Jinja2 templates.

The generator enforces all 13 Critical Rules by template construction:
  - app.run() at module level (no __main__ guard) — Eduard's bug fixed
  - SSM names always UPPER_SNAKE_CASE — schema rejects bad names
  - Handler functions named handle_<name> — schema enforced
  - Path injection via payload["_path"] — bridge pattern baked in
  - LLM Selector with COMPLEX/STANDARD/SIMPLE — Chuck's spec baked in
  - CRM/billing adapters injected when adapters.crm/billing=true
  - Memory wiring stubbed pending Jomar's interface

Usage:
    # As library
    from code_generator import generate_main_py
    
    spec, errors = parse_spec(path)
    if errors: ...
    
    main_py = generate_main_py(spec)
    with open("main.py", "w") as f:
        f.write(main_py)
    
    # As CLI
    python code_generator.py specs/agent-01.spec.json [output.py]

Author: Jhosua, April 29, 2026
"""

from __future__ import annotations
import json
import sys
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from jinja2 import Environment, FileSystemLoader, StrictUndefined, TemplateNotFound
    JINJA2_AVAILABLE = True
except ImportError:
    JINJA2_AVAILABLE = False


# ============================================================
# CONSTANTS
# ============================================================

DEFAULT_AWS_REGION = "us-east-1"
GENERATOR_VERSION = "1.0"
TEMPLATE_NAME = "main.py.j2"

# Map API enum -> what python packages they need
API_TO_PACKAGE = {
    "ghl": "requests",
    "dataforseo": "requests",
    "anthropic": None,  # imported separately via uses_anthropic flag
    "google_places": "requests",
    "pagespeed": "requests",
    "local_falcon": "requests",
    "recurly": None,  # via billing adapter
    "heygen": "requests",
    "slack": "requests",
    "sendgrid": "requests",
    "s3": None,  # boto3 (always imported)
    "rds": None,  # via psycopg2 in extra_packages
    "dynamodb": None,  # boto3
    "ses": None,  # boto3
}


# ============================================================
# TEMPLATE CONTEXT BUILDER
# ============================================================

def build_template_context(
    spec: dict[str, Any],
    spec_path: str | Path = "<unknown>",
    aws_region: str = DEFAULT_AWS_REGION,
) -> dict[str, Any]:
    """
    Convert a spec dict into the context Jinja2 expects.
    
    Why a builder?
        - The template uses simpler shapes than the spec (e.g. uses_anthropic
          boolean instead of digging into apis array)
        - Centralizes any data transformation logic
    """
    apis = spec.get("apis", []) or []
    adapters = spec.get("adapters", {}) or {}
    memory = spec.get("memory", {}) or {}
    llm_tasks = spec.get("llm_tasks", []) or []
    
    # Detect what packages we need to import
    std_packages = set()
    for api in apis:
        pkg = API_TO_PACKAGE.get(api)
        if pkg:
            std_packages.add(pkg)
    
    # If any LLM tasks declared, we need anthropic
    uses_anthropic = bool(llm_tasks) or "anthropic" in apis
    
    return {
        # Identity
        "agent_id": spec["agent_id"],
        "agent_name": spec["agent_name"],
        "tenant_id": spec["tenant_id"],
        "phase": spec["phase"],
        "description": spec["description"],
        
        # Generator metadata
        "spec_path": str(spec_path),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generator_version": GENERATOR_VERSION,
        "aws_region": aws_region,
        
        # Imports
        "std_packages": list(std_packages),
        "uses_anthropic": uses_anthropic,
        "uses_crm_adapter": bool(adapters.get("crm")),
        "uses_billing_adapter": bool(adapters.get("billing")),
        
        # SSM
        "ssm_required": spec.get("ssm_params", {}).get("required", []) or [],
        "ssm_optional": spec.get("ssm_params", {}).get("optional", []) or [],
        
        # Paths
        "paths": spec.get("paths", []),
        
        # LLM
        "llm_tasks": llm_tasks,
        
        # Memory
        "memory_enabled": bool(memory.get("enabled")),
        "memory_strategy": memory.get("strategy", "semantic"),
        "memory_namespace_key": memory.get("namespace_key"),
        
        # Chain-invokes
        "chain_invokes": spec.get("chain_invokes", []) or [],
    }


# ============================================================
# CORE GENERATION
# ============================================================

def _get_jinja_env(template_dir: Path | None = None) -> "Environment":
    """Build Jinja2 environment pointing at templates/ folder."""
    if not JINJA2_AVAILABLE:
        raise RuntimeError(
            "Jinja2 not installed. Install with: pip install jinja2"
        )
    
    if template_dir is None:
        template_dir = Path(__file__).parent / "templates"
    
    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        # Don't trim newlines aggressively — keep readable output
        trim_blocks=False,
        lstrip_blocks=False,
        # Leave undefined vars in templates as errors (catch typos)
        undefined=StrictUndefined,
        # Don't autoescape — we're emitting Python, not HTML
        autoescape=False,
    )
    return env


def generate_main_py(
    spec: dict[str, Any],
    spec_path: str | Path = "<unknown>",
    aws_region: str = DEFAULT_AWS_REGION,
    template_dir: Path | None = None,
) -> str:
    """
    Generate a main.py file from a spec dict.
    
    Args:
        spec: Already-parsed spec (from spec_parser.parse_spec)
        spec_path: Path to original spec file (used in generated header)
        aws_region: AWS region for SSM client
        template_dir: Optional custom template directory
    
    Returns:
        Complete main.py source code as a string.
    """
    env = _get_jinja_env(template_dir)
    
    try:
        template = env.get_template(TEMPLATE_NAME)
    except TemplateNotFound:
        raise RuntimeError(
            f"Template '{TEMPLATE_NAME}' not found in {env.loader.searchpath}"
        )
    
    context = build_template_context(spec, spec_path=spec_path, aws_region=aws_region)
    
    rendered = template.render(**context)
    return rendered


def generate_from_spec_file(
    spec_path: str | Path,
    output_path: str | Path | None = None,
    aws_region: str = DEFAULT_AWS_REGION,
) -> tuple[str | None, list[str]]:
    """
    Top-level: parse spec file, validate, generate main.py.
    
    Returns:
        (generated_code, []) on success
        (None, [errors...]) if spec fails to parse
    """
    # Import parser at call time to avoid hard dep
    parser_path = Path(__file__).parent.parent / "parser"
    if str(parser_path) not in sys.path:
        sys.path.insert(0, str(parser_path))
    
    try:
        from spec_parser import parse_spec
    except ImportError as e:
        return None, [f"Could not import spec_parser: {e}"]
    
    spec, errors = parse_spec(spec_path)
    if errors:
        return None, errors
    
    try:
        code = generate_main_py(spec, spec_path=spec_path, aws_region=aws_region)
    except Exception as e:
        return None, [f"Generator error: {type(e).__name__}: {e}"]
    
    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(code)
    
    return code, []


# ============================================================
# CLI
# ============================================================

def main():
    """
    CLI: python code_generator.py <spec_file> [output_file] [region]
    
    If output_file is omitted, prints to stdout.
    """
    if len(sys.argv) < 2:
        print("Usage: python code_generator.py <spec_file> [output_file] [region]", file=sys.stderr)
        print("  region defaults to us-east-1", file=sys.stderr)
        print("  if output_file omitted, prints to stdout", file=sys.stderr)
        sys.exit(2)
    
    spec_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) >= 3 else None
    region = sys.argv[3] if len(sys.argv) >= 4 else DEFAULT_AWS_REGION
    
    code, errors = generate_from_spec_file(spec_path, output_path=output_path, aws_region=region)
    
    if errors:
        print(f"❌ FAILED to generate from {spec_path}:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        sys.exit(1)
    
    if output_path:
        line_count = code.count("\n")
        char_count = len(code)
        print(f"✅ Generated {output_path}")
        print(f"   Lines : {line_count}")
        print(f"   Chars : {char_count}")
    else:
        # Print to stdout
        print(code)
    
    sys.exit(0)


if __name__ == "__main__":
    main()
