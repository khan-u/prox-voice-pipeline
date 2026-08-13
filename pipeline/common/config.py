"""Pipeline configuration loader.

Reads config/pipeline.yml, selects the active mode section (local or azure),
and resolves ${VAR} placeholders against the process environment. The mode is
taken from PIPELINE_MODE if set, otherwise from the file's top-level `mode` key.
"""
import os
import re
from pathlib import Path

import yaml

_PLACEHOLDER = re.compile(r"\$\{([A-Z0-9_]+)\}")

# Repo root is three levels up from this file: pipeline/common/config.py -> repo.
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = REPO_ROOT / "pipeline" / "config" / "pipeline.yml"


def resolve_placeholders(value, env):
    """Replace every ${VAR} in a string with env[VAR].

    Pure: takes an explicit env mapping and returns a new value. Non-string
    values pass through unchanged. A missing variable raises KeyError so a
    misconfigured deployment fails loudly rather than reading an empty root.
    """
    if not isinstance(value, str):
        return value

    def sub(match):
        name = match.group(1)
        if name not in env:
            raise KeyError(name)
        return env[name]

    return _PLACEHOLDER.sub(sub, value)


def select_section(doc, mode, env):
    """Return the resolved config section for `mode` from a parsed yml doc.

    Pure: all inputs explicit. Resolves ${VAR} placeholders in every string
    leaf of the selected section against `env`.
    """
    if mode not in doc:
        raise KeyError(f"no config section for mode {mode!r}")
    section = doc[mode]
    return {k: resolve_placeholders(v, env) for k, v in section.items()}


def load_config(path=DEFAULT_CONFIG, env=None):
    """Load and resolve the active-mode config from a pipeline.yml path."""
    env = os.environ if env is None else env
    doc = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    mode = env.get("PIPELINE_MODE", doc.get("mode", "local"))
    resolved = select_section(doc, mode, env)
    resolved["mode"] = mode
    return resolved
