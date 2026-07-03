"""Plugin security: allowlisted discovery, import restrictions, signature hooks."""

from __future__ import annotations

import ast
import hashlib
import importlib
import importlib.util
import pkgutil
from pathlib import Path
from typing import Any

# Modules plugins may import at runtime (enforced at load time via AST scan).
ALLOWED_IMPORT_ROOTS: frozenset[str] = frozenset(
    {
        "__future__",
        "numpy",
        "np",
        "json",
        "math",
        "typing",
        "dataclasses",
        "abc",
        "collections",
        "datetime",
        "mantle_analytics",
        # Optional — allowed when installed; plugins should guard usage.
        "xarray",
        "rasterio",
        "pyarrow",
    }
)

_PACKAGE_ROOT = Path(__file__).resolve().parent
PLUGINS_ROOT = _PACKAGE_ROOT / "plugins"
CUSTOM_PLUGIN_ROOT = PLUGINS_ROOT / "custom"

# Built-in plugins ship under these package paths only (read-only).
BUILTIN_VRPM_PACKAGE = "mantle_analytics.plugins.builtin.vrpm"
BUILTIN_PRPM_PACKAGE = "mantle_analytics.plugins.builtin.prpm"
BUILTIN_PACKAGES = (BUILTIN_VRPM_PACKAGE, BUILTIN_PRPM_PACKAGE)

# Additional extension directories (absolute paths) from config allowlist.
_registered_extension_dirs: list[Path] = []


class PluginSecurityError(Exception):
    """Raised when a plugin fails security validation."""


def plugins_root() -> Path:
    """Return the Mantle analytics ``plugins/`` directory."""
    return PLUGINS_ROOT


def register_extension_dir(path: str | Path) -> None:
    """Register an admin-approved extension directory (must exist on disk)."""
    resolved = Path(path).resolve()
    if not resolved.is_dir():
        raise PluginSecurityError(f"extension directory does not exist: {resolved}")
    if resolved not in _registered_extension_dirs:
        _registered_extension_dirs.append(resolved)


def clear_extension_dirs() -> None:
    """Reset extension dirs (for tests)."""
    _registered_extension_dirs.clear()


def is_allowed_custom_dir(path: Path) -> bool:
    """Custom plugins load only from ``plugins/custom/`` or registered extension dirs."""
    resolved = path.resolve()
    try:
        resolved.relative_to(CUSTOM_PLUGIN_ROOT.resolve())
        return True
    except ValueError:
        pass
    return resolved in _registered_extension_dirs


def _is_allowed_import(name: str) -> bool:
    root = name.split(".", 1)[0]
    return root in ALLOWED_IMPORT_ROOTS


def validate_source_ast(source: str, *, path: str = "<plugin>") -> None:
    """Static analysis: reject dangerous constructs and disallowed imports."""
    try:
        tree = ast.parse(source, filename=path)
    except SyntaxError as exc:
        raise PluginSecurityError(f"invalid Python syntax in {path}: {exc}") from exc

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if not _is_allowed_import(alias.name):
                    raise PluginSecurityError(
                        f"disallowed import '{alias.name}' in {path}"
                    )
        elif isinstance(node, ast.ImportFrom):
            if node.module and not _is_allowed_import(node.module):
                raise PluginSecurityError(
                    f"disallowed import from '{node.module}' in {path}"
                )
        elif isinstance(node, ast.Global):
            raise PluginSecurityError(f"disallowed statement in {path}")
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id in ("exec", "eval", "compile", "__import__"):
                raise PluginSecurityError(
                    f"disallowed call to {node.func.id}() in {path}"
                )


def file_sha256(path: Path) -> str:
    """Compute SHA-256 digest of a plugin file (for signature verification hooks)."""
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_plugin_signature(
    path: Path,
    expected_digest: str | None,
    *,
    require_signature: bool = False,
) -> str:
    """Verify plugin file digest. Returns the computed digest.

    When ``require_signature`` is True and no expected digest is supplied,
    raises — hook for future signed-plugin distribution.
    """
    digest = file_sha256(path)
    if require_signature and not expected_digest:
        raise PluginSecurityError(f"signed plugin required but no digest for {path}")
    if expected_digest and digest != expected_digest:
        raise PluginSecurityError(f"plugin digest mismatch for {path}")
    return digest


def _validate_module_file(path: Path) -> None:
    source = path.read_text(encoding="utf-8")
    validate_source_ast(source, path=str(path))


def _load_package_module(package: str, module_name: str, *, kind: str) -> Any:
    """Import and AST-validate a plugin module from a known package path."""
    full_name = f"{package}.{module_name}"
    spec = importlib.util.find_spec(full_name)
    if spec is None or spec.origin is None:
        raise PluginSecurityError(f"unknown {kind} plugin module: {module_name}")

    origin = Path(spec.origin)
    _validate_module_file(origin)
    return importlib.import_module(full_name)


def discover_builtin_modules() -> dict[str, Any]:
    """Load PLUGIN objects from ``builtin/vrpm`` and ``builtin/prpm`` packages."""
    discovered: dict[str, Any] = {}
    for package in BUILTIN_PACKAGES:
        pkg = importlib.import_module(package)
        if pkg.__path__ is None:
            continue
        for module_info in pkgutil.iter_modules(pkg.__path__):
            if module_info.name.startswith("_"):
                continue
            module = _load_package_module(package, module_info.name, kind="built-in")
            if hasattr(module, "PLUGIN"):
                plugin = module.PLUGIN
                plugin_id = getattr(plugin, "id", module_info.name)
                discovered[plugin_id] = plugin
    return discovered


def discover_custom_modules(directories: list[Path]) -> dict[str, Any]:
    """Load PLUGIN objects from allowlisted custom plugin directories."""
    discovered: dict[str, Any] = {}
    for directory in directories:
        resolved = directory.resolve()
        if not is_allowed_custom_dir(resolved):
            raise PluginSecurityError(
                f"custom plugin directory not allowlisted: {resolved}"
            )
        if not resolved.is_dir():
            raise PluginSecurityError(f"custom plugin directory does not exist: {resolved}")
        for path in sorted(resolved.glob("*.py")):
            if path.name.startswith("_"):
                continue
            _validate_module_file(path)
            module_name = f"mantle_ext_{path.stem}"
            spec = importlib.util.spec_from_file_location(module_name, path)
            if spec is None or spec.loader is None:
                continue
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            if hasattr(module, "PLUGIN"):
                plugin = module.PLUGIN
                plugin_id = getattr(plugin, "id", path.stem)
                discovered[plugin_id] = plugin
    return discovered
