"""Guards the packaging contract in pyproject.toml so it cannot drift from the code."""

import importlib
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
tomllib = pytest.importorskip("tomllib" if sys.version_info >= (3, 11) else "tomli")


@pytest.fixture(scope="module")
def pyproject():
    return tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))


def test_version_is_single_sourced(pyproject):
    import natquery

    assert (
        "version" in pyproject["project"]["dynamic"]
        and "version" not in pyproject["project"]
    )
    assert pyproject["tool"]["setuptools"]["dynamic"]["version"] == {
        "attr": "natquery.__version__"
    }
    assert re.fullmatch(r"\d+\.\d+\.\d+", natquery.__version__)


def test_console_script_points_at_a_real_callable(pyproject):
    target = pyproject["project"]["scripts"]["natquery"]
    mod, fn = target.split(":")
    assert callable(getattr(importlib.import_module(mod), fn))


def test_runtime_dependencies_cover_every_third_party_import(pyproject):
    declared = {
        re.split(r"[<>=!~ \[]", d)[0].lower().replace("-", "_")
        for d in pyproject["project"]["dependencies"]
    }
    mapping = {"psycopg2": "psycopg2_binary", "yaml": "pyyaml"}
    imported = set()
    for f in (ROOT / "src/natquery").rglob("*.py"):
        for m in re.finditer(
            r"^\s*(?:from|import)\s+([A-Za-z_][\w]*)",
            f.read_text(encoding="utf-8"),
            re.M,
        ):
            imported.add(m.group(1))
    stdlib = set(sys.stdlib_module_names) | {"natquery"}
    third_party = {mapping.get(m, m).lower() for m in imported - stdlib}
    assert third_party <= declared, f"undeclared dependencies: {third_party - declared}"


def test_installed_sqlglot_is_inside_the_declared_range(pyproject):
    from importlib.metadata import version

    packaging = pytest.importorskip("packaging.requirements")
    spec = next(
        packaging.Requirement(d)
        for d in pyproject["project"]["dependencies"]
        if d.startswith("sqlglot")
    )
    assert spec.specifier.contains(version("sqlglot"), prereleases=False), (
        f"sqlglot {version('sqlglot')} is outside the tested range {spec.specifier}; "
        "run the suite on it, then widen pyproject.toml"
    )


def test_example_files_referenced_by_cli_exist_and_are_packaged(pyproject):
    patterns = pyproject["tool"]["setuptools"]["package-data"]["natquery"]
    examples = ROOT / "src/natquery/examples"
    for needed in ("policy.example.yaml", "semantics.example.json"):
        assert (examples / needed).exists()
        assert any(Path(needed).match(Path(p).name) for p in patterns), needed


def test_python_floor_matches_classifiers(pyproject):
    assert pyproject["project"]["requires-python"] == ">=3.10"
    classifiers = "\n".join(pyproject["project"]["classifiers"])
    assert "Python :: 3.10" in classifiers


def test_every_declared_marker_is_registered_and_pytest_is_strict(pyproject):
    ini = pyproject["tool"]["pytest"]["ini_options"]
    assert "--strict-markers" in ini["addopts"] and any(
        m.startswith("integration") for m in ini["markers"]
    )


def test_coverage_gate_is_on_and_nothing_is_silently_omitted(pyproject):
    assert (
        "--cov-fail-under=90" in pyproject["tool"]["pytest"]["ini_options"]["addopts"]
    )
    assert "omit" not in pyproject["tool"]["coverage"]["run"]


def test_no_module_is_orphaned():
    """Every module under src/natquery is imported by something (or is an entry point)."""
    entry = {"natquery.cli.main", "natquery.__init__"}
    names = {
        ".".join(p.relative_to(ROOT / "src").with_suffix("").parts): p
        for p in (ROOT / "src/natquery").rglob("*.py")
        if p.name != "__init__.py"
    }
    text = "\n".join(
        p.read_text(encoding="utf-8") for p in (ROOT / "src/natquery").rglob("*.py")
    )
    for name in names:
        short = name.rsplit(".", 1)[-1]
        parent = name.rsplit(".", 1)[0]
        used = (
            re.search(rf"(import|from)\s+{re.escape(name)}\b", text)
            or re.search(
                rf"from\s+{re.escape(parent)}\s+import\s+[^\n]*\b{short}\b", text
            )
            or re.search(
                rf"from\s+{re.escape(parent)}\s+import\s*\([^)]*\b{short}\b", text
            )
        )
        assert used or name in entry, f"{name} is never imported"
