"""Versioned prompt templates.

Templates live in files rather than in string literals so that a prompt change shows up as
a reviewable diff, and they carry a version in the filename so an investigation's trace can
record which wording produced it. Replaying a past incident against a new prompt is the
main reason this repository puts a log between Airflow and the agent, and it is worthless
if nobody can tell which prompt ran.

``string.Template`` rather than ``str.format``: prompts are full of JSON braces, and a
formatting language that treats them as syntax turns a prompt edit into a crash.
"""

from functools import cache
from pathlib import Path
from string import Template

#: Bumped when a template changes in a way that could change behaviour.
PROMPT_VERSION = "v3"

_DIRECTORY = Path(__file__).parent


@cache
def load(name: str, version: str = PROMPT_VERSION) -> str:
    """Read a template from disk, once per process.

    Args:
        name: The template name, such as ``"triage"``.
        version: The template version.

    Returns:
        The raw template text.

    Raises:
        FileNotFoundError: If no such template exists, which is a packaging bug rather
            than anything a running investigation should try to recover from.
    """
    return (_DIRECTORY / f"{name}.{version}.md").read_text(encoding="utf-8")


def render(name: str, version: str = PROMPT_VERSION, **values: object) -> str:
    """Render a template.

    Args:
        name: The template name.
        version: The template version.
        **values: Substitutions. A missing one raises rather than rendering a prompt with
            a hole in it.

    Returns:
        The rendered prompt.
    """
    return Template(load(name, version)).substitute(values)


def names() -> list[str]:
    """Every template shipped, for the prompt-coverage test."""
    return sorted(path.name.split(".")[0] for path in _DIRECTORY.glob(f"*.{PROMPT_VERSION}.md"))
