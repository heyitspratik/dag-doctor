"""The README makes claims. These check the ones that could quietly stop being true.

Documentation rots silently: a renamed make target or a deleted endpoint leaves the README
confidently wrong, and nobody notices until a stranger tries to follow it.
"""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
README = (ROOT / "README.md").read_text()
MAKEFILE = (ROOT / "Makefile").read_text()

#: Commands the README tells a reader to run.
QUOTED_TARGETS = sorted(set(re.findall(r"`?make ([a-z-]+)`?", README)))


@pytest.mark.parametrize("target", QUOTED_TARGETS)
def test_every_make_target_the_readme_mentions_exists(target):
    assert re.search(rf"^{re.escape(target)}:", MAKEFILE, re.MULTILINE), (
        f"README tells the reader to run `make {target}`, which the Makefile does not define"
    )


@pytest.mark.parametrize(
    "document",
    sorted(set(re.findall(r"\]\((docs/[a-z-]+\.md)\)", README))),
)
def test_every_document_the_readme_links_exists(document):
    assert (ROOT / document).exists()


@pytest.mark.parametrize(
    "path",
    ["LICENSE", "CONTRIBUTING.md", "deploy/helm/dag-doctor/README.md"],
)
def test_the_readme_links_resolve(path):
    assert path in README
    assert (ROOT / path).exists()


def test_the_readme_still_publishes_no_unmeasured_numbers():
    # The single most important check in this file. An accuracy figure that was never
    # measured is worth less than nothing, and the temptation to fill the table in with
    # something plausible is exactly what this guards against.
    accuracy = README.split("## Accuracy")[1].split("## Architecture")[0]

    if "Not yet measured" in accuracy:
        assert "%" not in accuracy, (
            "the accuracy section claims a percentage while still marked unmeasured"
        )


def test_the_worked_example_is_either_real_output_or_marked_absent():
    example = README.split("## A worked example")[1].split("## Accuracy")[0]

    assert "Not yet captured" in example or "confidence" in example


def test_the_readme_states_the_limitations_the_spec_requires():
    limitations = README.split("## Limitations")[1].split("## Project structure")[0]

    for claim in ("local models", "never applies", "seeded", "heuristic", "read-only"):
        assert claim in limitations, f"the limitations section no longer mentions {claim!r}"


def test_the_readme_justifies_the_technology_rather_than_only_naming_it():
    # A reviewer respects a stated reason far more than an unexplained choice.
    reasons = README.split("## Why the technology choices")[1].split("## Extending")[0]

    assert "Redpanda" in reasons
    assert "LangGraph" in reasons
    assert "test_hypothesis -> gather_evidence" in reasons


def test_the_readme_carries_both_diagrams():
    assert README.count("```mermaid") == 2
    assert "stateDiagram-v2" in README
    assert "flowchart" in README


def test_the_tool_table_lists_every_registered_tool():
    # A tool added without a row here is a tool nobody reading the README knows exists.
    from tests.unit.tools.test_factory import EXPECTED_TOOLS

    table = README.split("### The nine tools")[1].split("All database access")[0]

    for tool in EXPECTED_TOOLS:
        assert f"`{tool}`" in table


def test_the_quickstart_needs_no_key_and_no_signup():
    quickstart = README.split("## Quickstart")[1].split("## How it works")[0]

    assert "Zero cost, zero signup" in quickstart
    # The model download is the one real cost, and hiding its size would be a small lie.
    assert "GB" in quickstart
