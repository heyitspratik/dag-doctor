"""Assembling the investigation graph.

The shape, with the cycle that justifies a state machine over a chain:

    START -> triage
    triage          -> conclude          (a recognised signature the model agrees with)
                    -> gather_evidence   (otherwise)
    gather_evidence -> form_hypothesis
    form_hypothesis -> test_hypothesis
    test_hypothesis -> conclude          (confirmed, and confident enough)
                    -> gather_evidence   (refuted, and budget remains)   <- the loop
                    -> escalate          (budget exhausted)
    conclude -> END
    escalate -> END

The checkpointer is what makes an investigation survive a worker restart and lets a human
step through it afterwards. It is optional here only so the tests can run without a
database; the worker always supplies one.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph

from dag_doctor.core.models import FailureEvent
from dag_doctor.core.settings import BudgetSettings
from dag_doctor.graph.model import ModelCaller
from dag_doctor.graph.nodes.conclude import ConcludeNode
from dag_doctor.graph.nodes.escalate import EscalateNode
from dag_doctor.graph.nodes.form_hypothesis import FormHypothesisNode
from dag_doctor.graph.nodes.gather_evidence import GatherEvidenceNode
from dag_doctor.graph.nodes.test_hypothesis import TestHypothesisNode
from dag_doctor.graph.nodes.triage import TriageNode
from dag_doctor.graph.routing import (
    after_form_hypothesis,
    after_gather_evidence,
    test_router,
    triage_router,
)
from dag_doctor.graph.state import InvestigationState
from dag_doctor.graph.toolbox import Toolbox


def build_graph(
    caller: ModelCaller,
    toolbox: Toolbox,
    budgets: BudgetSettings,
    checkpointer: BaseCheckpointSaver[str] | None = None,
) -> CompiledStateGraph[InvestigationState]:
    """Assemble and compile the investigation graph.

    Args:
        caller: How nodes reach a model.
        toolbox: The tools the investigation may use.
        budgets: Limits and confidence thresholds, which the routing reads.
        checkpointer: Where step state is persisted. ``None`` runs without persistence,
            which is what the tests use.

    Returns:
        The compiled graph, ready to invoke.
    """
    graph: StateGraph[InvestigationState, None, InvestigationState, InvestigationState] = (
        StateGraph(InvestigationState)
    )

    graph.add_node("triage", TriageNode(caller))
    graph.add_node("gather_evidence", GatherEvidenceNode(caller, toolbox))
    graph.add_node("form_hypothesis", FormHypothesisNode(caller, toolbox))
    graph.add_node("test_hypothesis", TestHypothesisNode(caller, toolbox))
    graph.add_node("conclude", ConcludeNode(caller))
    graph.add_node("escalate", EscalateNode())

    graph.add_edge(START, "triage")
    graph.add_conditional_edges(
        "triage",
        triage_router(budgets),
        {"conclude": "conclude", "gather_evidence": "gather_evidence", "escalate": "escalate"},
    )
    graph.add_conditional_edges(
        "gather_evidence",
        after_gather_evidence,
        # conclude is the exhaustion exit: a round that learned nothing new ends here
        # rather than looping to re-read evidence the investigation already holds.
        {
            "form_hypothesis": "form_hypothesis",
            "conclude": "conclude",
            "escalate": "escalate",
        },
    )
    graph.add_conditional_edges(
        "form_hypothesis",
        after_form_hypothesis,
        {"test_hypothesis": "test_hypothesis", "escalate": "escalate"},
    )
    graph.add_conditional_edges(
        "test_hypothesis",
        test_router(budgets),
        # gather_evidence is the back edge: a refuted hypothesis sends the investigation
        # round again rather than concluding on a story that did not survive its test.
        {"conclude": "conclude", "gather_evidence": "gather_evidence", "escalate": "escalate"},
    )
    graph.add_edge("conclude", END)
    graph.add_edge("escalate", END)

    return graph.compile(checkpointer=checkpointer)


def initial_state(
    incident_id: UUID, failure: FailureEvent, budgets: BudgetSettings
) -> InvestigationState:
    """Build the state one investigation starts from.

    The budgets are copied onto the state rather than read from settings inside the nodes,
    so a replay of a past incident reruns under the limits it actually had.

    Args:
        incident_id: The incident being investigated.
        failure: The failure that opened it.
        budgets: The limits for this run.

    Returns:
        The starting state.
    """
    return InvestigationState(
        incident_id=incident_id,
        failure=failure,
        max_iterations=budgets.max_iterations,
        max_tool_calls=budgets.max_tool_calls,
    )


@asynccontextmanager
async def postgres_checkpointer(dsn: str) -> AsyncIterator[BaseCheckpointSaver[str]]:
    """Open the Postgres checkpointer, creating its tables on first use.

    Args:
        dsn: The agent's own database.

    Yields:
        A checkpointer bound to that database.
    """
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    # The saver speaks psycopg directly, so the SQLAlchemy driver prefix has to go.
    async with AsyncPostgresSaver.from_conn_string(dsn.replace("+psycopg", "")) as saver:
        await saver.setup()
        yield saver
