"""Recording what the warehouse looked like before anything broke.

``compare_schema_snapshot`` and ``profile_table`` answer "what changed", and neither can
answer it the first time they see a table. On a cold database the snapshot tool records the
*current* shape as its baseline, which on the seeded scenarios is the shape after the drift
has already happened, so it reports "unchanged" forever and no drift hypothesis can be
confirmed.

In production the agent would be observing these tables continuously and the baseline would
already exist. For a demonstration it has to be established deliberately, before the
failures are seeded. That is what this does.
"""

import argparse
import asyncio

from dag_doctor.core.logging import configure_logging, get_logger
from dag_doctor.core.settings import Settings, get_settings
from dag_doctor.db.session import build_engine, build_session_factory
from dag_doctor.tools.connections import ConnectionRegistry
from dag_doctor.tools.data_profile import ProfileTable
from dag_doctor.tools.schema_inspect import record_snapshot, reflect_columns

logger = get_logger(__name__)

#: The warehouse tables the seeded scenarios read or damage. Snapshotting a table nothing
#: touches would cost time and tell nobody anything.
WAREHOUSE_TABLES: tuple[str, ...] = (
    "raw.orders",
    "raw.customers",
    "raw.payments_staging",
    "raw.events_daily",
    "raw.partner_feed",
    "raw.order_tags",
)


async def record(settings: Settings, tables: tuple[str, ...] = WAREHOUSE_TABLES) -> int:
    """Snapshot and profile each table, so later comparisons have something to compare to.

    Args:
        settings: Application settings, for the databases and the tool timeout.
        tables: The warehouse tables to record.

    Returns:
        How many tables were recorded successfully.
    """
    engine = build_engine(settings.db)
    session_factory = build_session_factory(engine)
    connections = ConnectionRegistry.from_settings(settings)
    profile = ProfileTable(connections, session_factory, settings.budgets.tool_timeout_s)

    recorded = 0
    try:
        for table in tables:
            try:
                columns = await reflect_columns(connections, "warehouse", table)
            except Exception as exc:
                # Not fatal. A table that is not there simply has no baseline, and the
                # tools say so later rather than inventing one.
                logger.warning(
                    "baseline.skipped", table=table, error=f"{type(exc).__name__}: {exc}"
                )
                continue

            async with session_factory() as session:
                # Unconditionally, so a stale snapshot cannot survive and leave every
                # later comparison measuring against the wrong reference.
                record_snapshot(session, "warehouse", table, columns)
                await session.commit()

            profile_result = await profile.run({"connection": "warehouse", "table": table})
            recorded += 1
            logger.info(
                "baseline.recorded",
                table=table,
                columns=len(columns),
                profiled=profile_result.ok,
            )
    finally:
        await connections.dispose()
        await engine.dispose()
    return recorded


def main() -> int:
    """Entry point for ``make baseline``."""
    parser = argparse.ArgumentParser(
        description="Record the warehouse's shape before the failures are seeded."
    )
    parser.add_argument("--table", action="append", default=None, help="Repeatable")
    arguments = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.app_env, settings.log_level)
    tables = tuple(arguments.table) if arguments.table else WAREHOUSE_TABLES
    recorded = asyncio.run(record(settings, tables))
    logger.info("baseline.complete", recorded=recorded, of=len(tables))
    return 0 if recorded else 1


if __name__ == "__main__":
    raise SystemExit(main())
