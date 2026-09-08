"""The read-only guard.

The database grants a SELECT-only role, which is the outer guarantee. These cover the
inner one, which exists because a cloned repository gets reconfigured by people who never
read the grant, and because a prompt injection in a log line must not be able to turn a
diagnosis into a write.
"""

import pytest

from dag_doctor.core.exceptions import ReadOnlyViolationError
from dag_doctor.tools.sql import (
    TableRef,
    read_only_text,
    require_read_only,
    validate_identifier,
)


def test_a_plain_table_name_parses():
    table = TableRef.parse("orders")

    assert table.schema is None
    assert table.qualified == "orders"


def test_a_qualified_table_name_parses():
    table = TableRef.parse("raw.orders")

    assert table.schema == "raw"
    assert table.name == "orders"
    assert str(table) == "raw.orders"


def test_surrounding_whitespace_is_tolerated():
    assert TableRef.parse("  raw.orders  ").qualified == "raw.orders"


@pytest.mark.parametrize(
    "raw",
    [
        "orders; DROP TABLE customers",
        'orders" OR "1"="1',
        "raw.orders.extra",
        "a b",
        "",
        "1orders",
        "orders--comment",
        "pg_class WHERE 1=1",
        "orders'",
        "raw.'orders'",
    ],
)
def test_anything_that_is_not_a_plain_table_name_is_refused(raw):
    # Quoting would not be enough on its own. Refusing is simpler to reason about and
    # costs nothing, because real table names are plain identifiers.
    with pytest.raises(ReadOnlyViolationError):
        TableRef.parse(raw)


def test_the_rejected_name_is_reported_so_a_human_can_see_what_was_tried():
    with pytest.raises(ReadOnlyViolationError) as excinfo:
        TableRef.parse("orders; DROP TABLE customers")

    assert excinfo.value.details["table"] == "orders; DROP TABLE customers"


@pytest.mark.parametrize("name", ["customer_id", "_private", "col$1", "A1"])
def test_a_plain_column_name_is_accepted(name):
    assert validate_identifier(name, kind="column") == name


@pytest.mark.parametrize("name", ["count(*)", "a, b", "x; select 1", "", "a-b"])
def test_a_column_name_that_is_an_expression_is_refused(name):
    with pytest.raises(ReadOnlyViolationError):
        validate_identifier(name, kind="column")


@pytest.mark.parametrize(
    "sql",
    ["SELECT 1", "select a from b", "  WITH x AS (SELECT 1) SELECT * FROM x  ", "SELECT 1;"],
)
def test_a_read_statement_is_allowed(sql):
    assert require_read_only(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO orders VALUES (1)",
        "UPDATE orders SET status = 'x'",
        "DELETE FROM orders",
        "DROP TABLE orders",
        "TRUNCATE orders",
        "GRANT ALL ON orders TO public",
        "CREATE TABLE t (a int)",
        "ALTER TABLE orders ADD COLUMN x int",
        "SELECT 1; DROP TABLE orders",
        "SELECT 1; DELETE FROM orders",
        "WITH x AS (DELETE FROM orders RETURNING 1) SELECT * FROM x",
        "COPY orders TO '/tmp/out.csv'",
        "",
    ],
)
def test_anything_that_could_write_is_refused(sql):
    with pytest.raises(ReadOnlyViolationError):
        require_read_only(sql)


def test_stacked_statements_are_named_as_the_problem():
    with pytest.raises(ReadOnlyViolationError) as excinfo:
        require_read_only("SELECT 1; SELECT 2")

    assert "Multiple statements" in excinfo.value.message


def test_a_text_clause_is_checked_before_it_is_built():
    with pytest.raises(ReadOnlyViolationError):
        read_only_text("DELETE FROM orders")
