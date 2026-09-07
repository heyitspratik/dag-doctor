import pytest

from dag_doctor.core.exceptions import (
    BudgetExhaustedError,
    DagDoctorError,
    InvalidFailureEventError,
    ProviderUnavailableError,
    ReadOnlyViolationError,
    ResourceNotFoundError,
    ToolExecutionError,
    UnknownToolError,
)

_SUBCLASSES = [
    BudgetExhaustedError,
    InvalidFailureEventError,
    ProviderUnavailableError,
    ReadOnlyViolationError,
    ResourceNotFoundError,
    ToolExecutionError,
    UnknownToolError,
]


@pytest.mark.parametrize("error_type", _SUBCLASSES)
def test_every_error_descends_from_the_package_root(error_type):
    assert issubclass(error_type, DagDoctorError)


@pytest.mark.parametrize("error_type", _SUBCLASSES)
def test_every_error_carries_a_distinct_code_and_a_status(error_type):
    assert error_type.code != DagDoctorError.code
    assert 400 <= error_type.http_status < 600


def test_codes_are_unique_so_a_client_can_switch_on_them():
    codes = [error_type.code for error_type in _SUBCLASSES]

    assert len(codes) == len(set(codes))


def test_details_reach_the_error_envelope():
    error = ProviderUnavailableError("ollama is down", details={"base_url": "http://x:11434"})

    assert error.message == "ollama is down"
    assert error.details == {"base_url": "http://x:11434"}
    assert str(error) == "ollama is down"


def test_details_default_to_an_empty_mapping():
    assert ProviderUnavailableError("no context").details == {}


def test_tool_faults_are_catchable_as_one_family():
    # A caller that wants every tool fault should not have to enumerate the subclasses.
    for error_type in (UnknownToolError, ReadOnlyViolationError):
        assert issubclass(error_type, ToolExecutionError)
