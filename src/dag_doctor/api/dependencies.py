"""What routes are given, and how they get it.

Everything a route needs arrives through a dependency so a test can substitute it. The
application's own wiring lives on ``app.state``, put there by the lifespan, which keeps
this module free of module-level singletons that tests would have to reach around.
"""

import base64
import binascii
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Annotated

from fastapi import Depends, Header, Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dag_doctor.core.exceptions import ConfigValidationError, DagDoctorError
from dag_doctor.core.settings import Settings


class UnauthorisedError(DagDoctorError):
    """The API key is missing or wrong."""

    code = "UNAUTHORISED"
    http_status = 401


def get_settings_from(request: Request) -> Settings:
    """Return the settings this application was built with."""
    settings: Settings = request.app.state.settings
    return settings


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    """Open a session for one request.

    Read-only for every route that uses it, so nothing is committed; the one route that
    writes commits explicitly.
    """
    factory: async_sessionmaker[AsyncSession] = request.app.state.session_factory
    async with factory() as session:
        yield session


async def require_api_key(
    request: Request,
    x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> None:
    """Check the API key, when one is configured.

    An unset key leaves the API open, which is what makes the quickstart work without
    ceremony. Setting one turns it on everywhere at once rather than per route, so a route
    added later cannot be left unprotected by omission.

    Raises:
        UnauthorisedError: If a key is configured and the request did not present it.
    """
    configured = get_settings_from(request).api_key
    if configured is None:
        return
    if x_api_key != configured.get_secret_value():
        raise UnauthorisedError("A valid X-API-Key header is required")


def encode_cursor(received_at: datetime, identifier: str) -> str:
    """Encode a pagination cursor.

    Opaque on purpose: a client that parses it will depend on the ordering, and the
    ordering is ours to change.
    """
    return base64.urlsafe_b64encode(f"{received_at.isoformat()}|{identifier}".encode()).decode()


def decode_cursor(cursor: str) -> tuple[datetime, str]:
    """Decode a pagination cursor.

    Args:
        cursor: The value handed out by a previous page.

    Returns:
        The timestamp and identifier to resume after.

    Raises:
        ConfigValidationError: If the cursor is not one this API issued.
    """
    try:
        raw = base64.urlsafe_b64decode(cursor.encode()).decode()
        timestamp, identifier = raw.split("|", 1)
        parsed = datetime.fromisoformat(timestamp)
    except (ValueError, binascii.Error, UnicodeDecodeError) as exc:
        raise ConfigValidationError(
            "That cursor is not one this API issued", details={"cursor": cursor[:64]}
        ) from exc
    return (parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)), identifier


SessionDep = Annotated[AsyncSession, Depends(get_session)]
SettingsDep = Annotated[Settings, Depends(get_settings_from)]
ApiKeyDep = Annotated[None, Depends(require_api_key)]
