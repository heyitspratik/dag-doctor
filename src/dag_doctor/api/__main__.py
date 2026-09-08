"""Serving the API.

A module of its own so the container entrypoint is a console script rather than a uvicorn
command line duplicated between the Dockerfile, compose, and the Helm chart.
"""

import uvicorn

from dag_doctor.api.main import create_app
from dag_doctor.core.settings import get_settings


def main() -> None:
    """Run the API server."""
    settings = get_settings()
    uvicorn.run(
        create_app(settings),
        host="0.0.0.0",
        port=8000,
        log_config=None,
    )


if __name__ == "__main__":
    main()
