"""The Prometheus endpoint.

Deliberately unauthenticated and outside the versioned prefix, because that is where every
scraper looks and an API key on it means a scrape configuration nobody maintains.
"""

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

# Imported for the side effect of registering the collectors. Without it this endpoint
# would serve an empty document that looks like a working scrape.
from dag_doctor.core import metrics as _collectors

router = APIRouter(tags=["metrics"])


__all__ = ["router"]

_ = _collectors


@router.get("/metrics", summary="Prometheus metrics", include_in_schema=False)
async def metrics() -> Response:
    """Render the current values of every collector in this process."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
