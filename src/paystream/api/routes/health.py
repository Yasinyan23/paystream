from fastapi import APIRouter, Request
from opentelemetry.instrumentation.utils import suppress_instrumentation

from paystream.api.errors import ApiProblem

router = APIRouter(tags=["ops"])


@router.get("/health", summary="Liveness probe")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready", summary="Readiness probe: the database is reachable")
async def ready(request: Request) -> dict[str, str]:
    try:
        # Probed every few seconds: its queries must not become traces of their own.
        with suppress_instrumentation():
            await request.app.state.db.ping()
    except Exception as exc:
        raise ApiProblem(503, "service-unavailable", "Service unavailable", "Database is not reachable") from exc
    return {"status": "ready"}
