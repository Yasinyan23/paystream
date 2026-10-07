from fastapi import APIRouter, Request

from paystream.api.errors import ApiProblem

router = APIRouter(tags=["ops"])


@router.get("/health", summary="Liveness probe")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready", summary="Readiness probe: the database is reachable")
async def ready(request: Request) -> dict[str, str]:
    try:
        await request.app.state.db.ping()
    except Exception as exc:
        raise ApiProblem(503, "service-unavailable", "Service unavailable", "Database is not reachable") from exc
    return {"status": "ready"}
