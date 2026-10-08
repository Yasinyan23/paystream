from fastapi import APIRouter, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from paystream.api.deps import require_api_key

router = APIRouter(tags=["ops"])


@router.get("/metrics", summary="Prometheus metrics", include_in_schema=False)
async def metrics(request: Request) -> Response:
    if request.app.state.settings.metrics_require_api_key:
        await require_api_key(request, request.headers.get("X-API-Key"))
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
