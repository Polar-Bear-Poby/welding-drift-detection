from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter

from api.models.responses import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok", time=datetime.now(timezone.utc))

