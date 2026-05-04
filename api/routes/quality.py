from __future__ import annotations

from datetime import date
from typing import Literal

import psycopg2
from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_db
from api.models.responses import QualityRecord
from api.services.query_service import (
    fetch_battery_quality,
    fetch_latest_quality,
    fetch_quality_history,
)

router = APIRouter(prefix="/api/v1", tags=["quality"])


@router.get("/quality/latest", response_model=list[QualityRecord])
def quality_latest(
    limit: int = Query(default=50, ge=1, le=500),
    conn: psycopg2.extensions.connection = Depends(get_db),
) -> list[QualityRecord]:
    return [QualityRecord(**row) for row in fetch_latest_quality(conn, limit)]


@router.get("/quality/history", response_model=list[QualityRecord])
def quality_history(
    start_date: date,
    end_date: date,
    line_id: str | None = None,
    channel: Literal["laser_a", "laser_b"] | None = None,
    limit: int = Query(default=3000, ge=1, le=10000),
    conn: psycopg2.extensions.connection = Depends(get_db),
) -> list[QualityRecord]:
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")
    rows = fetch_quality_history(conn, start_date, end_date, line_id, channel, limit)
    return [QualityRecord(**row) for row in rows]


@router.get("/batteries/{product_id}", response_model=list[QualityRecord])
def battery_detail(
    product_id: str,
    conn: psycopg2.extensions.connection = Depends(get_db),
) -> list[QualityRecord]:
    rows = fetch_battery_quality(conn, product_id)
    if not rows:
        raise HTTPException(status_code=404, detail="product_id not found")
    return [QualityRecord(**row) for row in rows]

