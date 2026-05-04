from __future__ import annotations

import psycopg2
from fastapi import APIRouter, Depends, HTTPException

from api.deps import get_db
from api.models.responses import RunSummaryResponse
from api.services.query_service import fetch_run_summary

router = APIRouter(prefix="/api/v1", tags=["runs"])


@router.get("/runs/{run_id}", response_model=RunSummaryResponse)
def get_run(
    run_id: str,
    conn: psycopg2.extensions.connection = Depends(get_db),
) -> RunSummaryResponse:
    row = fetch_run_summary(conn, run_id)
    if not row:
        raise HTTPException(status_code=404, detail="run_id not found")
    return RunSummaryResponse(**row)

