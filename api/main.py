from __future__ import annotations

from fastapi import FastAPI

from api.routes.realtime import router as realtime_router

app = FastAPI(
    title="Welding Drift API (New Experiment Environment)",
    version="1.0.0",
    description=(
        "API for real-time welding drift detection experiment (new_src).\n\n"
        "This version is optimized for the CSV-based FileWatcher pipeline and does not require PostgreSQL."
    ),
)

# New experiment environment router (CSV-based, no PostgreSQL required)
app.include_router(realtime_router)

