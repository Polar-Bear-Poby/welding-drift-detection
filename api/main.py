from __future__ import annotations

from fastapi import FastAPI

from api.routes.experiment import router as experiment_router
from api.routes.health import router as health_router
from api.routes.inference import router as inference_router
from api.routes.quality import router as quality_router
from api.routes.runs import router as runs_router

app = FastAPI(
    title="Welding Drift API",
    version="0.1.0",
    description="API for welding quality history and rule-based inference demo.",
)

app.include_router(health_router)
app.include_router(quality_router)
app.include_router(runs_router)
app.include_router(inference_router)
app.include_router(experiment_router)

