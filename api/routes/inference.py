from __future__ import annotations

from fastapi import APIRouter

from api.models.requests import InferencePredictRequest
from api.models.responses import InferencePredictResponse
from inference.model import predict_rule

router = APIRouter(prefix="/api/v1", tags=["inference"])


@router.post("/inference/predict", response_model=InferencePredictResponse)
def inference_predict(payload: InferencePredictRequest) -> InferencePredictResponse:
    result = predict_rule(
        channel=payload.channel,
        cpd_score=payload.features.cpd_score,
        odd_even_gap=payload.features.odd_even_gap,
        record_count=payload.features.record_count,
    )
    return InferencePredictResponse(
        product_id=payload.product_id,
        channel=payload.channel,
        decision=result["decision"],
        score=result["score"],
        threshold=result["threshold"],
        inference_ms=result["inference_ms"],
        model_version=result["model_version"],
    )

