from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Literal

Channel = Literal["laser_a", "laser_b"]


@dataclass(frozen=True)
class RuleModelConfig:
    threshold: float = 0.7
    model_version: str = "rule-v1"


CONFIG = RuleModelConfig()


def _normalize(value: float, cap: float) -> float:
    if cap <= 0:
        return 0.0
    return max(0.0, min(1.0, value / cap))


def _inference_delay_seconds(channel: Channel) -> float:
    if channel == "laser_a":
        return random.uniform(0.012, 0.025)
    return random.uniform(0.028, 0.045)


def predict_rule(
    *,
    channel: Channel,
    cpd_score: float,
    odd_even_gap: float,
    record_count: int,
) -> dict:
    started = time.perf_counter()
    time.sleep(_inference_delay_seconds(channel))

    cpd_score_norm = _normalize(cpd_score, cap=0.2)
    gap_norm = _normalize(odd_even_gap, cap=0.1)
    count_penalty = 0.0 if record_count >= 16 else min(1.0, (16 - record_count) / 16)

    score = round(0.6 * cpd_score_norm + 0.3 * gap_norm + 0.1 * count_penalty, 4)
    decision = "drift" if score >= CONFIG.threshold else "normal"
    inference_ms = int((time.perf_counter() - started) * 1000)

    return {
        "decision": decision,
        "score": score,
        "threshold": CONFIG.threshold,
        "inference_ms": inference_ms,
        "model_version": CONFIG.model_version,
    }

