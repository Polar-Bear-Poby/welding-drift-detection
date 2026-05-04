from __future__ import annotations

import os
from datetime import date, timedelta

import requests
import streamlit as st

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")

st.set_page_config(page_title="Welding Drift Dashboard", layout="wide")
st.title("Welding Drift Dashboard")
st.caption("FastAPI + Streamlit demo for Session 7")


def api_get(path: str, params: dict | None = None):
    resp = requests.get(f"{API_BASE_URL}{path}", params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()


def api_post(path: str, payload: dict):
    resp = requests.post(f"{API_BASE_URL}{path}", json=payload, timeout=20)
    resp.raise_for_status()
    return resp.json()


page = st.sidebar.radio(
    "Menu",
    ("Overview", "Quality History", "Battery Detail", "Inference Demo"),
)

if page == "Overview":
    st.subheader("Overview")
    st.caption(
        "1 row = 1 battery process (laser_a or laser_b). "
        "quality_decision is determined by cpd_score proxy over 16 equal segments."
    )

    col_btn, col_auto = st.columns([2, 8])
    with col_btn:
        if st.button("🔄 Refresh Now"):
            pass  # 버튼을 누르면 전체 페이지가 rerun 되므로 pass만 해도 새로고침됨
    with col_auto:
        auto_refresh = st.checkbox("Auto-refresh (every 5 seconds)", value=True)

    @st.fragment(run_every=5 if auto_refresh else None)
    def render_overview_data():
        try:
            latest = api_get("/api/v1/quality/latest", params={"limit": 100})
            if latest:
                total = len(latest)
                # quality_decision: "drift" | "normal"
                drift_count = sum(1 for r in latest if r["quality_decision"] == "drift")
                normal_count = total - drift_count
                
                col1, col2, col3 = st.columns(3)
                col1.metric("Total (latest 100)", total)
                col2.metric("Normal", normal_count)
                col3.metric("Drift", drift_count, delta=f"{drift_count/total*100:.1f}%" if total else "0%")
                
                st.dataframe(latest[:30], use_container_width=True)
            else:
                st.info("No recent rows.")
        except Exception as exc:
            st.error(f"Failed to load overview: {exc}")

    render_overview_data()

elif page == "Quality History":
    st.subheader("Quality History")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        start_date = st.date_input("Start Date", value=date.today() - timedelta(days=7))
    with col2:
        end_date = st.date_input("End Date", value=date.today())
    with col3:
        line_id = st.text_input("Line ID (optional)", value="")
    with col4:
        channel = st.selectbox("Channel", ["all", "laser_a", "laser_b"])

    if st.button("Load History"):
        try:
            params = {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "limit": 3000,
            }
            if line_id.strip():
                params["line_id"] = line_id.strip()
            if channel != "all":
                params["channel"] = channel
            rows = api_get("/api/v1/quality/history", params=params)
            st.write(f"Rows: {len(rows)}")
            st.dataframe(rows, use_container_width=True, height=500)
            if rows:
                score_points = [
                    {"processed_at": r["processed_at"], "cpd_score": r["cpd_score"] or 0.0}
                    for r in rows
                ]
                st.line_chart(score_points, x="processed_at", y="cpd_score")
        except Exception as exc:
            st.error(f"Failed to load history: {exc}")

elif page == "Battery Detail":
    st.subheader("Battery Detail")
    product_id = st.text_input("Product ID", value="20220417_battery_001")
    if st.button("Load Battery"):
        try:
            rows = api_get(f"/api/v1/batteries/{product_id}")
            st.write(f"Rows: {len(rows)}")
            st.dataframe(rows, use_container_width=True)
        except Exception as exc:
            st.error(f"Failed to load battery detail: {exc}")

elif page == "Inference Demo":
    st.subheader("Inference Demo")
    product_id = st.text_input("Product ID", value="demo_battery_001")
    channel = st.selectbox("Channel", ["laser_a", "laser_b"])
    cpd_score = st.slider("CPD Score", min_value=0.0, max_value=0.3, value=0.03, step=0.001)
    odd_even_gap = st.slider(
        "Odd-Even Gap", min_value=0.0, max_value=0.2, value=0.01, step=0.001
    )
    record_count = st.slider("Record Count", min_value=1, max_value=32, value=16, step=1)

    if st.button("Run Inference"):
        payload = {
            "product_id": product_id,
            "channel": channel,
            "features": {
                "cpd_score": cpd_score,
                "odd_even_gap": odd_even_gap,
                "record_count": record_count,
            },
        }
        try:
            result = api_post("/api/v1/inference/predict", payload)
            st.success("Inference done")
            st.json(result)
        except Exception as exc:
            st.error(f"Inference failed: {exc}")

