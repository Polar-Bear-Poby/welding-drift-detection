from __future__ import annotations

import os
from datetime import date, datetime, timedelta

import requests
import streamlit as st

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")

st.set_page_config(page_title="Welding Drift Dashboard", layout="wide")
st.title("Welding Drift Dashboard")


def api_get(path: str, params: dict | None = None):
    resp = requests.get(f"{API_BASE_URL}{path}", params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()


def api_post(path: str, payload: dict):
    resp = requests.post(f"{API_BASE_URL}{path}", json=payload, timeout=20)
    resp.raise_for_status()
    return resp.json()


def api_post_query(path: str, params: dict):
    """Query-param 방식 POST (JSON body 없음)."""
    resp = requests.post(f"{API_BASE_URL}{path}", params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()


page = st.sidebar.radio(
    "Menu",
    (
        "Experiment Control",
        "Consumer View",
        "Overview",
    ),
)

st.sidebar.divider()
st.sidebar.caption("**Experiment Control** — 실험 환경 설정")
st.sidebar.caption("**Consumer View** — 실시간 컨슈머 처리 현황")
st.sidebar.caption("**Overview** — 최종 결과 집계")
st.sidebar.divider()
st.sidebar.info("New Experiment Environment (CSV-based, No PostgreSQL)")

if page == "Experiment Control":
    st.subheader("⚙️ 실험 환경 설정")
    st.caption("실시간 용접 드리프트 탐지 파이프라인의 파라미터를 설정하고 실험을 제어합니다.")

    # ── 상태 조회 ─────────────────────────────────────────────────────────────
    try:
        _exp = api_get("/api/v1/realtime/experiment/status")
        is_running = _exp.get("running", False)
        current_pid = _exp.get("pid")
    except Exception:
        is_running = False
        current_pid = None

    if is_running:
        st.success(f"현재 실험이 실행 중입니다. (PID: {current_pid})")
    else:
        st.info("실험 대기 중입니다.")

    st.divider()

    # ── 파라미터 설정 ─────────────────────────────────────────────────────────
    with st.container(border=True):
        st.markdown("#### 파라미터 설정")
        p1, p2, p3, p4 = st.columns(4)
        param_batteries = p1.number_input("배터리 수", 1, 500, 20, 1,
                                          disabled=is_running,
                                          help="총 처리 배터리 수")
        param_lines     = p2.number_input("생산라인",  1, 10,   2,  1,
                                          disabled=is_running,
                                          help="DataFeeder 스레드 수")
        param_consumers = p3.selectbox("컨슈머 수", [2, 4, 6, 8],
                                       index=1,
                                       disabled=is_running,
                                       help="짝수 — laser_a/b 균등 배분")
        param_interval  = p4.number_input("주기(초)", 0.5, 30.0, 3.0, 0.5,
                                          disabled=is_running,
                                          help="DataFeeder 파일 생성 주기")
        
        st.markdown("##### 처리 지연 설정 (시뮬레이션)")
        p5, p6 = st.columns(2)
        param_la_delay  = p5.number_input("laser_a 지연(초)", 0.0, 30.0, 2.0, 0.5,
                                          disabled=is_running,
                                          help="laser_a Consumer 처리 지연")
        param_lb_delay  = p6.number_input("laser_b 지연(초)", 0.0, 30.0, 2.0, 0.5,
                                          disabled=is_running,
                                          help="laser_b Consumer 처리 지연")

    st.write("")
    bc1, bc2 = st.columns(2)
    with bc1:
        if st.button("▶  실험 시작", disabled=is_running,
                     type="primary", use_container_width=True):
            try:
                r = api_post_query("/api/v1/realtime/experiment/start", {
                    "batteries": param_batteries, "lines": param_lines,
                    "consumers": param_consumers, "interval": param_interval,
                    "la_delay": param_la_delay,   "lb_delay": param_lb_delay,
                })
                st.success(f"실험 시작됨 (PID: {r.get('pid')})")
                st.rerun()
            except Exception as exc:
                st.error(str(exc))
    with bc2:
        if st.button("■  실험 종료 및 데이터 정리", type="secondary",
                     use_container_width=True):
            try:
                r = api_post_query("/api/v1/realtime/experiment/stop", {})
                st.warning("실험 종료 및 잔여 데이터가 삭제되었습니다.")
                st.rerun()
            except Exception as exc:
                st.error(str(exc))

elif page == "Consumer View":
    st.subheader("👷 컨슈머 전용 뷰")
    st.caption("실시간 파이프라인 단계별 처리 현황을 모니터링합니다.")

    auto_refresh = st.toggle("Auto-refresh (2초)", value=True, key="cons_rf")

    @st.fragment(run_every=2 if auto_refresh else None)
    def render_consumer_view():
        try:
            cp = api_get("/api/v1/realtime/consumer/progress")
            c_states  = cp.get("consumers", {})
            done_batt = cp.get("done_batteries", [])
            last_ts   = (cp.get("last_ts") or "")[:19].replace("T", " ")

            # ── 상단 통계 ─────────────────────────────────────────────────────
            today = date.today().strftime("%Y-%m-%d")
            s1, s2, s3 = st.columns(3)
            s1.metric("오늘의 완료 배터리", f"{len(done_batt)} 개", f"기준일: {today}")
            
            # 드리프트 탐지 수식 및 완료 수
            done_segments = len(done_batt) * 16 # 채널 기준
            s2.metric("용접 구간 데이터 추출", f"{done_segments} 개 완료", "수식: 배터리 * 용접 구간")
            s3.metric("최근 처리 시각", last_ts.split(" ")[-1] if last_ts else "—")

            st.divider()

            # ── 탭 구성 ───────────────────────────────────────────────────────
            tab_recomb, tab_extract, tab_drift = st.tabs([
                "🔄 재결합 (Recombination)",
                "✂️ 용접 구간 데이터 추출 (Segment Extraction)",
                "🔍 드리프트 탐지 (Drift Detection)"
            ])

            def render_active_consumers(stage_name, container):
                # 해당 단계에 있는 컨슈머만 필터링
                active = {k: v for k, v in c_states.items() if v.get("stage") == stage_name}
                
                if not active:
                    container.info(f"현재 '{stage_name}' 단계에서 처리 중인 데이터가 없습니다.")
                    return

                # 채널별 그룹화
                la_active = {k: v for k, v in active.items() if v.get("channel") == "laser_a"}
                lb_active = {k: v for k, v in active.items() if v.get("channel") == "laser_b"}

                def render_cards(label, consumers, color):
                    if not consumers: return
                    st.markdown(f"**{label}**")
                    cids = sorted(consumers.keys(), key=lambda x: int(x) if x.isdigit() else 0)
                    cols = st.columns(min(len(cids), 4))
                    for i, cid in enumerate(cids):
                        info = consumers[cid]
                        bid  = info.get("battery_id", "-")
                        ts_s = (info.get("ts") or "")[11:19]
                        with cols[i % len(cols)]:
                            with st.container(border=True):
                                st.markdown(
                                    f"<div style='text-align:center'>"
                                    f"<div style='font-size:0.8rem;color:#888'>{label} Consumer {cid}</div>"
                                    f"<div style='font-size:1.2rem;font-weight:bold;color:{color}'>battery_{bid}</div>"
                                    f"<div style='font-size:0.7rem;color:#555'>{ts_s} 처리 중</div>"
                                    f"</div>",
                                    unsafe_allow_html=True
                                )

                with container:
                    render_cards("Laser A", la_active, "#00BFFF")
                    st.write("")
                    render_cards("Laser B", lb_active, "#FF69B4")

            render_active_consumers("재결합", tab_recomb)
            render_active_consumers("용접 구간 데이터 추출", tab_extract)
            render_active_consumers("드리프트 탐지", tab_drift)

        except Exception as exc:
            st.error(f"데이터 조회 실패: {exc}")

    render_consumer_view()

elif page == "Overview":
    st.subheader("📊 Overview — 최종 결과 집계")
    st.caption("실험 진행률 및 품질 판정 결과를 실시간으로 확인합니다.")

    try:
        prog = api_get("/api/v1/realtime/progress")
        processed = prog.get("processed_batteries", 0)
        drift     = prog.get("drift_count", 0)
        normal    = prog.get("normal_count", 0)
        pct       = prog.get("progress_pct", 0.0)

        m1, m2, m3 = st.columns(3)
        m1.metric("처리 완료 배터리", f"{processed}개")
        m2.metric("정상 (Normal)", f"{normal}건")
        m3.metric("이상 (Drift)", f"{drift}건", delta=f"{drift/(drift+normal)*100:.1f}%" if (drift+normal) else "0%")

        st.progress(min(pct / 100.0, 1.0), text=f"전체 실험 진행률: {pct:.1f}%")

        st.divider()

        # 최근 결과 테이블
        rows = api_get("/api/v1/realtime/latest", params={"limit": 100})
        if rows:
            st.markdown("#### 최근 품질 판정 이력")
            st.dataframe(rows, use_container_width=True, height=400)
            
            st.markdown("#### CPD Score 분포")
            chart_data = [{"battery": f"{r['battery_id']}_{r.get('channel_name','')}", "score": r.get("cpd_score") or 0.0} for r in rows]
            st.bar_chart(chart_data, x="battery", y="score")
        else:
            st.info("아직 결과 데이터가 없습니다.")
    except Exception as exc:
        st.error(f"데이터 조회 실패: {exc}")
