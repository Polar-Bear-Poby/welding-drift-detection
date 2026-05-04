import sys
import os
import signal
from streamlit.web import cli as stcli

def force_quit(signum, frame):
    print("\n[Force Quit] Streamlit을 강제로 종료합니다...")
    os._exit(0)  # 스레드 무시하고 즉시 프로세스 사살

if __name__ == "__main__":
    # Ctrl+C (SIGINT) 신호를 가로채서 강제 종료 함수 실행
    signal.signal(signal.SIGINT, force_quit)
    
    # Streamlit 명령어 주입
    sys.argv = ["streamlit", "run", "frontend/app.py"]
    
    try:
        sys.exit(stcli.main())
    except KeyboardInterrupt:
        force_quit(signal.SIGINT, None)
