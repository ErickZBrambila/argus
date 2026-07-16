#!/usr/bin/env bash
# Standard Argus start script
# Usage: ./start.sh          — start in background
#        ./start.sh stop     — stop running instance
#        ./start.sh restart  — stop then start
#        ./start.sh status   — show if running

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/.argus.pid"
LOG_FILE="$SCRIPT_DIR/logs/argus.log"
PYTHON="$SCRIPT_DIR/.venv/bin/python"

start() {
    if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
        echo "Argus is already running (PID $(cat "$PID_FILE"))"
        return 1
    fi
    mkdir -p "$SCRIPT_DIR/logs"
    cd "$SCRIPT_DIR" || exit 1
    nohup "$PYTHON" -m argus.main >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "Argus started (PID $!) — logs: $LOG_FILE"
    echo "Dashboard: http://localhost:8000"
}

stop() {
    if [[ -f "$PID_FILE" ]]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            kill "$PID"
            rm -f "$PID_FILE"
            echo "Argus stopped (PID $PID)"
        else
            echo "PID $PID not found — clearing stale pid file"
            rm -f "$PID_FILE"
        fi
    else
        # Fallback: find by process name
        PIDS=$(pgrep -f "argus.main" 2>/dev/null)
        if [[ -n "$PIDS" ]]; then
            echo "$PIDS" | xargs kill
            echo "Argus stopped (PIDs: $PIDS)"
        else
            echo "Argus is not running"
        fi
    fi
}

status() {
    if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
        PID=$(cat "$PID_FILE")
        PYTHON_VER=$("$PYTHON" --version 2>&1)
        echo "Argus is running (PID $PID)"
        echo "Python: $PYTHON_VER"
        echo "Log:    $LOG_FILE"
        echo "Dashboard: http://localhost:8000"
    else
        echo "Argus is not running"
        rm -f "$PID_FILE" 2>/dev/null
    fi
}

case "${1:-start}" in
    start)   start ;;
    stop)    stop ;;
    restart) stop; sleep 2; start ;;
    status)  status ;;
    *) echo "Usage: $0 {start|stop|restart|status}"; exit 1 ;;
esac
