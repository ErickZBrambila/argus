VENV        := .venv/bin
ARGUS       := $(VENV)/argus
ARGUS_WEB   := $(VENV)/argus-web
LOG         := argus.log
WEB_LOG     := argus-web.log

.PHONY: start stop restart status logs

start:
	@echo "Starting argus..."
	@nohup $(ARGUS) >> $(LOG) 2>&1 &
	@sleep 2
	@echo "Starting argus-web..."
	@nohup $(ARGUS_WEB) >> $(WEB_LOG) 2>&1 &
	@sleep 3
	@$(MAKE) --no-print-directory status

stop:
	@echo "Stopping argus and argus-web..."
	@pkill -9 -f "$(VENV)/argus" 2>/dev/null || true
	@lsof -ti :8000 | xargs kill -9 2>/dev/null || true
	@sleep 3
	@echo "Stopped."

restart: stop
	@$(MAKE) --no-print-directory start

status:
	@echo ""
	@pgrep -f "$(VENV)/argus$$" > /dev/null \
		&& echo "  argus      ✓  running  (pid $$(pgrep -f '$(VENV)/argus$$'))" \
		|| echo "  argus      ✗  stopped"
	@lsof -ti :8000 > /dev/null \
		&& echo "  argus-web  ✓  running  (port 8000)" \
		|| echo "  argus-web  ✗  stopped"
	@echo ""

logs:
	@tail -f $(LOG)
