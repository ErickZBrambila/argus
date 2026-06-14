FROM python:3.11-slim

# Build deps for packages with C extensions (pandas, numpy, cryptography)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Install source — two-step for better layer caching
WORKDIR /app
COPY pyproject.toml .
COPY argus/ argus/
RUN pip install --no-cache-dir -e .

# /data is the runtime working directory — SQLite and flashcards land here.
# Mount a named volume at /data for persistence across container restarts.
WORKDIR /data

EXPOSE 8000

# WEB_HOST must be 0.0.0.0 inside the container so the port is reachable
# from the host via Docker's port mapping. ARGUS_NO_TERMINAL disables Rich
# terminal UI (no TTY in a detached container).
ENV WEB_HOST=0.0.0.0 \
    ARGUS_NO_TERMINAL=1

# Run as non-root to limit blast radius if a dependency has a vulnerability
RUN useradd --no-create-home --shell /bin/false argus \
    && chown -R argus:argus /data
USER argus

CMD ["argus"]
