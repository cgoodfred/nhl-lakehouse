FROM python:3.12-slim

# uv for fast, lockfile-driven installs.
COPY --from=ghcr.io/astral-sh/uv:0.5.0 /uv /usr/local/bin/uv

WORKDIR /app

# Install deps first so they cache across app edits.
COPY pyproject.toml uv.lock /app/
RUN uv sync --frozen --no-dev

COPY app.py lib.py /app/
COPY pages/ /app/pages/

EXPOSE 8501

ENTRYPOINT ["uv", "run", "--no-sync", "streamlit", "run", "app.py", \
            "--server.port=8501", "--server.address=0.0.0.0", \
            "--server.headless=true"]
