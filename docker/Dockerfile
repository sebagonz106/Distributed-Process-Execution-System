FROM python:3.12-slim

# gcc and make are the toolchain of the demonstration jobs (plan/01, D01.7).
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libc6-dev make \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Ray reports usage statistics by default; this project sends none.
# PYTHONPATH lets the processes Ray starts on every node import dpes (D07.11).
ENV RAY_USAGE_STATS_ENABLED=0 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app
COPY dpes dpes
COPY tests tests
COPY experiments experiments
