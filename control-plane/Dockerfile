FROM python:3.10

ENV PIP_DEFAULT_TIMEOUT=100
ENV UV_PROJECT_ENVIRONMENT=/opt/origo-control-plane/.venv
ENV PATH="/opt/origo-control-plane/.venv/bin:${PATH}"

RUN apt-get update && apt-get install -y git openssh-client libgomp1 \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir uv

WORKDIR /opt/origo-control-plane
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

ENV DAGSTER_HOME=/opt/origo-control-plane
COPY . /opt/origo-control-plane
RUN uv sync --frozen --no-dev
