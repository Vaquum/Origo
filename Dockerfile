FROM python:3.11.12

ARG ORIGO_CODE_SHA
ENV ORIGO_CODE_SHA=${ORIGO_CODE_SHA}
LABEL org.opencontainers.image.revision=${ORIGO_CODE_SHA}

ENV PIP_DEFAULT_TIMEOUT=100

RUN apt-get update && apt-get install -y git openssh-client libgomp1

COPY docker-requirements.txt .
RUN pip install --no-cache-dir -r docker-requirements.txt

ENV DAGSTER_HOME=/opt/app
ENV PYTHONPATH=/opt/app
WORKDIR /opt/app
COPY . /opt/app
RUN mkdir -p /opt/dagster-instance && pip install --no-cache-dir .
