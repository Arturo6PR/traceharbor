# syntax=docker/dockerfile:1.7

FROM python:3.12.14-slim-bookworm AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /build

COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN python -m pip wheel --wheel-dir /wheels .

FROM python:3.12.14-slim-bookworm AS runtime

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN groupadd --gid 10001 traceharbor \
    && useradd --uid 10001 --gid traceharbor --no-create-home --shell /usr/sbin/nologin traceharbor \
    && install -d -o traceharbor -g traceharbor /app /var/lib/traceharbor
COPY --from=builder /wheels /wheels
RUN python -m pip install /wheels/*.whl \
    && rm -rf /wheels

WORKDIR /app
USER 10001:10001

ENTRYPOINT ["traceharbor"]
CMD ["--help"]
