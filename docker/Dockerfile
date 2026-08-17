# Processor image (§12). One image, three entrypoints:
#   vr-foraging-packaging     — the per-session processor (default ENTRYPOINT)
#   vr-foraging-orchestrator work / serve — the worker / dashboard (compose overrides the command)
#
# Build context is the repo root (see docker-compose.yaml / CI workflow), and
# `.dockerignore` deliberately does NOT exclude `.git` — setuptools_scm needs
# the tag history to produce a real version string, not `0.1.dev1+...` (§12).
#
# Build with: docker build -f docker/Dockerfile --build-arg GIT_COMMIT=$(git rev-parse HEAD) .

FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS build
# `git` is not in the base image, but setuptools_scm shells out to `git describe`
# during `uv sync`'s build step — without it, every sidecar and ledger row would
# silently get a placeholder dev version instead of a real one (§12).
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev --extra orchestration
COPY . .
RUN uv sync --frozen --no-dev --extra orchestration

FROM python:3.13-slim-bookworm
ARG GIT_COMMIT=unknown
LABEL org.opencontainers.image.revision=$GIT_COMMIT
LABEL org.opencontainers.image.source=https://github.com/AllenNeuralDynamics/Aind.Behavior.VrForaging.Packaging
ENV PROCESSOR_GIT_COMMIT=$GIT_COMMIT
# opencv-python (transitive, via contraqctor's camera contract) dynamically
# links against these at import time; the slim base has none of them and the
# import fails outright (`ImportError: libxcb.so.1: cannot open shared object
# file`) before any of our own code runs. No video is ever decoded in this
# image — these are opencv's own import-time requirement, not a feature we use.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 libglib2.0-0 libsm6 libxext6 libxrender1 libgomp1 libxcb1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /app /app
ENV PATH="/app/.venv/bin:$PATH"
# Static `docker` CLI binary, not the daemon — needed only by the `worker`
# entrypoint (`vr-foraging-orchestrator work`), which launches sibling processor
# containers through the mounted docker.sock (§4a). Kept in the same image
# as the processor entrypoint per the "one image, three entrypoints" design
# (§13 compose), so this is a few MB the processor path never exercises.
COPY --from=docker:27-cli /usr/local/bin/docker /usr/local/bin/docker
RUN useradd -u 10001 -m runner
USER runner
ENTRYPOINT ["vr-foraging-packaging"]
