# Processor image. One image, one ENTRYPOINT, three uses:
#   vr-foraging-server process       — one session (what the worker launches)
#   vr-foraging-server work          — the worker itself
#   vr-foraging-server serve         — the dashboard
#
# `vr-foraging-packaging` (the published CLI) is on PATH too, for a human poking
# around inside the image; nothing launches it.
#
# Context is the repo root. `Dockerfile.dockerignore` keeps `.git` — setuptools_scm
# needs the tags, or the image is stamped `0.1.dev1+...`.
#
# Build: docker build -f docker/Dockerfile --build-arg GIT_COMMIT=$(git rev-parse HEAD) .

FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS build
# setuptools_scm shells out to `git describe` during `uv sync`; without git the version
# silently becomes a placeholder.
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
WORKDIR /app
# Manifests first, so the dependency layer caches before any source is copied.
COPY pyproject.toml uv.lock ./
COPY packaging/pyproject.toml ./packaging/
COPY server/pyproject.toml ./server/
# `--no-install-workspace`, not `--no-install-project`: the latter excludes only the
# root, so uv still tried to build the server member before `.git` was copied and died
# with "unable to detect version".
RUN uv sync --frozen --no-install-workspace --no-dev --all-packages
COPY . .
RUN uv sync --frozen --no-dev --all-packages

FROM python:3.13-slim-bookworm
ARG GIT_COMMIT=unknown
LABEL org.opencontainers.image.revision=$GIT_COMMIT
LABEL org.opencontainers.image.source=https://github.com/AllenNeuralDynamics/Aind.Behavior.VrForaging.Packaging
ENV PROCESSOR_GIT_COMMIT=$GIT_COMMIT
# opencv-python (transitive, via contraqctor) links against these at import time; the
# slim base has none and the import fails before any of our code runs. No video is decoded.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 libglib2.0-0 libsm6 libxext6 libxrender1 libgomp1 libxcb1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /app /app
ENV PATH="/app/.venv/bin:$PATH"
# `docker` CLI, not the daemon — only `work` uses it, to launch sibling containers.
COPY --from=docker:27-cli /usr/local/bin/docker /usr/local/bin/docker
RUN useradd -u 10001 -m runner
# Both mount points must EXIST here, owned by `runner`: Docker seeds a fresh named
# volume from the image dir (ownership included), else creates it root:root 0755 and
# this non-root user cannot write to its own work volume.
RUN mkdir -p /work /var/lib/vrf && chown runner:runner /work /var/lib/vrf
USER runner
# The server CLI: `process` is what writes a sidecar.
ENTRYPOINT ["vr-foraging-server"]
