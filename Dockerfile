FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /mtgjson

RUN apt update \
    && apt install -y --no-install-recommends git bzip2 xz-utils zip htop  \
    && apt purge -y --auto-remove \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies (cached when only source code changes)
COPY ./pyproject.toml ./uv.lock ./
RUN uv sync --no-dev --frozen --no-install-project

# Copy source and install the project itself
COPY ./mtgjson5 ./mtgjson5
RUN uv sync --no-dev --frozen

ENTRYPOINT ["uv", "run", "python3", "-m", "mtgjson5", "--use-envvars"]
