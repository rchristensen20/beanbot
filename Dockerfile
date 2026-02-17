FROM python:3.12.3-slim

WORKDIR /app

# Install into the system environment
ENV UV_PROJECT_ENVIRONMENT="/usr/local/"
ENV UV_COMPILE_BYTECODE=1

# Install uv
RUN pip install --no-cache-dir uv

# Copy dependency definitions first (cached unless deps change)
COPY pyproject.toml uv.lock README.md ./

# Install dependencies only (not the project itself)
# --frozen: use exact versions from lock file
# Cache mount: even if pyproject.toml changes (version bump), packages
# are linked from cache instead of re-downloaded
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project

# Now copy source code (changes frequently, but deps are already installed)
COPY src src

# Install the project itself (fast — just links the local package)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen

# Create data directory (populated at runtime via volume mount or bot usage)
RUN mkdir -p data

# Non-root user: bot process can only write to data/
RUN useradd --create-home --uid 10000 beanbot && chown beanbot:beanbot data
USER beanbot

# Command to run the bot
CMD ["python", "src/bot.py"]
