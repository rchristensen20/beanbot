FROM python:3.12.3-slim

WORKDIR /app

# Install into the system environment
ENV UV_PROJECT_ENVIRONMENT="/usr/local/"
ENV UV_COMPILE_BYTECODE=1

# Install uv
RUN pip install --no-cache-dir uv

# Copy dependency definitions and metadata needed for the build
COPY pyproject.toml .
COPY uv.lock .
COPY README.md .
COPY src src

# Install dependencies (including the project itself)
# --frozen: Use exact versions from the lock file
RUN uv sync --frozen

# Create data directory (populated at runtime via volume mount or bot usage)
RUN mkdir -p data

# Non-root user: bot process can only write to data/
RUN useradd --create-home --uid 1000 beanbot && chown beanbot:beanbot data
USER beanbot

# Command to run the bot
CMD ["python", "src/bot.py"]