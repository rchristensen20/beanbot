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

# Copy remaining data
COPY data data

# Command to run the bot
CMD ["python", "src/bot.py"]