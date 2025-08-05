# Use official Python 3.13 slim image
FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY *.py ./

# Create logs directory
RUN mkdir -p logs

# Create service-account directory for mounted secrets
RUN mkdir -p /service-account

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV GOOGLE_APPLICATION_CREDENTIALS=/service-account/dbt-mark_cia-data-6be89f0747c2.json

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash appuser && \
    chown -R appuser:appuser /app && \
    chown -R appuser:appuser /service-account
USER appuser

# Default command (can be overridden)
CMD ["python", "orchestrator.py"]