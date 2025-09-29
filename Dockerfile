FROM python:3.12-slim

# Install system dependencies
WORKDIR /app


# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only necessary application code
COPY . .

EXPOSE 8000

# Use environment variables for Gunicorn configuration
CMD ["sh", "-c", "gunicorn --bind 0.0.0.0:8000 --timeout ${GUNICORN_TIMEOUT:-300} --workers ${GUNICORN_WORKERS:-1} --max-requests 1000 --max-requests-jitter 100 --worker-tmp-dir /tmp app:app"]
