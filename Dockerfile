FROM python:3.13-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY tile-scrapper.py .
COPY .env* ./

# Create log directory
RUN mkdir -p /app/logs

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the scraper
ENTRYPOINT ["python", "tile-scrapper.py"]
CMD ["--help"]
