FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY orchestrator.py .

# Expose port
EXPOSE 8080

# Run the application
CMD ["python", "orchestrator.py"]
