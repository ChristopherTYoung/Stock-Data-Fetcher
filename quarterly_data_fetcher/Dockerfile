FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/quarterly_data_fetcher/requirements.txt
RUN pip install --no-cache-dir -r /app/quarterly_data_fetcher/requirements.txt

COPY . /app/quarterly_data_fetcher

EXPOSE 8000

CMD ["uvicorn", "quarterly_data_fetcher.main:app", "--host", "0.0.0.0", "--port", "8000"]
