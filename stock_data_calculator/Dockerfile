FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/stock_data_calculator/requirements.txt
RUN pip install --no-cache-dir -r /app/stock_data_calculator/requirements.txt

COPY . /app/stock_data_calculator

EXPOSE 8000

CMD ["uvicorn", "stock_data_calculator.main:app", "--host", "0.0.0.0", "--port", "8000"]
