FROM python:3.12-slim

WORKDIR /app

COPY kafka/sales-forecast/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-u", "kafka/sales-forecast/processor/sales_processor.py"]
