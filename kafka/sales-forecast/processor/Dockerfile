FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sales_processor.py .

CMD ["python", "-u", "sales_processor.py"]
