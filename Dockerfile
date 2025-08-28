FROM python:3.11-slim

WORKDIR /app

# Copy requirements and main.py from the current directory
COPY requirements.txt .
COPY main.py .

RUN pip install --no-cache-dir -r requirements.txt

CMD uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
