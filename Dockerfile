FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Убиваем любые старые процессы перед запуском
CMD pkill -9 -f main.py || true; python3 main.py
