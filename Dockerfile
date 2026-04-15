FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install -U aiogram
RUN apt-get update && apt-get install -y procps

COPY main.py .

# Убиваем любые старые процессы перед запуском
CMD pkill -9 -f main.py || true; python main.py
