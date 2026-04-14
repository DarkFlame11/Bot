FROM python:3.11-slim
WORKDIR /app
RUN pip install --upgrade pip
VOLUME /app
ENV PORT=8080
COPY . .
CMD ["python", "main.py"]
