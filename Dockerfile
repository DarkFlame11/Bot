FROM python:1
WORKDIR /app
RUN pip install --no-cache-dir -r requirements.txt
VOLUME /app
COPY . .
CMD ["python", "main.py"]
