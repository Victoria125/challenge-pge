FROM python:3.12-slim

WORKDIR /app

# instala dependencias primeiro (cache de camada do Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]
