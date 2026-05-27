# Tujuan
# Image build aplikasi Python Telegram signal trader.
# Caller
# docker-compose service app.
# Dependensi
# python:3.11-slim, requirements.txt.
# Main Functions
# Build runtime container.
# Side Effects
# Instalasi dependensi pip di image.

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-m", "CaraCrypto"]
