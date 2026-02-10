FROM python:3.11-slim

WORKDIR /app

# Copiar requirements primero para cache de Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY src/ ./src/

# Variables de entorno por defecto
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

# Puerto de la API
EXPOSE 9777

# Comando de inicio
CMD ["python", "src/api.py"]

