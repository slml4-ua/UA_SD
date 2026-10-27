FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV DB_PATH=/data/central.db
# Parámetros (variables de entorno del servicio): SOCKET_PORT, KAFKA_HOST, KAFKA_PORT
CMD ["sh", "-c", "python -u WM_Central.py ${SOCKET_PORT:-5050} ${KAFKA_HOST} ${KAFKA_PORT:-9092}"]