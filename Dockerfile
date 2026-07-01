# 1. Imagen base idéntica y ligera
FROM python:3.11-slim

# 2. Directorio de trabajo
WORKDIR /app

# 3. Instalar directamente las librerías necesarias de forma limpia
RUN pip install --no-cache-dir paho-mqtt requests

# 4. Copiar el script del puente de comunicación
COPY mqtt_bridge.py .

# 5. Ejecutar el daemon continuamente
CMD ["python", "mqtt_bridge.py"]
