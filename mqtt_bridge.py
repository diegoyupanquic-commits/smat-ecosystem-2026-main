import os
import paho.mqtt.client as mqtt
import requests
import json
import sys
import time  # Necesario para controlar el umbral de tiempo (60 segundos)

# CONFIGURACIÓN DEL ENTORNO SMAT
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "fisi/smat/estaciones/+/lecturas"  # El '+' es un wildcard para el ID de la estación
API_URL = os.environ.get("API_URL", "http://backend:8000/lecturas/")
# Token JWT generado previamente desde Swagger o la App móvil
JWT_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."

# ----------------------------------------------------------------------
# RETO SEMANA 11: MEMORIA CACHÉ LOCAL (Deadband Filter)
# Estructura de la caché: { estacion_id: {"value": ultimo_valor, "timestamp": tiempo_guardado} }
# ----------------------------------------------------------------------
cache_local = {}


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("🟢 Conectado exitosamente al Broker MQTT")
        # Suscribirse al tópico global de lecturas de estaciones
        client.subscribe(MQTT_TOPIC)
        print(f"📡 Escuchando transmisiones en el tópico: {MQTT_TOPIC}")
    else:
        print(f"🔴 Error de conexión al Broker. Código de retorno: {rc}")
        sys.exit(1)


def on_message(client, userdata, msg):
    try:
        # 1. Decodificar el payload binario de MQTT a JSON string
        payload_raw = msg.payload.decode("utf-8")
        data_json = json.loads(payload_raw)

        # 2. Extraer el ID dinámico de la estación desde la estructura del tópico
        topic_parts = msg.topic.split('/')
        estacion_id = int(topic_parts[3])

        nuevo_valor = float(data_json["valor"])
        tiempo_actual = time.time()

        print(f"\n📩 Telemetría recibida de Estación [{estacion_id}]: {data_json}")

        # ------------------------------------------------------------------
        # LÓGICA DEL FILTRO POR UMBRAL DE CAMBIO
        # ------------------------------------------------------------------
        should_send = False
        razon_envio = ""

        if estacion_id not in cache_local:
            # Si es la primera vez que la estación reporta, se envía sí o sí
            should_send = True
            razon_envio = "Primera lectura registrada de esta estación."
        else:
            ultimo_registro = cache_local[estacion_id]
            ultimo_valor = ultimo_registro["value"]
            ultimo_tiempo = ultimo_registro["timestamp"]

            # Calcular la variación porcentual absoluta respecto al último valor guardado
            if ultimo_valor == 0:
                variacion = 1.0 if nuevo_valor != 0 else 0.0
            else:
                variacion = abs(nuevo_valor - ultimo_valor) / ultimo_valor

            tiempo_transcurrido = tiempo_actual - ultimo_tiempo

            # Condición 1: Variación mayor al ±5%
            if variacion > 0.05:
                should_send = True
                razon_envio = f"Variación significativa detectada: {variacion * 100:.2f}% (excede el ±5%)."
            # Condición 2: Reporte mínimo de vida (más de 60 segundos transcurridos)
            elif tiempo_transcurrido > 60:
                should_send = True
                razon_envio = f"Reporte de vida: Pasaron {tiempo_transcurrido:.1f} segundos desde la última inserción."
            else:
                # Si no cumple ninguna, el Bridge bloquea la petición HTTP redundante
                print(f"🛑 [Filtro Activo] Dato redundante bloqueado. (Var: {variacion*100:.2f}%, Tiempo: {tiempo_transcurrido:.1f}s)")

        # ------------------------------------------------------------------
        # INGESTIÓN DE DATOS EN FASTAPI (Solo si superó el filtro)
        # ------------------------------------------------------------------
        if should_send:
            print(f"⚡ [Filtro Permitido] {razon_envio}")

            # Formatear la carga útil para cumplir con el esquema de FastAPI
            api_payload = {
                "valor": nuevo_valor,
                "estacion_id": estacion_id
            }

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {JWT_TOKEN}"
            }

            # Ingestión segura mediante HTTP POST
            response = requests.post(API_URL, json=api_payload, headers=headers)

            if response.status_code in [200, 201]:
                print(f"💾 [DB Sincronizada] Lectura de {nuevo_valor} cm guardada en SQLite.")
                # Actualizar la caché local únicamente tras una inserción exitosa
                cache_local[estacion_id] = {
                    "value": nuevo_valor,
                    "timestamp": tiempo_actual
                }
            else:
                print(f"⚠️ [Fallo de Ingesta] API rechazó el dato. Código: {response.status_code} - {response.text}")

    except KeyError as e:
        print(f"❌ Error de esquema: Falta la llave {e} en el payload MQTT.")
    except ValueError:
        print("❌ Error de casteo: El valor o el ID de la estación no son numéricos.")
    except Exception as e:
        print(f"❌ Error crítico en el Bridge: {e}")


# Inicialización del cliente de red MQTT
bridge_client = mqtt.Client()
bridge_client.on_connect = on_connect
bridge_client.on_message = on_message

try:
    print("🚀 Inicializando el Bridge de Acoplamiento SMAT con Filtro Deadband...")
    bridge_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    # Mantener el hilo escuchando activamente de forma síncrona
    bridge_client.loop_forever()
except KeyboardInterrupt:
    print("\n🛑 Bridge detenido por el administrador.")
