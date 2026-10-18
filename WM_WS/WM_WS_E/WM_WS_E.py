"""WM_WS_E - Engine de una estación de riego (caudalímetro y electroválvula).

Uso:
    python WM_WS_E.py <ip_broker> <puerto_broker> <ip_monitor> <puerto_monitor>

Se conecta a su WM_WS_M por sockets (el Monitor le pregunta su salud cada
segundo y le indica el ID de la estación) y por Kafka recibe las órdenes de
Central y publica el caudal y el volumen del riego.

Topics de Kafka (mensajes en JSON):
    ws_commands   Central -> Engine
        {"ws_id": "WS1" | "ALL", "cmd": "START", "operator_id": "OP1", "duracion": 30}
        {"ws_id": "WS1" | "ALL", "cmd": "STOP" | "BLOCK" | "ACTIVATE"}
    ws_telemetry  Engine -> Central
        {"type": "TELEMETRY", "ws_id", "operator_id", "caudal", "volumen"}
        {"type": "FINISHED", "ws_id", "operator_id", "volumen", "duracion", "motivo"}
        {"type": "MANUAL_REQUEST", "ws_id", "duracion"}

Menú local: permite pedir un riego manual, finalizarlo y simular (o quitar)
una fuga pulsando una opción.
"""

import json
import random
import socket
import sys
import threading
import time
import uuid

from kafka import KafkaConsumer, KafkaProducer

import protocol

TOPIC_COMANDOS = "ws_commands"
TOPIC_TELEMETRIA = "ws_telemetry"

DISPONIBLE = "DISPONIBLE"
REGANDO = "REGANDO"
FUGA = "FUGA"
FUERA_DE_SERVICIO = "FUERA_DE_SERVICIO"

DURACION_POR_DEFECTO = 20
ESPERA_REINTENTO = 2.0

_print_lock = threading.Lock()


def log(mensaje):
    with _print_lock:
        print(f"[{time.strftime('%H:%M:%S')}] {mensaje}", flush=True)


def crear_productor(broker):
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except Exception as e:
            log(f"No se puede conectar con Kafka ({e}); reintentando...")
            time.sleep(ESPERA_REINTENTO)


def crear_consumidor(broker):
    # Un grupo distinto en cada ejecución: Kafka no recuerda lo ya leído, así que
    # al reiniciar el Engine no se repiten órdenes antiguas. Con un grupo
    # compartido, además, Kafka repartiría los mensajes entre las estaciones en
    # vez de entregárselos a todas.
    grupo = f"ws-engine-{uuid.uuid4().hex[:8]}"
    while True:
        try:
            return KafkaConsumer(
                TOPIC_COMANDOS,
                bootstrap_servers=broker,
                group_id=grupo,
                auto_offset_reset="latest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
        except Exception as e:
            log(f"No se puede conectar con Kafka ({e}); reintentando...")
            time.sleep(ESPERA_REINTENTO)


class Engine:
    def __init__(self, broker):
        self.broker = broker
        self.lock = threading.RLock()
        self.productor = None
        self.ws_id = None  # lo comunica el Monitor en la primera comprobación

        self.fallo = False  # fuga simulada
        self.bloqueada = False  # fuera de servicio por orden de Central
        self.regando = False
        self.operador = None
        self.caudal = 0.0
        self.volumen = 0.0
        self.inicio = 0.0
        self.duracion = 0

    # ------------------------------------------------------------- estado

    @property
    def estado(self):
        if self.fallo:
            return FUGA
        if self.bloqueada:
            return FUERA_DE_SERVICIO
        if self.regando:
            return REGANDO
        return DISPONIBLE

    def _publicar(self, mensaje):
        if self.productor is not None:
            self.productor.send(TOPIC_TELEMETRIA, mensaje)

    # ------------------------------------------------------------- riego

    def iniciar_riego(self, operador, duracion):
        with self.lock:
            if self.estado != DISPONIBLE:
                log(f"Orden de riego ignorada: la estación está {self.estado}")
                return False
            self.regando = True
            self.operador = operador
            self.volumen = 0.0
            self.caudal = random.uniform(8, 12)
            self.inicio = time.time()
            self.duracion = duracion
            log(f"Electroválvula ABIERTA - riego para {operador} durante {duracion} s")
            return True

    def finalizar_riego(self, motivo):
        with self.lock:
            if not self.regando:
                return False
            duracion = time.time() - self.inicio
            self._publicar(
                {
                    "type": "FINISHED",
                    "ws_id": self.ws_id,
                    "operator_id": self.operador,
                    "volumen": round(self.volumen, 2),
                    "duracion": round(duracion, 1),
                    "motivo": motivo,
                }
            )
            log(
                f"Electroválvula CERRADA ({motivo}) - {self.volumen:.1f} L "
                f"en {duracion:.0f} s"
            )
            self.regando = False
            self.operador = None
            self.caudal = 0.0
            return True

    def tick(self):
        """Se llama cada segundo: mide el caudal y publica la telemetría."""
        with self.lock:
            if not self.regando:
                return
            if time.time() - self.inicio >= self.duracion:
                self.finalizar_riego("TIEMPO")
                return
            self.caudal = random.uniform(8, 12)
            self.volumen += self.caudal / 60
            self._publicar(
                {
                    "type": "TELEMETRY",
                    "ws_id": self.ws_id,
                    "operator_id": self.operador,
                    "caudal": round(self.caudal, 2),
                    "volumen": round(self.volumen, 2),
                }
            )
            log(f"REGANDO  {self.caudal:5.1f} L/min  {self.volumen:7.1f} L")

    def bloquear(self):
        with self.lock:
            self.finalizar_riego("CENTRAL")
            self.bloqueada = True
            log("Estación FUERA DE SERVICIO por orden de Central")

    def activar(self):
        with self.lock:
            self.bloqueada = False
            log("Estación ACTIVADA por orden de Central")

    def alternar_fallo(self):
        """Simula (o quita) una fuga. Si hay riego en curso, lo corta."""
        with self.lock:
            self.fallo = not self.fallo
            if self.fallo:
                self.finalizar_riego("FUGA")
                log("FUGA simulada: el Engine responderá KO al Monitor")
            else:
                log("Fuga resuelta: el Engine vuelve a responder OK")
            return self.fallo

    def solicitar_manual(self, duracion):
        """Petición de riego hecha desde el menú de la propia estación."""
        if self.ws_id is None:
            log("Aún no se conoce el ID de la estación (esperando al Monitor)")
            return False
        self._publicar(
            {"type": "MANUAL_REQUEST", "ws_id": self.ws_id, "duracion": duracion}
        )
        log("Petición de riego manual enviada a Central")
        return True

    # --------------------------------------------------------- Kafka (órdenes)

    def manejar_comando(self, mensaje):
        destino = mensaje.get("ws_id")
        if destino not in (self.ws_id, "ALL"):
            return
        orden = mensaje.get("cmd")
        if orden == "START":
            self.iniciar_riego(
                mensaje.get("operator_id"),
                mensaje.get("duracion", DURACION_POR_DEFECTO),
            )
        elif orden == "STOP":
            self.finalizar_riego("CENTRAL")
        elif orden == "BLOCK":
            self.bloquear()
        elif orden == "ACTIVATE":
            self.activar()

    def hilo_comandos(self):
        consumidor = crear_consumidor(self.broker)
        for mensaje in consumidor:
            self.manejar_comando(mensaje.value)

    def hilo_tick(self):
        while True:
            time.sleep(1)
            self.tick()

    # -------------------------------------------------------- sockets (Monitor)

    def hilo_monitor(self, ip, puerto):
        """Mantiene la conexión con el Monitor y responde a sus comprobaciones."""
        while True:
            try:
                with socket.create_connection((ip, puerto), timeout=5) as sock:
                    if not protocol.server_handshake(sock):
                        continue
                    log("Conectado con el Monitor")
                    while True:
                        texto = protocol.server_recv_request(sock)
                        if texto is None:
                            break
                        campos = texto.split(protocol.SEP)
                        if campos[0] == "HEALTH" and len(campos) == 2:
                            if self.ws_id is None:
                                self.ws_id = campos[1]
                                log(f"Estación asignada: {self.ws_id}")
                            respuesta = "KO" if self.fallo else "OK"
                        else:
                            respuesta = "KO"
                        protocol.server_send_answer(sock, respuesta)
            except OSError:
                pass
            log("Sin conexión con el Monitor; reintentando...")
            time.sleep(ESPERA_REINTENTO)


def arrancar(engine, ip_monitor, puerto_monitor):
    engine.productor = crear_productor(engine.broker)
    threading.Thread(
        target=engine.hilo_monitor, args=(ip_monitor, puerto_monitor), daemon=True
    ).start()
    threading.Thread(target=engine.hilo_comandos, daemon=True).start()
    threading.Thread(target=engine.hilo_tick, daemon=True).start()


def menu(engine):
    while True:
        print(
            "\n1) Solicitar riego manual\n2) Finalizar riego\n"
            "3) Simular / quitar fuga\n4) Ver estado\n0) Salir"
        )
        try:
            opcion = input("> ").strip()
        except EOFError:
            # Sin terminal interactiva (por ejemplo, en un contenedor)
            while True:
                time.sleep(3600)

        if opcion == "1":
            texto = input(f"Duración en segundos [{DURACION_POR_DEFECTO}]: ").strip()
            try:
                duracion = int(texto) if texto else DURACION_POR_DEFECTO
            except ValueError:
                print("Duración no válida")
                continue
            engine.solicitar_manual(duracion)
        elif opcion == "2":
            if not engine.finalizar_riego("MENU"):
                print("No hay ningún riego en curso")
        elif opcion == "3":
            engine.alternar_fallo()
        elif opcion == "4":
            print(f"Estación {engine.ws_id}: {engine.estado}")
        elif opcion == "0":
            break


def main():
    if len(sys.argv) != 5:
        print(
            "Uso: python WM_WS_E.py <ip_broker> <puerto_broker> "
            "<ip_monitor> <puerto_monitor>"
        )
        sys.exit(1)
    try:
        broker = f"{sys.argv[1]}:{int(sys.argv[2])}"
        ip_monitor = sys.argv[3]
        puerto_monitor = int(sys.argv[4])
    except ValueError:
        print("Los puertos deben ser números")
        sys.exit(1)

    engine = Engine(broker)
    arrancar(engine, ip_monitor, puerto_monitor)
    try:
        menu(engine)
    except KeyboardInterrupt:
        pass
    if engine.productor is not None:
        engine.productor.flush()
    log("Engine detenido")


if __name__ == "__main__":
    main()