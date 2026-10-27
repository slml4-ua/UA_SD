"""WM_WS_E - Engine de una estación de riego (caudalímetro y electroválvula).

Uso:
    python WM_WS_E.py <ip_broker> <puerto_broker> <ip_monitor> <puerto_monitor>

Se conecta a su WM_WS_M por sockets (el Monitor le pregunta su salud cada
segundo y le indica el ID de la estación) y por Kafka recibe las órdenes de
Central y publica el caudal, el volumen y el resultado de cada riego.

Topics de Kafka (mensajes en JSON):
    ws_commands        Central -> Engine
        {"cmd": "START", "ws_id", "req_id", "operator_id", "duracion", "ts"}
        {"cmd": "STOP" | "BLOCK" | "ACTIVATE", "ws_id": "WS1" | "ALL", ...}
        {"cmd": "ACK_FINISHED" | "DENIED", "ws_id", "req_id", ...}
    ws_telemetry       Engine -> Central
        {"type": "TELEMETRY", "ws_id", "req_id", "operator_id", "caudal", "volumen"}
        {"type": "FINISHED", "ws_id", "req_id", "operator_id", "volumen",
         "duracion", "motivo"}
        {"type": "MANUAL_REQUEST", "ws_id", "req_id", "operator_id", "duracion", "ts"}
    central_heartbeat  Central -> todos (señal de vida de Central)

El resultado de cada riego se guarda en un fichero local antes de enviarse y se
reenvía hasta que Central confirma que lo ha registrado, de modo que no se
pierde aunque Central, Kafka o el propio Engine se caigan. La carpeta del
fichero se indica con la variable de entorno WM_DATA_DIR (por defecto, la
carpeta actual).

Menú local: permite pedir un riego manual, finalizarlo y simular (o quitar)
una fuga pulsando una opción.
"""

import json
import os
import random
import socket
import sys
import threading
import time
import uuid

import bus
import protocol

TOPIC_COMANDOS = "ws_commands"
TOPIC_TELEMETRIA = "ws_telemetry"
TOPIC_LATIDO = "central_heartbeat"

DISPONIBLE = "DISPONIBLE"
REGANDO = "REGANDO"
FUGA = "FUGA"
FUERA_DE_SERVICIO = "FUERA_DE_SERVICIO"
AVERIADA = "AVERIADA"  # sin comunicación con su Monitor

DURACION_POR_DEFECTO = 20
ESPERA_REINTENTO = 2.0
LIMITE_SILENCIO_MONITOR = 5.0  # s sin preguntas del Monitor => estación averiada
LIMITE_SILENCIO_CENTRAL = 5.0  # s sin latido de Central => Central caída
CADUCIDAD_ORDEN = 15.0  # una orden START más antigua no se ejecuta
SEGUNDOS_ENTRE_REENVIOS = 5.0

_print_lock = threading.Lock()


def log(mensaje):
    with _print_lock:
        print(f"[{time.strftime('%H:%M:%S')}] {mensaje}", flush=True)


class Engine:
    def __init__(self, broker, directorio="."):
        self.broker = broker
        self.directorio = directorio
        self.lock = threading.RLock()
        self.productor = None
        self.ws_id = None  # lo comunica el Monitor en la primera comprobación

        self.fallo = False  # fuga simulada
        self.bloqueada = False  # fuera de servicio por orden de Central
        self.riego = None  # riego en curso (dict)
        self.pendientes = {}  # req_id -> FINISHED que Central aún no ha confirmado
        self.vistos = []  # req_id de las órdenes START ya tratadas
        self.ultimo_envio = {}  # req_id -> instante del último envío del resultado

        self.ultimo_health = 0.0  # última pregunta recibida del Monitor
        self.ultimo_latido = 0.0  # último latido recibido de Central
        self.monitor_estaba_ok = None
        self.central_estaba_ok = None
        self.comandos_listos = threading.Event()
        self.arranque = time.time()

    # ------------------------------------------------------------- estado

    @property
    def monitor_ok(self):
        return (
            self.ws_id is not None
            and time.time() - self.ultimo_health < LIMITE_SILENCIO_MONITOR
        )

    @property
    def central_ok(self):
        return time.time() - self.ultimo_latido < LIMITE_SILENCIO_CENTRAL

    @property
    def regando(self):
        return self.riego is not None

    @property
    def estado(self):
        if not self.monitor_ok:
            return AVERIADA
        if self.fallo:
            return FUGA
        if self.bloqueada:
            return FUERA_DE_SERVICIO
        if self.riego is not None:
            return REGANDO
        return DISPONIBLE

    def motivo_no_disponible(self):
        """Por qué no puede iniciarse un riego ahora (None si puede)."""
        if not self.monitor_ok:
            return "estación AVERIADA: sin comunicación con su Monitor"
        if self.fallo:
            return "estación con FUGA"
        if self.bloqueada:
            return "estación FUERA DE SERVICIO"
        if self.riego is not None:
            return "ya hay un riego en curso"
        return None

    # --------------------------------------------------------- persistencia

    def _ruta(self):
        return os.path.join(self.directorio, f"engine_{self.ws_id}.json")

    def guardar(self):
        """Guarda el estado en disco (se llama con el candado cogido)."""
        if self.ws_id is None:
            return
        datos = {
            "bloqueada": self.bloqueada,
            "riego": self.riego,
            "pendientes": list(self.pendientes.values()),
            "vistos": self.vistos[-100:],
        }
        ruta = self._ruta()
        temporal = ruta + ".tmp"
        try:
            os.makedirs(self.directorio, exist_ok=True)
            with open(temporal, "w", encoding="utf-8") as f:
                json.dump(datos, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temporal, ruta)
        except OSError as e:
            log(f"No se pudo guardar el estado en disco: {e}")

    def cargar(self):
        """Recupera el estado guardado, incluido un riego que quedó a medias."""
        try:
            with open(self._ruta(), encoding="utf-8") as f:
                datos = json.load(f)
        except (OSError, ValueError):
            return
        with self.lock:
            self.bloqueada = bool(datos.get("bloqueada"))
            self.vistos = list(datos.get("vistos", []))
            for mensaje in datos.get("pendientes", []):
                self.pendientes[mensaje["req_id"]] = mensaje
            riego = datos.get("riego")
            if riego:
                # El Engine se cayó durante un riego: se conserva lo ya regado
                mensaje = self._mensaje_fin(riego, "ENGINE_CAIDO", riego.get("ultimo"))
                self.pendientes[riego["req_id"]] = mensaje
                log(
                    f"Riego interrumpido por una caída: se recupera su resultado "
                    f"({mensaje['volumen']:.1f} L)"
                )
            self.guardar()
        if self.bloqueada:
            log("Estación FUERA DE SERVICIO (recuperado del estado guardado)")

    # ------------------------------------------------------------- Kafka

    def _publicar(self, mensaje, confirmar=True, espera=None):
        if self.productor is None:
            return False
        return bus.enviar(
            self.productor, TOPIC_TELEMETRIA, mensaje, espera=espera, confirmar=confirmar
        )

    def _mensaje_fin(self, riego, motivo, instante=None):
        instante = instante or time.time()
        return {
            "type": "FINISHED",
            "ws_id": self.ws_id,
            "req_id": riego["req_id"],
            "operator_id": riego["operador"],
            "volumen": round(riego["volumen"], 2),
            "duracion": round(max(0.0, instante - riego["inicio"]), 1),
            "motivo": motivo,
        }

    def _enviar_resultado(self, mensaje):
        self.ultimo_envio[mensaje["req_id"]] = time.time()
        self._publicar(mensaje, espera=3)

    # ------------------------------------------------------------- riego

    def iniciar_riego(self, req_id, operador, duracion, ts):
        with self.lock:
            if not req_id:
                return
            if req_id in self.vistos or (self.riego and self.riego["req_id"] == req_id):
                return  # orden repetida
            self.vistos.append(req_id)
            motivo = self.motivo_no_disponible()
            if motivo is None and time.time() - ts > CADUCIDAD_ORDEN:
                motivo = "la orden llegó demasiado tarde"
            if motivo is not None:
                log(f"Orden de riego ignorada: {motivo}")
                mensaje = {
                    "type": "FINISHED", "ws_id": self.ws_id, "req_id": req_id,
                    "operator_id": operador, "volumen": 0.0, "duracion": 0.0,
                    "motivo": "NO_INICIADO",
                }
                self.pendientes[req_id] = mensaje
                self.guardar()
            else:
                ahora = time.time()
                self.riego = {
                    "req_id": req_id,
                    "operador": operador,
                    "inicio": ahora,
                    "ultimo": ahora,
                    "duracion": duracion,
                    "volumen": 0.0,
                    "caudal": random.uniform(8, 12),
                }
                self.guardar()
                log(f"Electroválvula ABIERTA - riego para {operador} durante {duracion} s")
                mensaje = None
        if mensaje is None:
            return
        self._enviar_resultado(mensaje)

    def finalizar_riego(self, motivo, req_id=None):
        """Cierra el riego en curso. Devuelve True si había alguno que cerrar."""
        with self.lock:
            riego = self.riego
            if riego is None or (req_id and riego["req_id"] != req_id):
                return False
            mensaje = self._mensaje_fin(riego, motivo)
            self.riego = None
            self.pendientes[riego["req_id"]] = mensaje
            self.guardar()
            log(
                f"Electroválvula CERRADA ({motivo}) - {mensaje['volumen']:.1f} L "
                f"en {mensaje['duracion']:.0f} s"
            )
        self._enviar_resultado(mensaje)
        return True

    def tick(self):
        """Se llama cada segundo: mide el caudal y publica la telemetría."""
        self._vigilar()
        with self.lock:
            riego = self.riego
            if riego is None:
                return
            ahora = time.time()
            if not self.monitor_ok:
                log("Sin comunicación con el Monitor: se cierra el riego")
                self.finalizar_riego("MONITOR")
                return
            if ahora - riego["inicio"] >= riego["duracion"]:
                self.finalizar_riego("TIEMPO")
                return
            riego["caudal"] = random.uniform(8, 12)
            riego["volumen"] += riego["caudal"] / 60
            riego["ultimo"] = ahora
            self.guardar()
            mensaje = {
                "type": "TELEMETRY",
                "ws_id": self.ws_id,
                "req_id": riego["req_id"],
                "operator_id": riego["operador"],
                "caudal": round(riego["caudal"], 2),
                "volumen": round(riego["volumen"], 2),
            }
            log(f"REGANDO  {riego['caudal']:5.1f} L/min  {riego['volumen']:7.1f} L")
        self._publicar(mensaje, confirmar=False)

    def _vigilar(self):
        """Avisa por pantalla cuando cambia la conexión con el Monitor o con Central."""
        monitor, central = self.monitor_ok, self.central_ok
        if self.ws_id is not None and monitor != self.monitor_estaba_ok:
            if monitor:
                if self.monitor_estaba_ok is not None:
                    log("Monitor recuperado: la estación vuelve a estar operativa")
            else:
                log("AVERIADA: sin comunicación con el Monitor. No se admiten riegos")
            self.monitor_estaba_ok = monitor
        if central != self.central_estaba_ok:
            if central:
                if self.central_estaba_ok is not None:
                    log("Conexión con WM_Central restablecida")
            else:
                if self.central_estaba_ok is None and time.time() - self.arranque < 10:
                    return  # recién arrancado: aún no ha dado tiempo a oír a Central
                log("Imposible conectar con WM_Central")
            self.central_estaba_ok = central

    def bloquear(self):
        with self.lock:
            self.finalizar_riego("CENTRAL")
            self.bloqueada = True
            self.guardar()
            log("Estación FUERA DE SERVICIO por orden de Central")

    def activar(self):
        with self.lock:
            self.bloqueada = False
            self.guardar()
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

    def solicitar_manual(self, operador, duracion):
        """Petición de riego hecha desde el menú de la propia estación."""
        if self.ws_id is None:
            log("Aún no se conoce el ID de la estación (esperando al Monitor)")
            return False
        motivo = self.motivo_no_disponible()
        if motivo is not None:
            log(f"No se admite el riego: {motivo}")
            return False
        if not self.central_ok:
            log("Imposible conectar con WM_Central: no se admiten riegos nuevos")
            return False
        enviada = self._publicar(
            {
                "type": "MANUAL_REQUEST",
                "ws_id": self.ws_id,
                "req_id": uuid.uuid4().hex[:12],
                "operator_id": operador,
                "duracion": duracion,
                "ts": time.time(),
            },
            espera=10,
        )
        if enviada:
            log("Petición de riego manual enviada a Central")
        else:
            log("Imposible conectar con Kafka: la petición no se ha enviado")
        return enviada

    # --------------------------------------------------------- Kafka (órdenes)

    def manejar_comando(self, mensaje):
        destino = mensaje.get("ws_id")
        if destino not in (self.ws_id, "ALL"):
            return
        orden = mensaje.get("cmd")
        if orden == "START":
            self.iniciar_riego(
                mensaje.get("req_id"),
                mensaje.get("operator_id"),
                mensaje.get("duracion", DURACION_POR_DEFECTO),
                mensaje.get("ts", 0),
            )
        elif orden == "STOP":
            self.finalizar_riego("CENTRAL", mensaje.get("req_id"))
        elif orden == "BLOCK":
            self.bloquear()
        elif orden == "ACTIVATE":
            self.activar()
        elif orden == "ACK_FINISHED":
            with self.lock:
                if self.pendientes.pop(mensaje.get("req_id"), None) is not None:
                    self.ultimo_envio.pop(mensaje.get("req_id"), None)
                    self.guardar()
        elif orden == "DENIED":
            log(f"Central deniega el riego: {mensaje.get('motivo')}")

    def hilo_comandos(self, lector):
        for mensaje in lector.mensajes():
            try:
                self.manejar_comando(mensaje)
            except Exception as e:
                log(f"Error al procesar una orden: {e!r}")

    def hilo_latido(self):
        lector = bus.Lector(
            self.broker, [TOPIC_LATIDO], f"ws-latido-{uuid.uuid4().hex[:8]}", log=log
        )
        for _ in lector.mensajes():
            self.ultimo_latido = time.time()

    def hilo_tick(self):
        while True:
            time.sleep(1)
            try:
                self.tick()
            except Exception as e:
                log(f"Error en el ciclo de riego: {e!r}")

    def hilo_reenvio(self):
        """Reenvía los resultados que Central todavía no ha confirmado."""
        while True:
            time.sleep(1)
            if not self.central_ok:
                continue
            with self.lock:
                pendientes = [
                    m for m in self.pendientes.values()
                    if time.time() - self.ultimo_envio.get(m["req_id"], 0)
                    >= SEGUNDOS_ENTRE_REENVIOS
                ]
            for mensaje in pendientes:
                self._enviar_resultado(mensaje)

    # -------------------------------------------------------- sockets (Monitor)

    def asignar_id(self, ws_id):
        """Primera comprobación del Monitor: fija el ID y arranca el lector de órdenes."""
        self.ws_id = ws_id
        log(f"Estación asignada: {ws_id}")
        self.cargar()
        lector = bus.Lector(
            self.broker, [TOPIC_COMANDOS], f"ws-engine-{ws_id}", log=log
        )
        self.comandos_listos = lector.listo
        threading.Thread(target=self.hilo_comandos, args=(lector,), daemon=True).start()

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
                            self.ultimo_health = time.time()
                            if self.ws_id is None:
                                self.asignar_id(campos[1])
                            sano = not self.fallo and self.comandos_listos.is_set()
                            respuesta = "OK" if sano else "KO"
                        else:
                            respuesta = "KO"
                        protocol.server_send_answer(sock, respuesta)
            except OSError:
                pass
            log("Sin conexión con el Monitor; reintentando...")
            time.sleep(ESPERA_REINTENTO)


def arrancar(engine, ip_monitor, puerto_monitor):
    engine.productor = bus.crear_productor(engine.broker, log)
    threading.Thread(
        target=engine.hilo_monitor, args=(ip_monitor, puerto_monitor), daemon=True
    ).start()
    threading.Thread(target=engine.hilo_latido, daemon=True).start()
    threading.Thread(target=engine.hilo_tick, daemon=True).start()
    threading.Thread(target=engine.hilo_reenvio, daemon=True).start()


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
            operador = input("ID del operario [OP1]: ").strip() or "OP1"
            texto = input(f"Duración en segundos [{DURACION_POR_DEFECTO}]: ").strip()
            try:
                duracion = int(texto) if texto else DURACION_POR_DEFECTO
            except ValueError:
                print("Duración no válida")
                continue
            engine.solicitar_manual(operador, duracion)
        elif opcion == "2":
            if not engine.finalizar_riego("MENU"):
                print("No hay ningún riego en curso")
        elif opcion == "3":
            engine.alternar_fallo()
        elif opcion == "4":
            print(
                f"Estación {engine.ws_id}: {engine.estado} | "
                f"Monitor: {'OK' if engine.monitor_ok else 'SIN CONEXION'} | "
                f"Central: {'OK' if engine.central_ok else 'Imposible conectar con WM_Central'} | "
                f"Resultados por confirmar: {len(engine.pendientes)}"
            )
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

    engine = Engine(broker, os.environ.get("WM_DATA_DIR", "."))
    arrancar(engine, ip_monitor, puerto_monitor)
    try:
        menu(engine)
    except KeyboardInterrupt:
        pass
    log("Engine detenido")


if __name__ == "__main__":
    main()