"""WM_FO - aplicación del operario de campo (Field Operator).

Uso:
    python WM_FO.py <ip_broker> <puerto_broker> <id_operario> [fichero_peticiones]

Permite pedir un riego en una estación y ver el desarrollo del servicio:
Central valida la petición, la autoriza o la deniega, y el operario recibe la
telemetría de la estación que le está regando y el resumen final. Solo se
reciben los mensajes de los riegos propios, pero se pueden ver todas las
estaciones y su estado.

Fichero de peticiones (opcional): una petición por línea, con el ID de la
estación y la duración en segundos separados por ; , # o espacio. Las líneas
vacías y las que empiezan por # se ignoran. Ejemplo:
    WS1;30
    WS2;15
Se envía la primera, se espera a su conclusión (éxito o fracaso), se esperan
4 segundos y se pasa a la siguiente.

Topics de Kafka (mensajes en JSON):
    fo_requests        FO -> Central   REQUEST
    central_events     Central -> FO   INFO, RESPONSE, TELEMETRY, RESULT, AVISO
    central_heartbeat  Central -> todos  HEARTBEAT (con la lista de estaciones)

Si el FO se cierra durante un riego, el riego continúa; al volver a abrirlo con
el mismo ID de operario se reciben los mensajes que se hubieran perdido, entre
ellos el resultado del servicio.
"""

import re
import sys
import threading
import time
import uuid

import bus

TOPIC_PETICIONES = "fo_requests"
TOPIC_EVENTOS = "central_events"
TOPIC_LATIDO = "central_heartbeat"

DURACION_POR_DEFECTO = 20
ESPERA_ENTRE_PETICIONES = 4
LIMITE_SILENCIO_CENTRAL = 5.0
ESPERA_RESPUESTA = 20  # s esperando que Central responda a una petición
MARGEN_RESULTADO = 60  # s de margen sobre la duración para recibir el resultado

_print_lock = threading.Lock()


def log(mensaje):
    with _print_lock:
        print(f"[{time.strftime('%H:%M:%S')}] {mensaje}", flush=True)


class Operario:
    def __init__(self, broker, operador):
        self.broker = broker
        self.operador = operador
        self.productor = None
        self.lock = threading.Lock()
        self.estaciones = []  # última lista recibida de Central
        self.ultimo_latido = 0.0
        self.central_estaba_ok = None
        self.arranque = time.time()
        self.esperas = {}  # req_id -> {"respuesta": Event, "resultado": Event, "status"}
        self.vistos = set()
        self.lector_eventos = bus.Lector(
            broker, [TOPIC_EVENTOS], f"fo-{operador}", log=log
        )

    @property
    def central_ok(self):
        return time.time() - self.ultimo_latido < LIMITE_SILENCIO_CENTRAL

    # ------------------------------------------------------- mensajes de Central

    def _espera(self, req_id):
        with self.lock:
            return self.esperas.setdefault(
                req_id,
                {"respuesta": threading.Event(), "resultado": threading.Event(),
                 "status": None},
            )

    def hilo_eventos(self):
        for m in self.lector_eventos.mensajes():
            if m.get("operator_id") != self.operador:
                continue  # solo los mensajes de los riegos de este operario
            try:
                self.mostrar(m)
            except Exception as e:
                log(f"Error al procesar un mensaje: {e!r}")

    def mostrar(self, m):
        tipo, req_id = m.get("type"), m.get("req_id")
        clave = (tipo, req_id)
        if tipo in ("RESPONSE", "RESULT"):
            if clave in self.vistos:
                return  # mensaje repetido
            self.vistos.add(clave)
        if tipo == "INFO":
            log(f"  Central: {m.get('mensaje')}")
        elif tipo == "RESPONSE":
            espera = self._espera(req_id)
            espera["status"] = m.get("status")
            espera["respuesta"].set()
            marca = "OK" if m.get("status") == "AUTORIZADA" else "XX"
            log(f"[{marca}] {m.get('status')}: {m.get('mensaje')}")
        elif tipo == "TELEMETRY":
            if time.time() - m.get("ts", 0) < 10:  # no repite telemetría antigua
                log(f"  {m.get('ws_id')}: {m.get('caudal')} L/min | {m.get('volumen')} L")
        elif tipo == "RESULT":
            self._espera(req_id)["resultado"].set()
            log(f"[FIN] {m.get('mensaje')}")
        elif tipo == "AVISO":
            log(f"[!!] {m.get('mensaje')}")

    def hilo_latido(self):
        lector = bus.Lector(
            self.broker, [TOPIC_LATIDO], f"fo-latido-{uuid.uuid4().hex[:8]}", log=log
        )
        for m in lector.mensajes():
            self.ultimo_latido = time.time()
            self.estaciones = m.get("estaciones", [])

    def hilo_vigilancia(self):
        """Avisa cuando se pierde o se recupera la conexión con Central."""
        while True:
            time.sleep(1)
            ok = self.central_ok
            if ok == self.central_estaba_ok:
                continue
            if ok:
                if self.central_estaba_ok is not None:
                    log("Conexión con WM_Central restablecida")
            else:
                if self.central_estaba_ok is None and time.time() - self.arranque < 10:
                    continue
                log("Imposible conectar con WM_Central")
            self.central_estaba_ok = ok

    # ---------------------------------------------------------------- peticiones

    def pedir(self, ws_id, duracion):
        """Envía una petición de riego. Devuelve su req_id, o None si no se envió."""
        if not self.central_ok:
            log("Imposible conectar con WM_Central: no se puede pedir un riego ahora")
            return None
        req_id = uuid.uuid4().hex[:12]
        self._espera(req_id)
        enviada = bus.enviar(
            self.productor,
            TOPIC_PETICIONES,
            {
                "type": "REQUEST",
                "req_id": req_id,
                "operator_id": self.operador,
                "ws_id": ws_id,
                "duracion": duracion,
                "ts": time.time(),
            },
            espera=15,
        )
        if not enviada:
            log("Imposible conectar con Kafka: la petición no se ha enviado")
            return None
        log(f"Petición enviada: {ws_id} durante {duracion} s")
        return req_id

    def pedir_y_esperar(self, ws_id, duracion):
        """Pide un riego y espera a que Central responda."""
        req_id = self.pedir(ws_id, duracion)
        if req_id is None:
            return None
        if not self._espera(req_id)["respuesta"].wait(ESPERA_RESPUESTA):
            log("Central no responde todavía; la petición queda registrada")
        return req_id

    def ejecutar_fichero(self, ruta):
        """Pide uno a uno los riegos de un fichero, esperando a cada conclusión."""
        try:
            with open(ruta, encoding="utf-8") as f:
                lineas = f.read().splitlines()
        except OSError as e:
            log(f"No se puede leer el fichero {ruta}: {e}")
            return
        peticiones = []
        for linea in lineas:
            linea = linea.strip()
            if not linea or linea.startswith("#"):
                continue
            campos = [c for c in re.split(r"[;,#\s]+", linea) if c]
            try:
                duracion = int(campos[1]) if len(campos) > 1 else DURACION_POR_DEFECTO
            except ValueError:
                log(f"Línea ignorada (duración no válida): {linea}")
                continue
            peticiones.append((campos[0], duracion))
        log(f"Fichero {ruta}: {len(peticiones)} peticiones")
        for numero, (ws_id, duracion) in enumerate(peticiones, 1):
            log(f"Petición {numero}/{len(peticiones)} del fichero")
            req_id = None
            while req_id is None:  # si no se puede enviar, se reintenta la misma
                req_id = self.pedir_y_esperar(ws_id, duracion)
                if req_id is None:
                    time.sleep(5)
            espera = self._espera(req_id)
            if espera["status"] == "AUTORIZADA":
                if not espera["resultado"].wait(duracion + MARGEN_RESULTADO):
                    log("El resultado aún no ha llegado; se continúa con la siguiente")
            if numero < len(peticiones):
                log(f"Esperando {ESPERA_ENTRE_PETICIONES} s antes de la siguiente...")
                time.sleep(ESPERA_ENTRE_PETICIONES)
        log("Fichero completado")

    def mostrar_estaciones(self):
        if not self.central_ok:
            print("Imposible conectar con WM_Central: no hay datos de las estaciones")
            return
        if not self.estaciones:
            print("Central no tiene estaciones registradas")
            return
        print(f"{'ID':<6}{'UBICACION':<20}ESTADO")
        for e in self.estaciones:
            print(f"{e['id']:<6}{e['ubicacion']:<20}{e['estado']}")


def menu(operario):
    while True:
        print(
            f"\n--- Operario {operario.operador} ---\n"
            "1) Solicitar riego\n2) Ejecutar peticiones desde fichero\n"
            "3) Ver estaciones\n0) Salir"
        )
        try:
            opcion = input("> ").strip()
        except EOFError:
            while True:  # sin terminal interactiva (p. ej. en un contenedor)
                time.sleep(3600)
        if opcion == "1":
            operario.mostrar_estaciones()
            ws_id = input("Estación (ID): ").strip()
            if not ws_id:
                continue
            texto = input(f"Duración en segundos [{DURACION_POR_DEFECTO}]: ").strip()
            try:
                duracion = int(texto) if texto else DURACION_POR_DEFECTO
            except ValueError:
                print("Duración no válida")
                continue
            operario.pedir_y_esperar(ws_id, duracion)
        elif opcion == "2":
            ruta = input("Ruta del fichero: ").strip()
            if ruta:
                threading.Thread(
                    target=operario.ejecutar_fichero, args=(ruta,), daemon=True
                ).start()
        elif opcion == "3":
            operario.mostrar_estaciones()
        elif opcion == "0":
            break


def main():
    if len(sys.argv) not in (4, 5):
        print(
            "Uso: python WM_FO.py <ip_broker> <puerto_broker> <id_operario> "
            "[fichero_peticiones]"
        )
        sys.exit(1)
    try:
        broker = f"{sys.argv[1]}:{int(sys.argv[2])}"
    except ValueError:
        print("El puerto debe ser un número")
        sys.exit(1)
    operador = sys.argv[3]
    fichero = sys.argv[4] if len(sys.argv) == 5 else None

    operario = Operario(broker, operador)
    log(f"Operario {operador}: conectando con Kafka en {broker}...")
    operario.productor = bus.crear_productor(broker, log)
    threading.Thread(target=operario.hilo_eventos, daemon=True).start()
    threading.Thread(target=operario.hilo_latido, daemon=True).start()
    threading.Thread(target=operario.hilo_vigilancia, daemon=True).start()
    operario.lector_eventos.listo.wait()
    limite = time.time() + 6  # da tiempo a oír el primer latido de Central
    while operario.ultimo_latido == 0 and time.time() < limite:
        time.sleep(0.2)
    if operario.central_ok:
        log("Conectado con WM_Central")
    else:
        log("Imposible conectar con WM_Central")
    log("Listo")
    if fichero:
        threading.Thread(
            target=operario.ejecutar_fichero, args=(fichero,), daemon=True
        ).start()
    try:
        menu(operario)
    except KeyboardInterrupt:
        pass
    operario.lector_eventos.cerrar()
    log("Operario desconectado")


if __name__ == "__main__":
    main()