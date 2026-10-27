"""WM_Central - módulo central de WaterManagement.

Uso:
    python WM_Central.py <puerto_sockets> <ip_broker> <puerto_broker>

Servidor de sockets (registro/autenticación y avisos de avería de los
WM_WS_M) sobre una base de datos SQLite.
"""

import socket
import sys
import threading
import time

import db
import protocol

LIMITE_SILENCIO_MONITOR = 10  # s sin mensajes de un Monitor => DESCONECTADA


class Estacion:
    """Estado en memoria de una estación de riego."""

    def __init__(self, ws_id, ubicacion, bloqueada=False):
        self.id = ws_id
        self.ubicacion = ubicacion
        self.conectada = False  # hay un Monitor registrado y vivo
        self.averia = False  # el Monitor ha comunicado una avería
        self.bloqueada = bloqueada  # fuera de servicio por orden de Central

    @property
    def estado(self):
        if not self.conectada:
            return db.DESCONECTADA
        if self.averia:
            return db.FUGA
        if self.bloqueada:
            return db.FUERA_DE_SERVICIO
        return db.DISPONIBLE


lock = threading.RLock()  # protege `estaciones`
estaciones = {}  # ws_id -> Estacion
conectadas = {}  # ws_id -> conexión del Monitor que la tiene registrada
conectadas_lock = threading.Lock()

_print_lock = threading.Lock()


def log(mensaje):
    with _print_lock:
        print(f"[{time.strftime('%H:%M:%S')}] {mensaje}", flush=True)


def sincronizar(est):
    """Refleja en SQLite el estado actual de la estación."""
    db.set_estado(est.id, est.estado)


def cargar_estaciones():
    """Al arrancar: carga las WS conocidas, todas DESCONECTADAS."""
    with lock:
        for fila in db.list_ws():
            estaciones[fila["id"]] = Estacion(
                fila["id"], fila["ubicacion"], bool(fila["bloqueada"])
            )

# --------------------------------------------------------- sockets (WM_WS_M)

def procesar_mensaje(texto, conn, ws_id_conexion):
    """Interpreta un mensaje de un Monitor y devuelve (respuesta, ws_id).

    ws_id es el id de la WS asociada a esta conexión (None hasta que se
    registre correctamente).
    """
    campos = texto.split(protocol.SEP)
    operacion = campos[0]

    if operacion == "REGISTER" and len(campos) == 3:
        ws_id, ubicacion = campos[1].strip(), campos[2].strip()
        if not ws_id or not ubicacion:
            return "KO#datos vacios", ws_id_conexion
        with conectadas_lock:
            if ws_id in conectadas:
                return "KO#ya conectada", ws_id_conexion
            conectadas[ws_id] = conn
        nueva = db.register_ws(ws_id, ubicacion)
        with lock:
            est = estaciones.get(ws_id)
            if est is None:
                est = estaciones[ws_id] = Estacion(ws_id, ubicacion)
            est.ubicacion = ubicacion
            est.conectada = True
            est.averia = True  # hasta que el Monitor confirme que el Engine está bien
            sincronizar(est)
        log(f"{ws_id} {'dada de alta' if nueva else 'conectada'} en {ubicacion}")
        return f"OK#{ws_id}", ws_id

    # El resto de operaciones exigen haberse registrado antes
    if ws_id_conexion is None:
        return "KO#no registrada", ws_id_conexion

    if len(campos) != 2 or campos[1] != ws_id_conexion:
        return "KO#operacion desconocida", ws_id_conexion

    if operacion == "PING":
        return "OK", ws_id_conexion

    if operacion == "FAULT":
        with lock:
            est = estaciones[ws_id_conexion]
            if not est.averia:
                log(f"{est.id} AVERIA detectada por su Monitor")
            est.averia = True
            sincronizar(est)
        return "OK", ws_id_conexion

    if operacion == "RECOVERED":
        with lock:
            est = estaciones[ws_id_conexion]
            if est.averia:
                log(f"{est.id} avería resuelta")
            est.averia = False
            sincronizar(est)
        return "OK", ws_id_conexion

    return "KO#operacion desconocida", ws_id_conexion


def monitor_desconectado(ws_id):
    with lock:
        est = estaciones.get(ws_id)
        if est is None:
            return
        est.conectada = False
        sincronizar(est)
    log(f"{ws_id} DESCONECTADA (sin señal de su Monitor)")


def atender_cliente(conn, addr):
    """Hilo que atiende a un Monitor durante toda su conexión."""
    ws_id = None
    conn.settimeout(LIMITE_SILENCIO_MONITOR)
    try:
        if not protocol.server_handshake(conn):
            return
        while True:
            texto = protocol.server_recv_request(conn)
            if texto is None:  # EOT o conexión cerrada
                break
            respuesta, ws_id = procesar_mensaje(texto, conn, ws_id)
            protocol.server_send_answer(conn, respuesta)
    except OSError:
        pass
    finally:
        if ws_id is not None:
            with conectadas_lock:
                es_la_actual = conectadas.get(ws_id) is conn
                if es_la_actual:
                    del conectadas[ws_id]
            if es_la_actual:
                monitor_desconectado(ws_id)
        conn.close()

def servidor_sockets(puerto):
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind(("0.0.0.0", puerto))
    servidor.listen()
    log(f"Servidor de sockets escuchando en el puerto {puerto}")
    while True:
        conn, addr = servidor.accept()
        threading.Thread(target=atender_cliente, args=(conn, addr), daemon=True).start()


# ------------------------------------------------------------------- panel

def mostrar_estado():
    with lock:
        filas = sorted(estaciones.values(), key=lambda e: e.id)
        datos = [(e.id, e.ubicacion, e.estado) for e in filas]
    with _print_lock:
        print("-" * 60)
        print(f"{'ID':<8}{'UBICACION':<20}{'ESTADO':<20}")
        for ws_id, ubicacion, estado in datos:
            print(f"{ws_id:<8}{ubicacion:<20}{estado:<20}")
        if not datos:
            print("(sin estaciones registradas)")
        print("-" * 60, flush=True)

# -------------------------------------------------------------------- main

def main():
    if len(sys.argv) != 4:
        print("Uso: python WM_Central.py <puerto_sockets> <ip_broker> <puerto_broker>")
        sys.exit(1)
    try:
        puerto = int(sys.argv[1])
        broker = f"{sys.argv[2]}:{int(sys.argv[3])}"
    except ValueError:
        print("Los puertos deben ser números")
        sys.exit(1)

    db.init()
    cargar_estaciones()
    log(f"Broker Kafka configurado: {broker}")

    threading.Thread(target=servidor_sockets, args=(puerto,), daemon=True).start()

    try:
        while True:
            mostrar_estado()
            time.sleep(5)
    except KeyboardInterrupt:
        log("Central detenida")


if __name__ == "__main__":
    main()