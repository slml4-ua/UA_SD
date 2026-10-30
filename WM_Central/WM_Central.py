"""WM_Central - módulo central de WaterManagement.

Uso:
    python WM_Central.py <puerto_sockets> <ip_broker> <puerto_broker>

Servidor de sockets (registro/autenticación y avisos de avería de los
WM_WS_M) sobre una base de datos SQLite.
"""

import collections
import contextlib
import socket
import sys
import threading
import time
import uuid

import bus
import db
import protocol
from panel import (
    GRIS, NARANJA, NEGRITA, ROJO, VERDE, VERDE_CLARO, Pantalla, ancho_util, linea,
)

TOPIC_PETICIONES = "fo_requests"
TOPIC_TELEMETRIA = "ws_telemetry"
TOPIC_ADMIN = "central_admin"
TOPIC_COMANDOS = "ws_commands"
TOPIC_EVENTOS = "central_events"
TOPIC_LATIDO = "central_heartbeat"
GRUPO_KAFKA = "wm-central"

CADUCIDAD_PETICION = 60  # s: una petición más antigua se deniega
DURACION_MAXIMA = 3600
DURACION_POR_DEFECTO = 20

MOTIVOS_FIN = {
    "TIEMPO": "tiempo agotado",
    "MENU": "finalizado desde la estación",
    "CENTRAL": "finalizado por Central",
    "FUGA": "fuga detectada",
    "MONITOR": "monitor de la estación caído",
    "ENGINE_CAIDO": "la estación se reinició durante el riego",
    "NO_INICIADO": "la estación no pudo iniciar el riego",
}

NOMBRES_ESTADO = {
    db.DISPONIBLE: "disponible",
    db.REGANDO: "regando",
    db.FUGA: "averiada (fuga)",
    db.FUERA_DE_SERVICIO: "fuera de servicio",
    db.DESCONECTADA: "desconectada",
}
LIMITE_SILENCIO_MONITOR = 10  # s sin mensajes de un Monitor => DESCONECTADA
LINEAS_EVENTOS = 5

class Estacion:
    """Estado en memoria de una estación de riego."""

    def __init__(self, ws_id, ubicacion, bloqueada=False):
        self.id = ws_id
        self.ubicacion = ubicacion
        self.conectada = False  # hay un Monitor registrado y vivo
        self.averia = False  # el Monitor ha comunicado una avería
        self.bloqueada = bloqueada  # fuera de servicio por orden de Central
        self.riego = None  # riego en curso: dict con req_id, operador, caudal, volumen

    @property
    def estado(self):
        if not self.conectada:
            return db.DESCONECTADA
        if self.averia:
            return db.FUGA
        if self.bloqueada:
            return db.FUERA_DE_SERVICIO
        if self.riego is not None:
            return db.REGANDO
        return db.DISPONIBLE


lock = threading.RLock()  # protege `estaciones`
estaciones = {}  # ws_id -> Estacion
conectadas = {}  # ws_id -> conexión del Monitor que la tiene registrada
conectadas_lock = threading.Lock()

productor = None
productor_listo = threading.Event()
salida = collections.deque()  # mensajes pendientes de publicar (hilos que no deben esperar)
salida_cond = threading.Condition()

eventos = collections.deque(maxlen=LINEAS_EVENTOS)
pantalla = Pantalla()


def log(mensaje):
    texto = f"{time.strftime('%H:%M:%S')} {mensaje}"
    eventos.append(texto)
    if not pantalla.interactiva:
        print(texto, flush=True)

# ------------------------------------------------------------------- Kafka

_hilo = threading.local()


@contextlib.contextmanager
def lote():
    """Agrupa las publicaciones de un bloque y las envía al terminar.

    Así no se espera a Kafka con el candado de estado cogido: primero se decide
    y se anota todo en memoria y en SQLite, y después se publica en orden.
    """
    if getattr(_hilo, "buffer", None) is not None:  # ya dentro de otro lote
        yield
        return
    _hilo.buffer = []
    try:
        yield
    finally:
        pendientes, _hilo.buffer = _hilo.buffer, None
        for topic, mensaje, sincrono, confirmar in pendientes:
            _publicar(topic, mensaje, sincrono, confirmar)


def _publicar(topic, mensaje, sincrono=True, confirmar=True):
    """Publica en Kafka.

    sincrono=True: espera la confirmación del broker (reintentando).
    sincrono=False: lo deja en una cola y lo publica otro hilo, para que los
    hilos de sockets y de menú nunca se queden esperando a Kafka.
    """
    if getattr(_hilo, "buffer", None) is not None:
        _hilo.buffer.append((topic, mensaje, sincrono, confirmar))
        return True
    if not sincrono:
        with salida_cond:
            salida.append((topic, mensaje))
            salida_cond.notify()
        return True
    productor_listo.wait()
    return bus.enviar(productor, topic, mensaje, confirmar=confirmar)


def hilo_salida():
    while True:
        with salida_cond:
            while not salida:
                salida_cond.wait()
            topic, mensaje = salida.popleft()
        _publicar(topic, mensaje, sincrono=True)


def a_ws(ws_id, orden, sincrono=True, **datos):
    mensaje = {"cmd": orden, "ws_id": ws_id, "ts": time.time()}
    mensaje.update(datos)
    return _publicar(TOPIC_COMANDOS, mensaje, sincrono)


def a_fo(tipo, operador, sincrono=True, confirmar=True, **datos):
    if not operador or operador == "CENTRAL":
        return True
    mensaje = {"type": tipo, "operator_id": operador, "ts": time.time()}
    mensaje.update(datos)
    return _publicar(TOPIC_EVENTOS, mensaje, sincrono, confirmar)


def hilo_latido():
    """Cada segundo avisa a todos de que Central está viva y de cómo están las WS."""
    while True:
        with lock:
            lista = [
                {"id": e.id, "ubicacion": e.ubicacion, "estado": e.estado}
                for e in sorted(estaciones.values(), key=lambda x: x.id)
            ]
        if productor_listo.is_set():
            bus.enviar(
                productor,
                TOPIC_LATIDO,
                {"type": "HEARTBEAT", "ts": time.time(), "estaciones": lista},
                confirmar=False,
            )
        time.sleep(1)


def arrancar_productor(broker):
    global productor
    # Los topics se crean con una sola partición para que los mensajes de cada
    # topic conserven su orden
    while not bus.asegurar_topics(
        broker,
        [TOPIC_PETICIONES, TOPIC_TELEMETRIA, TOPIC_ADMIN, TOPIC_COMANDOS,
         TOPIC_EVENTOS, TOPIC_LATIDO],
        log,
    ):
        time.sleep(2)
    productor = bus.crear_productor(broker, log)
    productor_listo.set()
    log("Conectada con Kafka")


def sincronizar(est):
    """Refleja en SQLite el estado actual de la estación."""
    db.set_estado(est.id, est.estado)


def cargar_estaciones():
    """Al arrancar: carga las WS conocidas (DESCONECTADAS) y los riegos abiertos."""
    with lock:
        for fila in db.list_ws():
            estaciones[fila["id"]] = Estacion(
                fila["id"], fila["ubicacion"], bool(fila["bloqueada"])
            )
        for r in db.riegos_abiertos():
            est = estaciones.get(r["ws_id"])
            if est is not None:
                est.riego = {
                    "req_id": r["req_id"],
                    "operador": r["operator_id"],
                    "duracion": r["duracion_prevista"],
                    "caudal": 0.0,
                    "volumen": 0.0,
                }
                log(f"Riego pendiente de cierre en {est.id} ({r['operator_id']})")

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
        averia_detectada(ws_id_conexion)
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


def averia_detectada(ws_id):
    with lock:
        est = estaciones[ws_id]
        habia_averia = est.averia
        est.averia = True
        sincronizar(est)
        riego = est.riego
    if not habia_averia:
        log(f"{ws_id} AVERIA detectada por su Monitor")
    if riego is not None:
        # La estación cierra el riego por su cuenta; si el Engine no está
        # operativo, el resultado llegará cuando se recupere.
        a_ws(ws_id, "STOP", sincrono=False, req_id=riego["req_id"])
        a_fo(
            "AVISO", riego["operador"], sincrono=False, ws_id=ws_id,
            req_id=riego["req_id"],
            mensaje=f"Avería en {ws_id}: el riego se interrumpe. "
                    "El resultado se mostrará en cuanto la estación se recupere.",
        )


def monitor_desconectado(ws_id):
    with lock:
        est = estaciones.get(ws_id)
        if est is None:
            return
        est.conectada = False
        sincronizar(est)
        riego = est.riego
    log(f"{ws_id} DESCONECTADA (sin señal de su Monitor)")
    if riego is not None:
        a_ws(ws_id, "STOP", sincrono=False, req_id=riego["req_id"])
        a_fo(
            "AVISO", riego["operador"], sincrono=False, ws_id=ws_id,
            req_id=riego["req_id"],
            mensaje=f"{ws_id} ha perdido la conexión: el riego se interrumpe. "
                    "El resultado se mostrará en cuanto se recupere.",
        )


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

# --------------------------------------------------- peticiones de riego

def motivo_denegacion(operador, ws_id, duracion, ts):
    """Devuelve el motivo por el que NO se puede autorizar, o None si se puede."""
    if not isinstance(duracion, int) or not 1 <= duracion <= DURACION_MAXIMA:
        return f"duración no válida (1-{DURACION_MAXIMA} s)"
    if time.time() - ts > CADUCIDAD_PETICION:
        return "petición caducada"
    if operador != "CENTRAL" and not db.operator_exists(operador):
        return f"operario {operador} no registrado"
    est = estaciones.get(ws_id)
    if est is None:
        return f"la estación {ws_id} no existe"
    if est.estado != db.DISPONIBLE:
        return f"la estación {ws_id} no está disponible ({NOMBRES_ESTADO[est.estado]})"
    return None


def responder(origen, operador, ws_id, req_id, autorizada, texto):
    """Comunica la decisión al operario (y a la estación si la petición era suya)."""
    estado = "AUTORIZADA" if autorizada else "DENEGADA"
    a_fo("RESPONSE", operador, ws_id=ws_id, req_id=req_id, status=estado, mensaje=texto)
    if origen == "WS" and not autorizada:
        a_ws(ws_id, "DENIED", req_id=req_id, motivo=texto)


def procesar_peticion(req_id, operador, ws_id, duracion, origen, ts):
    """Valida y, si procede, autoriza un riego. Es idempotente respecto a req_id.

    Tanto las autorizaciones como las denegaciones quedan registradas en SQLite,
    de modo que una petición repetida recibe siempre la misma respuesta.
    """
    with lote(), lock:
        existente = db.get_riego(req_id)
        if existente is not None:
            log(f"Petición {req_id[:6]} repetida: se reenvía la respuesta")
            motivo_previo = existente["motivo_fin"] or ""
            if motivo_previo.startswith("DENEGADA"):
                responder(origen, operador, ws_id, req_id, False,
                          "Denegada: " + motivo_previo[len("DENEGADA: "):])
                return
            if existente["fin"] is None:
                a_ws(ws_id, "START", req_id=req_id, operator_id=operador,
                     duracion=existente["duracion_prevista"])
            responder(origen, operador, ws_id, req_id, True,
                      f"Riego autorizado en {ws_id}")
            return

        a_fo("INFO", operador, req_id=req_id, ws_id=ws_id,
             mensaje=f"Central ha recibido tu petición para {ws_id}")
        motivo = motivo_denegacion(operador, ws_id, duracion, ts)
        if motivo is not None:
            db.crear_riego(req_id, ws_id, operador,
                           duracion if isinstance(duracion, int) else None)
            db.cerrar_riego(req_id, 0, 0, f"DENEGADA: {motivo}")
            log(f"{operador} -> {ws_id}: DENEGADA ({motivo})")
            responder(origen, operador, ws_id, req_id, False, f"Denegada: {motivo}")
            return

        db.crear_riego(req_id, ws_id, operador, duracion)
        est = estaciones[ws_id]
        est.riego = {
            "req_id": req_id, "operador": operador, "duracion": duracion,
            "caudal": 0.0, "volumen": 0.0,
        }
        sincronizar(est)
        a_fo("INFO", operador, req_id=req_id, ws_id=ws_id,
             mensaje=f"{ws_id} disponible: se ordena abrir la electroválvula")
        a_ws(ws_id, "START", req_id=req_id, operator_id=operador, duracion=duracion)
        responder(origen, operador, ws_id, req_id, True,
                  f"Riego autorizado en {ws_id} durante {duracion} s")
        log(f"{operador} -> {ws_id}: AUTORIZADA ({duracion} s)")


def procesar_telemetria(m):
    ws_id, req_id = m.get("ws_id"), m.get("req_id")
    with lock:
        est = estaciones.get(ws_id)
        if est is None or est.riego is None or est.riego["req_id"] != req_id:
            return
        est.riego["caudal"] = float(m.get("caudal", 0))
        est.riego["volumen"] = float(m.get("volumen", 0))
        operador = est.riego["operador"]
    a_fo("TELEMETRY", operador, confirmar=False, ws_id=ws_id, req_id=req_id,
         caudal=m.get("caudal"), volumen=m.get("volumen"))


def procesar_fin(m):
    """Resultado final de un riego. Se puede recibir más de una vez."""
    ws_id, req_id = m.get("ws_id"), m.get("req_id")
    if not req_id:
        return
    with lote(), lock:
        db.cerrar_riego(
            req_id,
            float(m.get("volumen", 0)),
            float(m.get("duracion", 0)),
            m.get("motivo", "?"),
        )
        fila = db.get_riego(req_id)
        if fila is not None:
            est = estaciones.get(ws_id)
            if est is not None and est.riego is not None and est.riego["req_id"] == req_id:
                est.riego = None
                sincronizar(est)
            motivo = fila["motivo_fin"]
            texto = (
                f"Riego terminado en {ws_id}: {fila['volumen']:.1f} L en "
                f"{fila['duracion_real']:.0f} s ({MOTIVOS_FIN.get(motivo, motivo)})"
            )
            log(texto)
            a_fo("RESULT", fila["operator_id"], ws_id=ws_id, req_id=req_id,
                 volumen=fila["volumen"], duracion=fila["duracion_real"],
                 motivo=motivo, mensaje=texto)
        # La estación guarda el resultado hasta que Central lo confirma
        a_ws(ws_id, "ACK_FINISHED", req_id=req_id)


def procesar_manual(m):
    req_id, ws_id, operador = m.get("req_id"), m.get("ws_id"), m.get("operator_id")
    if not req_id or not ws_id:
        return
    procesar_peticion(req_id, operador, ws_id, m.get("duracion"), "WS", m.get("ts", 0))


def procesar_kafka(m):
    tipo = m.get("type")
    if tipo == "REQUEST":
        if m.get("req_id") and m.get("ws_id"):
            procesar_peticion(m["req_id"], m.get("operator_id"), m["ws_id"],
                              m.get("duracion"), "FO", m.get("ts", 0))
    elif tipo == "MANUAL_REQUEST":
        procesar_manual(m)
    elif tipo == "TELEMETRY":
        procesar_telemetria(m)
    elif tipo == "FINISHED":
        procesar_fin(m)
    elif tipo == "ADMIN":
        orden_central(m.get("cmd"), m.get("ws_id"), m.get("duracion", DURACION_POR_DEFECTO))


def hilo_kafka(broker):
    lector = bus.Lector(
        broker, [TOPIC_PETICIONES, TOPIC_TELEMETRIA, TOPIC_ADMIN], GRUPO_KAFKA,
        desde_el_principio=True, log=log,
    )
    for mensaje in lector.mensajes():
        try:
            procesar_kafka(mensaje)
        except Exception as e:  # un mensaje defectuoso no debe parar a Central
            log(f"Error procesando un mensaje de Kafka: {e!r}")


# ------------------------------------------------- órdenes desde Central

def orden_central(orden, destino, duracion=DURACION_POR_DEFECTO):
    """Inicia, bloquea o activa una estación (o todas con destino ALL)."""
    if orden not in ("START", "BLOCK", "ACTIVATE") or not destino:
        return
    try:
        duracion = int(duracion)
    except (TypeError, ValueError):
        duracion = DURACION_POR_DEFECTO
    with lote(), lock:
        if destino == "ALL":
            objetivos = sorted(estaciones)
        elif destino in estaciones:
            objetivos = [destino]
        else:
            log(f"La estación {destino} no existe")
            return
        for ws_id in objetivos:
            if orden == "START":
                procesar_peticion(uuid.uuid4().hex[:12], "CENTRAL", ws_id, duracion,
                                  "CENTRAL", time.time())
            else:
                est = estaciones[ws_id]
                est.bloqueada = orden == "BLOCK"
                db.set_bloqueada(ws_id, est.bloqueada)
                sincronizar(est)
                a_ws(ws_id, orden, sincrono=False)
                log(f"{ws_id}: {'BLOQUEADA (fuera de servicio)' if est.bloqueada else 'ACTIVADA'}")

# -------------------------------------------------------------------- panel

def fila_estacion(est, volumen, fase, ancho):
    estado = est.estado
    riego = est.riego
    if estado == db.REGANDO and riego is not None:
        volumen += riego["volumen"]
    base = f"{est.id:<5.5}{est.ubicacion:<12.12}{volumen:>6.1f} L "
    if estado == db.DISPONIBLE:
        return linea(base + "DISPONIBLE", ancho, VERDE)
    if estado == db.REGANDO:
        datos = f"{riego['caudal']:.1f}L/m {riego['volumen']:.1f}L {riego['operador']}"
        return linea(base + "REGANDO " + datos, ancho, VERDE if fase else VERDE_CLARO)
    if estado == db.FUGA:
        return linea(base + "FUGA", ancho, ROJO)
    if estado == db.FUERA_DE_SERVICIO:
        return linea(base + "FUERA DE SERVICIO", ancho, NARANJA)
    return linea(base + "DESCONECTADA", ancho, GRIS)


def construir_panel(fase):
    ancho = ancho_util()
    volumenes = db.volumenes_totales()
    with lock:
        filas = [
            fila_estacion(e, volumenes.get(e.id, 0.0), fase, ancho)
            for e in sorted(estaciones.values(), key=lambda x: x.id)
        ]
    if not filas:
        filas = [linea("(sin estaciones registradas)", ancho)]
    lineas = [
        linea("WATERMANAGEMENT - CENTRAL", ancho, NEGRITA),
        linea(f"{'ID':<5}{'UBICACION':<12}{'VOLUMEN':>7}  ESTADO", ancho, NEGRITA),
    ]
    lineas += filas
    lineas.append(linea("Ultimos eventos:", ancho, NEGRITA))
    recientes = list(eventos)
    recientes += [""] * (LINEAS_EVENTOS - len(recientes))
    lineas += [linea(texto, ancho) for texto in recientes]
    lineas.append(linea("1 Iniciar  2 Bloquear  3 Activar  0 Salir", ancho, NEGRITA))
    return lineas


def hilo_panel():
    """Refresca el panel cada segundo (en su sitio si hay terminal)."""
    ultimo_texto = None
    ultima_impresion = 0.0
    while True:
        time.sleep(1)
        fase = int(time.time()) % 2
        lineas = construir_panel(fase)
        if pantalla.interactiva:
            if not pantalla.refrescar(lineas):
                pantalla.imprimir(lineas)
        else:
            # Sin terminal (logs): solo se imprime cuando cambia algo relevante
            texto = "\n".join(construir_panel(1))
            if texto != ultimo_texto or time.time() - ultima_impresion > 30:
                print(texto, flush=True)
                ultimo_texto, ultima_impresion = texto, time.time()


# --------------------------------------------------------------------- menú

def preguntar_destino():
    destino = input("Estación (ID o ALL): ").strip()
    if destino.upper() == "ALL":
        return "ALL"
    return destino


def menu():
    """Bucle del menú de Central (solo con terminal interactiva)."""
    pantalla.imprimir(construir_panel(1))
    threading.Thread(target=hilo_panel, daemon=True).start()
    while True:
        try:
            opcion = input("> ").strip()
        except EOFError:
            while True:
                time.sleep(3600)
        if opcion == "0":
            return
        if opcion not in ("1", "2", "3"):
            pantalla.imprimir(construir_panel(1))
            continue
        pantalla.pausar()
        destino = preguntar_destino()
        if opcion == "1":
            texto = input(f"Duración en segundos [{DURACION_POR_DEFECTO}]: ").strip()
            try:
                duracion = int(texto) if texto else DURACION_POR_DEFECTO
            except ValueError:
                log("Duración no válida")
                pantalla.imprimir(construir_panel(1))
                continue
            orden_central("START", destino, duracion)
        elif opcion == "2":
            orden_central("BLOCK", destino)
        else:
            orden_central("ACTIVATE", destino)
        pantalla.imprimir(construir_panel(1))
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

    for destino, args in (
        (servidor_sockets, (puerto,)),
        (arrancar_productor, (broker,)),
        (hilo_salida, ()),
        (hilo_latido, ()),
        (hilo_kafka, (broker,)),
    ):
        threading.Thread(target=destino, args=args, daemon=True).start()
    try:
        if pantalla.interactiva:
            menu()
        else:
            threading.Thread(target=hilo_panel, daemon=True).start()
            while True:
                time.sleep(3600)
    except KeyboardInterrupt:
        pass
    print("Central detenida")


if __name__ == "__main__":
    main()