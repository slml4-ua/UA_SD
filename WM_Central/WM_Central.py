
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
 
# ws_id -> conexión del Monitor que la tiene registrada
conectadas = {}
conectadas_lock = threading.Lock()
 
_print_lock = threading.Lock()
 
 
def log(mensaje):
    with _print_lock:
        print(f"[{time.strftime('%H:%M:%S')}] {mensaje}", flush=True)
 
 
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
        log(f"WS {ws_id} {'dada de alta' if nueva else 'reconectada'} en {ubicacion}")
        return f"OK#{ws_id}", ws_id
 
    # El resto de operaciones exigen haberse registrado antes
    if ws_id_conexion is None:
        return "KO#no registrada", ws_id_conexion
 
    if operacion == "FAULT" and len(campos) == 2 and campos[1] == ws_id_conexion:
        db.set_estado(ws_id_conexion, db.FUGA)
        log(f"WS {ws_id_conexion} -> FUGA detectada")
        return "OK", ws_id_conexion
 
    if operacion == "RECOVERED" and len(campos) == 2 and campos[1] == ws_id_conexion:
        db.set_estado(ws_id_conexion, db.DISPONIBLE)
        log(f"WS {ws_id_conexion} -> averia resuelta, DISPONIBLE")
        return "OK", ws_id_conexion
 
    return "KO#operacion desconocida", ws_id_conexion
 
 
def atender_cliente(conn, addr):
    """Hilo que atiende a un Monitor durante toda su conexión."""
    ws_id = None
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
                if conectadas.get(ws_id) is conn:
                    del conectadas[ws_id]
                    db.set_estado(ws_id, db.DESCONECTADA)
                    log(f"WS {ws_id} -> DESCONECTADA")
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
    filas = db.list_ws()
    with _print_lock:
        print("-" * 60)
        print(f"{'ID':<8}{'UBICACION':<20}{'ESTADO':<20}")
        for f in filas:
            print(f"{f['id']:<8}{f['ubicacion']:<20}{f['estado']:<20}")
        if not filas:
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
 
