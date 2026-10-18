"""WM_WS_M - Monitor de una estación de riego (Watering Station).

Uso:
    python WM_WS_M.py <puerto_engine> <ip_central> <puerto_central> <id_ws> <ubicacion>

Se registra en WM_Central por sockets, espera la conexión del WM_WS_E de su
estación y le pregunta su estado de salud cada segundo. Si el Engine responde
KO, deja de responder o se desconecta, avisa a Central de la avería; cuando
vuelve a responder OK, avisa de la recuperación.
"""

import socket
import sys
import threading
import time

import protocol

INTERVALO_SALUD = 1.0
SEGUNDOS_ENTRE_PINGS = 3
TIMEOUT_SOCKET = 2.0
ESPERA_REINTENTO = 2.0

_print_lock = threading.Lock()


def log(mensaje):
    with _print_lock:
        print(f"[{time.strftime('%H:%M:%S')}] {mensaje}", flush=True)


class RegistroRechazado(Exception):
    """Central ha respondido KO al registro."""


class EnlaceCentral:
    """Conexión persistente con WM_Central.

    Si se cae, se reconecta y se vuelve a registrar. Cada registro incrementa
    `epoca`, para que quien lo use sepa que Central ha podido perder el estado.
    """

    def __init__(self, ip, puerto, ws_id, ubicacion):
        self.ip = ip
        self.puerto = puerto
        self.ws_id = ws_id
        self.ubicacion = ubicacion
        self.sock = None
        self.epoca = 0
        self.lock = threading.Lock()

    def _abrir(self):
        sock = socket.create_connection((self.ip, self.puerto), timeout=TIMEOUT_SOCKET)
        try:
            if not protocol.client_handshake(sock):
                raise ConnectionError("Central no acepta el ENQ")
            respuesta = protocol.client_request(
                sock, f"REGISTER#{self.ws_id}#{self.ubicacion}"
            )
            if respuesta is None:
                raise ConnectionError("sin respuesta al registro")
            if respuesta != f"OK#{self.ws_id}":
                raise RegistroRechazado(respuesta)
        except Exception:
            sock.close()
            raise
        self.sock = sock
        self.epoca += 1

    def _cerrar(self):
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    def conectar_inicial(self):
        """Registro inicial: reintenta hasta que Central responda."""
        while True:
            try:
                with self.lock:
                    self._abrir()
                log(f"Registrada en Central como {self.ws_id} ({self.ubicacion})")
                return
            except RegistroRechazado as e:
                log(f"Central rechaza el registro: {e}")
            except OSError as e:
                log(f"No se puede conectar con Central ({e}); reintentando...")
            time.sleep(ESPERA_REINTENTO)

    def enviar(self, texto):
        """Envía un mensaje a Central y devuelve su respuesta, o None si falla.

        Si la conexión está caída, intenta reconectar y registrarse de nuevo.
        """
        with self.lock:
            for _ in range(2):
                try:
                    if self.sock is None:
                        self._abrir()
                        log("Reconectada con Central")
                    respuesta = protocol.client_request(self.sock, texto)
                    if respuesta is not None:
                        return respuesta
                except (OSError, RegistroRechazado):
                    pass
                self._cerrar()
            return None

    def cerrar(self):
        with self.lock:
            if self.sock is not None:
                protocol.client_close(self.sock)
            self._cerrar()


class EnlaceEngine:
    """Servidor de sockets al que se conecta el WM_WS_E de esta estación."""

    def __init__(self, puerto, ws_id):
        self.puerto = puerto
        self.ws_id = ws_id
        self.conn = None
        self.ha_conectado = False
        self.lock = threading.Lock()

    def aceptar_conexiones(self):
        servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        servidor.bind(("0.0.0.0", self.puerto))
        servidor.listen()
        log(f"Esperando al Engine en el puerto {self.puerto}")
        while True:
            conn, addr = servidor.accept()
            conn.settimeout(TIMEOUT_SOCKET)
            try:
                if not protocol.client_handshake(conn):
                    conn.close()
                    continue
            except OSError:
                conn.close()
                continue
            with self.lock:
                if self.conn is not None:
                    self.conn.close()
                self.conn = conn
                self.ha_conectado = True
            log(f"Engine conectado desde {addr[0]}")

    def comprobar_salud(self):
        """Pregunta al Engine por su salud.

        Devuelve True (OK), False (KO, sin respuesta o desconectado) o None si
        el Engine aún no se ha conectado nunca.
        """
        with self.lock:
            conn = self.conn
            ha_conectado = self.ha_conectado
        if conn is None:
            return None if not ha_conectado else False
        try:
            respuesta = protocol.client_request(conn, f"HEALTH#{self.ws_id}")
        except OSError:
            respuesta = None
        if respuesta is None:
            with self.lock:
                if self.conn is conn:
                    self.conn = None
            conn.close()
            return False
        return respuesta == "OK"


def main():
    if len(sys.argv) != 6:
        print(
            "Uso: python WM_WS_M.py <puerto_engine> <ip_central> "
            "<puerto_central> <id_ws> <ubicacion>"
        )
        sys.exit(1)
    try:
        puerto_engine = int(sys.argv[1])
        ip_central = sys.argv[2]
        puerto_central = int(sys.argv[3])
    except ValueError:
        print("Los puertos deben ser números")
        sys.exit(1)
    ws_id = sys.argv[4]
    ubicacion = sys.argv[5]

    central = EnlaceCentral(ip_central, puerto_central, ws_id, ubicacion)
    central.conectar_inicial()

    engine = EnlaceEngine(puerto_engine, ws_id)
    threading.Thread(target=engine.aceptar_conexiones, daemon=True).start()

    averia = False
    epoca_notificada = central.epoca
    ciclos = 0
    try:
        while True:
            inicio = time.time()
            sano = engine.comprobar_salud()
            ciclos += 1
            if ciclos % SEGUNDOS_ENTRE_PINGS == 0:
                # Mantiene viva la conexión y detecta si Central se ha caído
                central.enviar(f"PING#{ws_id}")

            if sano is False and not averia:
                averia = True
                log("AVERIA: el Engine responde KO o no responde")
                central.enviar(f"FAULT#{ws_id}")
                epoca_notificada = central.epoca
            elif sano and averia:
                averia = False
                log("Avería resuelta, el Engine vuelve a responder OK")
                central.enviar(f"RECOVERED#{ws_id}")
                epoca_notificada = central.epoca
            elif averia and central.epoca != epoca_notificada:
                # Central se ha reconectado y no conoce la avería en curso
                central.enviar(f"FAULT#{ws_id}")
                epoca_notificada = central.epoca

            time.sleep(max(0.0, INTERVALO_SALUD - (time.time() - inicio)))
    except KeyboardInterrupt:
        log("Monitor detenido")
    finally:
        central.cerrar()


if __name__ == "__main__":
    main()