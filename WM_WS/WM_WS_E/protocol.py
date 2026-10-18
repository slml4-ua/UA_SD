"""Protocolo de sockets <STX><DATA><ETX><LRC> con ENQ/ACK/NACK/EOT.
 
Flujo (cliente -> servidor):
    ENQ                      -> ACK/NACK
    STX DATA ETX LRC         -> ACK/NACK
                             <- STX DATA ETX LRC  (respuesta)
    ACK/NACK                 ->
    ... (más peticiones) ...
    EOT                      -> cierra la conexión
 
El LRC es el XOR, byte a byte, de DATA.
"""
 
STX = b"\x02"
ETX = b"\x03"
ENQ = b"\x05"
ACK = b"\x06"
NACK = b"\x15"
EOT = b"\x04"
 
SEP = "#"
 
 
def lrc(data: bytes) -> bytes:
    """XOR de todos los bytes de data, devuelto como un único byte."""
    resultado = 0
    for b in data:
        resultado ^= b
    return bytes([resultado])
 
 
def build_frame(texto: str) -> bytes:
    data = texto.encode("utf-8")
    return STX + data + ETX + lrc(data)
 
 
def _recv_exact(sock, n: int):
    """Lee exactamente n bytes. Devuelve None si la conexión se cierra antes."""
    buf = b""
    while len(buf) < n:
        trozo = sock.recv(n - len(buf))
        if not trozo:
            return None
        buf += trozo
    return buf
 
 
def _recv_frame_body(sock):
    """Tras leer STX: lee DATA hasta ETX y luego el LRC.
 
    Devuelve (texto, lrc_ok), o None si la conexión se cierra.
    """
    data = b""
    while True:
        b = _recv_exact(sock, 1)
        if b is None:
            return None
        if b == ETX:
            break
        data += b
    recibido = _recv_exact(sock, 1)
    if recibido is None:
        return None
    return data.decode("utf-8", errors="replace"), recibido == lrc(data)
 
 
# ---------------------------------------------------------------- servidor
 
def server_handshake(conn) -> bool:
    """Espera ENQ y responde ACK. False si no llega ENQ."""
    b = _recv_exact(conn, 1)
    if b != ENQ:
        return False
    conn.sendall(ACK)
    return True
 
 
def server_recv_request(conn):
    """Espera una petición válida.
 
    Devuelve el texto de DATA, o None si el cliente envía EOT o se cierra.
    Si el LRC no cuadra responde NACK y sigue esperando (el cliente reenvía).
    """
    while True:
        b = _recv_exact(conn, 1)
        if b is None or b == EOT:
            return None
        if b != STX:
            continue
        resultado = _recv_frame_body(conn)
        if resultado is None:
            return None
        texto, ok = resultado
        if ok:
            conn.sendall(ACK)
            return texto
        conn.sendall(NACK)
 
 
def server_send_answer(conn, texto: str) -> bool:
    """Envía la respuesta y espera ACK. True si el cliente la acepta."""
    conn.sendall(build_frame(texto))
    return _recv_exact(conn, 1) == ACK
 
 
# ----------------------------------------------------------------- cliente
 
def client_handshake(sock) -> bool:
    """Envía ENQ y espera ACK."""
    sock.sendall(ENQ)
    return _recv_exact(sock, 1) == ACK
 
 
def client_request(sock, texto: str):
    """Envía una petición y devuelve la respuesta (texto), o None si falla."""
    sock.sendall(build_frame(texto))
    if _recv_exact(sock, 1) != ACK:
        return None
    if _recv_exact(sock, 1) != STX:
        return None
    resultado = _recv_frame_body(sock)
    if resultado is None:
        return None
    respuesta, ok = resultado
    sock.sendall(ACK if ok else NACK)
    return respuesta if ok else None
 
 
def client_close(sock):
    """Envía EOT para cerrar la conversación."""
    try:
        sock.sendall(EOT)
    except OSError:
        pass