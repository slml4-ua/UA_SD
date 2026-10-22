"""Base de datos SQLite de WM_Central.
 
Una sola conexión compartida por todos los hilos, protegida con un candado.
La ruta se puede cambiar con la variable de entorno DB_PATH (útil en Railway,
donde la BD debe estar en un volumen persistente).
"""
 
import os
import sqlite3
import threading
import time
 
DB_PATH = os.environ.get("DB_PATH", "central.db")
 
# Estados de una estación de riego
DISPONIBLE = "DISPONIBLE"
REGANDO = "REGANDO"
FUGA = "FUGA"
FUERA_DE_SERVICIO = "FUERA_DE_SERVICIO"
DESCONECTADA = "DESCONECTADA"
 
_lock = threading.Lock()
_conn = None

# Columnas añadidas después de la primera versión de las tablas
_COLUMNAS_NUEVAS = [
    ("ws", "bloqueada", "INTEGER DEFAULT 0"),
    ("irrigation", "req_id", "TEXT"),
    ("irrigation", "duracion_prevista", "INTEGER"),
    ("irrigation", "duracion_real", "REAL"),
] 
 
def init():
    """Abre la BD, crea las tablas y deja todas las WS como DESCONECTADA.
 
    Al arrancar Central no puede conocer el estado real de ninguna estación
    hasta que ésta conecte, así que todas empiezan DESCONECTADA.
    """
    global _conn
    _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    _conn.row_factory = sqlite3.Row
    with _lock:
        _conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS operator (
                id      TEXT PRIMARY KEY,
                nombre  TEXT
            );
            CREATE TABLE IF NOT EXISTS ws (
                id          TEXT PRIMARY KEY,
                ubicacion   TEXT,
                estado      TEXT NOT NULL,
                caudal      REAL DEFAULT 0,
                volumen     REAL DEFAULT 0,
                operator_id TEXT,
                last_seen   REAL
            );
            CREATE TABLE IF NOT EXISTS irrigation (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                ws_id       TEXT NOT NULL,
                operator_id TEXT,
                inicio      REAL,
                fin         REAL,
                volumen     REAL,
                motivo_fin  TEXT
            );
            """
        )
        # Operarios de ejemplo (si la tabla está vacía)
        if _conn.execute("SELECT COUNT(*) FROM operator").fetchone()[0] == 0:
            _conn.executemany(
                "INSERT INTO operator (id, nombre) VALUES (?, ?)",
                [("OP1", "Operario 1"), ("OP2", "Operario 2"), ("OP3", "Operario 3")],
            )
        _conn.execute(
            "UPDATE ws SET estado = ?, caudal = 0, operator_id = NULL", (DESCONECTADA,)
        )

        for tabla, columna, tipo in _COLUMNAS_NUEVAS:
            existentes = [f["name"] for f in _conn.execute(f"PRAGMA table_info({tabla})")]
            if columna not in existentes:
                _conn.execute(f"ALTER TABLE {tabla} ADD COLUMN {columna} {tipo}")
        _conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_irrigation_req ON irrigation(req_id)"
        )
        _conn.commit()
 
 
# ------------------------------------------------------------------ lectura
 
def get_ws(ws_id):
    with _lock:
        return _conn.execute("SELECT * FROM ws WHERE id = ?", (ws_id,)).fetchone()
 
 
def list_ws():
    with _lock:
        return _conn.execute("SELECT * FROM ws ORDER BY id").fetchall()
 
 
def operator_exists(operator_id) -> bool:
    with _lock:
        fila = _conn.execute(
            "SELECT 1 FROM operator WHERE id = ?", (operator_id,)
        ).fetchone()
        return fila is not None
 
 
# ---------------------------------------------------------------- escritura
 
def register_ws(ws_id, ubicacion):
    """Alta de una WS nueva o actualización de la ubicación de una conocida.
 
    La deja DISPONIBLE. Devuelve True si era una WS nueva.
    """
    with _lock:
        existe = _conn.execute("SELECT 1 FROM ws WHERE id = ?", (ws_id,)).fetchone()
        if existe:
            _conn.execute(
                "UPDATE ws SET ubicacion = ?, estado = ?, caudal = 0, "
                "operator_id = NULL, last_seen = ? WHERE id = ?",
                (ubicacion, DISPONIBLE, time.time(), ws_id),
            )
        else:
            _conn.execute(
                "INSERT INTO ws (id, ubicacion, estado, last_seen) VALUES (?, ?, ?, ?)",
                (ws_id, ubicacion, DISPONIBLE, time.time()),
            )
        _conn.commit()
        return not existe
 
 
def set_estado(ws_id, estado):
    with _lock:
        _conn.execute(
            "UPDATE ws SET estado = ?, last_seen = ? WHERE id = ?",
            (estado, time.time(), ws_id),
        )
        _conn.commit()


def operator_exists(operator_id) -> bool:
    with _lock:
        fila = _conn.execute(
            "SELECT 1 FROM operator WHERE id = ?", (operator_id,)
        ).fetchone()
        return fila is not None


def get_riego(req_id):
    with _lock:
        return _conn.execute(
            "SELECT * FROM irrigation WHERE req_id = ?", (req_id,)
        ).fetchone()


def riegos_abiertos():
    """Riegos autorizados que todavía no han terminado."""
    with _lock:
        return _conn.execute(
            "SELECT * FROM irrigation WHERE fin IS NULL AND req_id IS NOT NULL"
        ).fetchall()


def volumenes_totales():
    """Agua total suministrada por cada WS en riegos ya terminados."""
    with _lock:
        filas = _conn.execute(
            "SELECT ws_id, COALESCE(SUM(volumen), 0) AS total "
            "FROM irrigation WHERE fin IS NOT NULL GROUP BY ws_id"
        ).fetchall()
    return {f["ws_id"]: f["total"] for f in filas}


def set_bloqueada(ws_id, bloqueada):
    with _lock:
        _conn.execute(
            "UPDATE ws SET bloqueada = ? WHERE id = ?", (1 if bloqueada else 0, ws_id)
        )
        _conn.commit()


def crear_riego(req_id, ws_id, operator_id, duracion):
    """Registra un riego autorizado. Si el req_id ya existe no hace nada.

    Devuelve (fila, es_nuevo).
    """
    with _lock:
        cursor = _conn.execute(
            "INSERT OR IGNORE INTO irrigation "
            "(req_id, ws_id, operator_id, duracion_prevista, inicio) "
            "VALUES (?, ?, ?, ?, ?)",
            (req_id, ws_id, operator_id, duracion, time.time()),
        )
        _conn.commit()
        fila = _conn.execute(
            "SELECT * FROM irrigation WHERE req_id = ?", (req_id,)
        ).fetchone()
        return fila, cursor.rowcount > 0


def cerrar_riego(req_id, volumen, duracion, motivo):
    """Anota el resultado final de un riego. Devuelve True si estaba abierto."""
    with _lock:
        cursor = _conn.execute(
            "UPDATE irrigation SET fin = ?, volumen = ?, duracion_real = ?, "
            "motivo_fin = ? WHERE req_id = ? AND fin IS NULL",
            (time.time(), volumen, duracion, motivo, req_id),
        )
        _conn.commit()
        return cursor.rowcount > 0