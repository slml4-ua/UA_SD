"""Acceso a Kafka con entrega fiable.

Garantías que ofrece este módulo:
  - Un mensaje se da por enviado solo cuando el broker confirma que lo ha
    guardado (acks="all"); si no se confirma, se reintenta.
  - Los lectores usan un grupo fijo y confirman (commit) cada lote solo después
    de haberlo procesado. Si el proceso se cae a medias, al volver recibe otra
    vez lo que no llegó a confirmar, y también lo publicado mientras estuvo
    caído. Por eso un mensaje puede llegar más de una vez, y quien lo procesa
    debe ser capaz de reconocer los repetidos (por su req_id).
"""

import json
import threading
import time

from kafka import KafkaConsumer, KafkaProducer

ESPERA_REINTENTO = 2.0
TIMEOUT_CONFIRMACION = 10.0


def codificar(mensaje):
    return json.dumps(mensaje).encode("utf-8")


def decodificar(datos):
    """Devuelve el diccionario del mensaje, o None si no es un JSON válido."""
    try:
        mensaje = json.loads(datos.decode("utf-8"))
    except (ValueError, UnicodeDecodeError, AttributeError):
        return None
    return mensaje if isinstance(mensaje, dict) else None


def crear_productor(broker, log=print):
    """Crea el productor; si Kafka no está disponible, reintenta hasta lograrlo."""
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=broker,
                acks="all",
                retries=5,
                max_block_ms=5000,
            )
        except Exception as e:
            log(f"No se puede conectar con Kafka ({e}); reintentando...")
            time.sleep(ESPERA_REINTENTO)


def asegurar_topics(broker, topics, log=print):
    """Crea los topics que falten (1 partición), sin fallar si ya existen.

    Devuelve True si los topics quedan disponibles y False si no se ha podido
    hablar con Kafka.

    Con la creación automática de topics del broker no haría falta, pero así
    el sistema funciona igual aunque esa opción esté desactivada y los lectores
    tienen desde el primer momento las particiones que van a leer.
    """
    admin = None
    try:
        from kafka.admin import KafkaAdminClient, NewTopic

        admin = KafkaAdminClient(bootstrap_servers=broker)
        faltan = [t for t in topics if t not in set(admin.list_topics())]
        if faltan:
            admin.create_topics(
                [NewTopic(name=t, num_partitions=1, replication_factor=1) for t in faltan]
            )
        return True
    except Exception as e:
        # Si ya existen (o los crea otro módulo a la vez) no pasa nada
        if "exist" in str(e).lower():
            return True
        log(f"Aviso al preparar los topics de Kafka: {e}")
        return False
    finally:
        if admin is not None:
            try:
                admin.close()
            except Exception:
                pass


def enviar(productor, topic, mensaje, espera=None, confirmar=True):
    """Publica un mensaje JSON en un topic.

    Con confirmar=True espera la confirmación del broker y reintenta hasta
    conseguirla (o hasta `espera` segundos, si se indica). Devuelve True si el
    mensaje quedó confirmado. Con confirmar=False no espera: es para datos que
    se pueden perder sin consecuencias, como la telemetría de cada segundo.
    """
    datos = codificar(mensaje)
    limite = None if espera is None else time.time() + espera
    while True:
        try:
            futuro = productor.send(topic, datos)
            if not confirmar:
                return True
            futuro.get(timeout=TIMEOUT_CONFIRMACION)
            return True
        except Exception:
            if not confirmar:
                return False
            if limite is not None and time.time() >= limite:
                return False
            time.sleep(0.5)


class Lector:
    """Consumidor de uno o varios topics, con grupo fijo y commit manual.

    desde_el_principio=True: si el grupo no ha leído nunca, empieza por el
    primer mensaje del topic. Con False empieza por lo que se publique a partir
    de ahora. En ambos casos, si el grupo ya había leído, continúa donde lo dejó.

    `listo` se activa cuando el lector ya tiene sus particiones asignadas: a
    partir de ese momento no se pierde nada de lo que se publique.
    """

    def __init__(self, broker, topics, grupo, desde_el_principio=False, log=print):
        self.broker = broker
        self.topics = list(topics)
        self.grupo = grupo
        self.inicio = "earliest" if desde_el_principio else "latest"
        self.log = log
        self.listo = threading.Event()
        self._consumidor = None
        self._pendientes = []

    def _conectar(self):
        self.listo.clear()
        while True:
            consumidor = None
            try:
                asegurar_topics(self.broker, self.topics, self.log)
                consumidor = KafkaConsumer(
                    bootstrap_servers=self.broker,
                    group_id=self.grupo,
                    enable_auto_commit=False,
                    auto_offset_reset=self.inicio,
                    session_timeout_ms=6000,
                    heartbeat_interval_ms=2000,
                )
                # Conoce los topics antes de suscribirse, para que el grupo
                # reparta las particiones con la información completa
                consumidor.topics()
                consumidor.subscribe(self.topics)
                limite = time.time() + 30
                while not consumidor.assignment() and time.time() < limite:
                    for registros in consumidor.poll(timeout_ms=500).values():
                        self._pendientes.extend(registros)
                if not consumidor.assignment():
                    raise RuntimeError("Kafka no ha asignado particiones")
                for particion in consumidor.assignment():
                    consumidor.position(particion)
                if not self._pendientes:
                    # Fija el punto de partida del grupo para que, si el módulo
                    # se cae ahora, al volver reciba lo publicado entre medias
                    consumidor.commit()
                self._consumidor = consumidor
                self.listo.set()
                return
            except Exception as e:
                self.log(f"No se puede leer de Kafka ({e}); reintentando...")
                if consumidor is not None:
                    try:
                        consumidor.close()
                    except Exception:
                        pass
                time.sleep(ESPERA_REINTENTO)

    def mensajes(self):
        """Generador de mensajes (diccionarios). Confirma cada lote al terminarlo."""
        if self._consumidor is None:
            self._conectar()
        while True:
            if self._pendientes:
                lote, self._pendientes = self._pendientes, []
            else:
                try:
                    respuesta = self._consumidor.poll(timeout_ms=1000)
                except Exception as e:
                    self.log(f"Error leyendo de Kafka ({e}); reconectando...")
                    self.cerrar()
                    self._conectar()
                    continue
                lote = [r for registros in respuesta.values() for r in registros]
            for registro in lote:
                mensaje = decodificar(registro.value)
                if mensaje is not None:
                    yield mensaje
            if lote:
                try:
                    self._consumidor.commit()
                except Exception as e:
                    self.log(f"No se pudo confirmar la lectura ({e})")

    def cerrar(self):
        if self._consumidor is not None:
            try:
                self._consumidor.close()
            except Exception:
                pass
            self._consumidor = None
            self.listo.clear()