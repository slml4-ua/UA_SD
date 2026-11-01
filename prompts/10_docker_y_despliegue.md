# 10 — Docker y despliegue

Revisa los Dockerfiles y `requirements.txt` de Central, FO, Engine y Monitor. Mantén Python 3.12 y el arranque real de cada módulo.

Comprueba argumentos, variables de entorno y persistencia: Central usa `DB_PATH`, `SOCKET_PORT`, `KAFKA_HOST` y `KAFKA_PORT`; Engine usa `WM_DATA_DIR`. Conserva la topología de la práctica: Central, SQLite y Kafka en cloud; FO y WS en los equipos del laboratorio.

Propón instrucciones reproducibles para construir, configurar y ejecutar cada contenedor. No inventes servicios ni variables: contrástalas con los archivos existentes.
