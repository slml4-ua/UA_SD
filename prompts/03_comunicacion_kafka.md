# 03 — Comunicación Kafka

Revisa los `bus.py` y su uso en Central, FO y Engine. El proyecto utiliza `kafka-python` y mensajes JSON.

Comprueba productores, consumidores, creación de topics, grupos, confirmaciones y reintentos. Respeta los topics actuales: `fo_requests`, `ws_telemetry`, `central_admin`, `ws_commands`, `central_events` y `central_heartbeat`.

Verifica que cada proceso consume y publica los mensajes que le corresponden, que un error no detiene el consumidor y que los mensajes duplicados no provocan operaciones repetidas. Propón cambios compatibles y explica cómo probarlos.
