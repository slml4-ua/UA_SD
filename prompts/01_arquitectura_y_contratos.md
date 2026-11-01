# 01 — Arquitectura y contratos

Revisa la arquitectura de WaterManagement y sus cuatro procesos: `WM_Central`, `WM_FO`, `WM_WS_E` y `WM_WS_M`.

Comprueba cómo se comunican mediante Kafka y sockets TCP. Mantén los topics existentes (`fo_requests`, `ws_telemetry`, `central_admin`, `ws_commands`, `central_events` y `central_heartbeat`) y los contratos definidos en los módulos `bus.py` y `protocol.py`.

Dibuja o resume el flujo de una petición de riego, desde el operario hasta la estación y su respuesta final. Señala dependencias o incompatibilidades y propone solo cambios necesarios, compatibles con el código actual.
