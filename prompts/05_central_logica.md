# 05 — Lógica de WM_Central

Revisa `WM_Central/WM_Central.py`, coordinador principal de la red. Mantén sus argumentos: puerto TCP, IP del broker y puerto Kafka.

Comprueba el registro de monitores por sockets, las peticiones de riego por Kafka, la validación de operario/estación/duración y los estados `DISPONIBLE`, `REGANDO`, `FUGA`, `FUERA_DE_SERVICIO` y `DESCONECTADA`.

Central debe autorizar o denegar, enviar comandos, procesar telemetría y resultados, y gestionar bloqueos, activaciones y desconexiones. Mantén SQLite, `req_id` e idempotencia. Corrige solo lo necesario y explica el flujo y las pruebas.
