# 08 — Engine de la estación (WM_WS_E)

Revisa `WM_WS/WM_WS_E/WM_WS_E.py`. Recibe IP/puerto de Kafka y dirección/puerto del Monitor.

El Engine consume órdenes de `ws_commands`, simula la electroválvula y el caudalímetro, y publica telemetría en `ws_telemetry`. Debe iniciar y detener riegos, acumular volumen, permitir finalizar desde el menú y responder a las comprobaciones de salud del Monitor.

Mantén la identificación de estación, los mensajes existentes y la recuperación de resultados pendientes. No autorices riegos por tu cuenta: la decisión corresponde a Central. Explica cómo probar inicio, parada y avería simulada.
