# 07 — Monitor de la estación (WM_WS_M)

Revisa `WM_WS/WM_WS_M/WM_WS_M.py`. El Monitor recibe puerto del Engine, IP/puerto de Central, ID de estación y ubicación.

Debe registrarse en Central mediante sockets, aceptar la conexión TCP del Engine y comprobar su salud aproximadamente cada segundo. Comunica `FAULT` cuando recibe `KO` o hay timeout, y `RECOVERED` cuando vuelve a recibir `OK`.

Gestiona desconexiones y reconexiones sin bloquear la supervisión. Mantén el protocolo compartido y muestra mensajes claros. Incluye pruebas de registro, fallo, recuperación y caída de Central.
