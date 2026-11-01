# 09 — Aplicación del operario (WM_FO)

Revisa `WM_FO/WM_FO.py`. Recibe IP/puerto de Kafka, ID del operario y, opcionalmente, un fichero de solicitudes.

La aplicación debe permitir pedir un riego manual, consultar estaciones y recibir los eventos de Central destinados a ese operario. Para el fichero, procesa las solicitudes secuencialmente y espera cuatro segundos entre servicios, tal como indica el enunciado.

Respeta los topics y mensajes actuales, muestra autorizaciones, denegaciones, telemetría y resultado final, y gestiona la pérdida de conexión con Central. Incluye un ejemplo de fichero y pruebas básicas.
