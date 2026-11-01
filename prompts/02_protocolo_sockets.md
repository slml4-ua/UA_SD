# 02 — Protocolo de sockets

Revisa los módulos `protocol.py` de Central, Monitor y Engine. Mantén el protocolo TCP basado en `STX`, `ETX`, `LRC` y el intercambio `ENQ/ACK/NACK/EOT`.

Comprueba el cálculo XOR del LRC, la lectura de tramas completas, el handshake, las respuestas y el cierre de conexión. Asegura que los tres procesos interpretan las tramas del mismo modo y gestionan desconexiones y mensajes inválidos.

Corrige solo lo necesario y añade ejemplos o pruebas breves de envío, recepción y detección de errores.
