# Prompts — WaterManagement

Prompts breves de referencia, adaptados a los módulos y tecnologías del repositorio. Se han reconstruido a partir del enunciado y del código; no son un historial literal de conversaciones con IA.

## Orden sugerido

`00` contexto · `01` arquitectura · `02` sockets · `03` Kafka · `04` SQLite · `05` Central · `06` panel · `07` Monitor · `08` Engine · `09` FO · `10` Docker · `11` pruebas · `12` depuración.

## Relación con el código

| Parte | Archivos principales | Prompts |
|---|---|---|
| Central | `WM_Central/WM_Central.py`, `db.py`, `panel.py`, `bus.py`, `protocol.py` | 02–06, 11–12 |
| Operarios | `WM_FO/WM_FO.py`, `bus.py` | 01, 03, 09, 11–12 |
| Engine | `WM_WS/WM_WS_E/WM_WS_E.py`, `bus.py`, `protocol.py` | 01–03, 08, 11–12 |
| Monitor | `WM_WS/WM_WS_M/WM_WS_M.py`, `protocol.py` | 01–02, 07, 11–12 |
| Despliegue | `Dockerfile`, `requirements.txt` de cada módulo | 00, 10–11 |

Usa primero el contexto general y después el prompt del componente. Antes de pedir cambios, facilita los archivos actuales. Mantén Python 3.12, Kafka (`kafka-python`), SQLite, sockets TCP y Docker.
