# 04 — Base de datos SQLite

Revisa `WM_Central/db.py` y la persistencia de WaterManagement. Mantén SQLite y la ruta configurable mediante `DB_PATH`.

Comprueba las tablas `operator`, `ws` e `irrigation`, sus claves y las funciones de alta, consulta, bloqueo y registro/cierre de riegos. Conserva la migración de columnas y la unicidad de `req_id`.

Asegura que al reiniciar Central las estaciones aparezcan desconectadas hasta registrarse de nuevo y que los riegos queden correctamente recuperables. Evita consultas inseguras y protege el acceso concurrente. Incluye pruebas breves de persistencia.
