# Actualización de Datos Masivos

- La función `insertar_datos_masivos()` permite insertar grandes cantidades de datos en una base de datos.
- Esta funcion recibe el tipo de conexion: **ambiente.data_connect_batch** o **ambiente.data_connect_dms**
- column_names: Es una lista con el nombre de las columnas, estas deben estar ordenadas en el mismo orden que data
- data: Es una lista de tuplas con los datos a insertar, debe tener el mismo largo y ordenadas con el mismo orden de column_names


- Ejemplo:
```python
insertar_datos_masivos(sql_query_manager,
                        ambiente,
                        db_manager,
                        ambiente.data_connect_batch, # ESPECIFICA TIPO DE CONEXION ambiente.data_connect_batch o ambiente.data_connect_dms
                        column_names, # ESPECIFICA column_names
                        data) # ESPECIFICA DATOS COMO LISTA DE TUPLAS
```