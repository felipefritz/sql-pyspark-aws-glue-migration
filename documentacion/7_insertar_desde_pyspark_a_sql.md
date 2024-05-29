# Actualización de Datos Masivos

- La función `insertar_datos_masivos()` permite insertar grandes cantidades de datos en una base de datos.
- Esta funcion recibe el tipo de conexion: **ambiente.data_connect_batch** o **ambiente.data_connect_dms**
- column_names: Es una lista con el nombre de las columnas, estas deben estar ordenadas en el mismo orden que data
- data: Es una lista de tuplas con los datos a insertar, debe tener el mismo largo y ordenadas con el mismo orden de column_names


- Ejemplo:
```python
data_as_list_of_tuples = pyspark_client.dataframe_to_list_of_tuples(dataframe=df)
column_names = df.columns
DataManager.insertar_datos_masivos(sql_query_manager,
                                  db_manager,
                                  data_connect=ambiente.data_connect_batch,  # REEMPLAZAR SI SE INSERTA EN DMS(MAMBU) O BATCH
                                  column_names=column_names,                  
                                  data=data_as_list_of_tuples                
                                  table_name=ambiente.table_name)            # REEMPLAZAR CON NOMBRE DE TABLA A INSERTAR REGISTROS
                                  
```