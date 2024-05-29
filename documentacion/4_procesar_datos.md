# Procesamiento de datos con pyspark_client y metodos disponibles:
 Metodos comunmente utilizados disponibles de la clase PysparkUtils y pyspark_client.


1. **pyspark_client.dataframe_to_list_of_tuples(dataframe=df)**: Convierte los datos de un dataframe a una lista de tuplas para ser insertado luego en una base de datos utilizando el metodo **DataManager.insertar_datos_masivos** o el metodo **DataManager.actualizar_datos_masivos**


2. **PysparkUtils.replace_null_values_with_zero(df, list_of_columns)**: Convierte a 0 todos los valores nulos de las columnas especificadas del dataframe


2. **PysparkUtils.replace_null_string_values_with_empty(df, list_of_columns: List[str])**: Convierte a vacio "" todos los valores nulos de las columnas especificadas del dataframe


2. **PysparkUtils.mover_columna_a_ultima_posicion(df, nombre_columna)**: Mueve a la ultima posicion una columna, su uso es especifico par las funciones de **DataManager.actualizar_datos_masivos**, esto hace un reordenamiento de los datos y columnas con el fin de hacer que las columnas coincidan con los datos


