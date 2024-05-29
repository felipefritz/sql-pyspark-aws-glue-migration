# Actualización de Datos Masivos desde dataframe pyspark a tabla SQL

La función `actualizar_datos_masivos()` permite actualizar grandes cantidades de datos en una base de datos. Puede manejar actualizaciones con una única condición.
 
**def actualizar_datos_masivos(sql_query_manager: SQLQueryManager, ambiente: Ambiente, db_manager: DatabaseManager, data_connect: dict, table_name: str, column_names: List[str], data: List[Tuple], condition_column: str, is_upsert: bool= True):**

- Esta funcion construye la query de actualizacion o upsert basado en la data y nombre de columnas.
Si is_upsert es falso, entonces solo necesitas actualizar y deberas modificar el orden de data 
para que el valor para la columna de la sentencia where, quede en la ultima posicion.


si la data viene asi:
**data = [(num_cta, valo1, valo2 )]**
- En donde **'num_cta'** es la columna para la sentencia **WHERE**,
deberas **modificar o reordenar el dataframe para seleccionar solo las columnas requeridas**.


## Ejemplo de uso:

```python
df.select('num_cta', 'valo1', 'valo2')
data_as_list_of_tuples = pyspark_client.dataframe_to_list_of_tuples(dataframe=df)



DataManager.actualizar_datos_masivos(sql_query_manager=sql_query_manager,
                                     ambiente=ambiente,
                                     db_manager=db_manager,
                                     data_connect=ambiente.data_connect_batch,  # REEMPLAZAR SI SE INSERTA EN DMS(MAMBU) O BATCH
                                     table_name=ambiente.table_name             # REEMPLAZAR CON NOMBRE DE TABLA A ACTUALIZAR
                                     column_names=df.column_names,              # Ejemplo: ['col1', 'col2', 'num_cta']
                                     data=data_as_list_of_tuples ,              # Ejemplo: [('dato_col_1', 'dato_col_2', 3), ('dato1, 'dato2', 4)]
                                     condition_column='num_cta',                # Ejemplo: 'num_cta'
                                     is_upsert=False)                           # o True si necesitas insertar si no existe y actualizar si existe

```