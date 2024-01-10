# Cargar Datos

Puedes cargar datos usando PyMySQL para obtener datos en formato de lista de tuplas y nombres de columnas, o usar PySpark para manejar grandes volúmenes de datos distribuidos.

## 1.1 Con PyMySQL
Se utiliza una conexión por cada consulta SQL. Una vez que se obtienen los datos, la conexión se cierra automáticamente. Los errores durante la consulta se manejan como `ValueError`.

```python
data, column_names = obtener_data_con_pymysql(data_connect=ambiente.data_connect_batch, # ESPECIFICAR data_connect_batch o data_connect_dms
                                              query=query_bigdata)
```

## 1.2 Con PySpark


### 1.2.1 Obtener DataFrame
 El metodo **obtener_data_con_pyspark()** retorna un dataframe, **column_names (lista)** y **data (en formato lista de tuplas)** para poder ser leido por pymysql
1. Puedes obtener un DataFrame desde `table_name`, `query`, o un archivo de texto en S3.

    a. **Desde Query**:
    ```python
    df, column_names, data = obtener_data_con_pyspark(pyspark_client=pyspark_client,
                                                      data_connect_type='batch', # ESPECIFICAR 'batch' o 'dms'
                                                      query=query_1) # ESPECIFICAR QUERY
    ```

    b. **Desde Table Name**:
    ```python
    df, column_names, data = obtener_data_con_pyspark(pyspark_client=pyspark_client,
                                                      data_connect_type='batch', # ESPECIFICAR 'batch' o 'dms'
                                                      table_name=ambiente.table_name) # ESPECIFICAR TABLA
    ```

    c. **Desde Archivo de Texto en S3**:
       _has_headers: Especifica si la primera fila del archivo de texto contiene los nombres de las columnas._
    ```python
    df = pyspark_client.load_dataframe_from_txt_file(s3_path=ambiente.interface_routes['route_s3'],
                                                     has_headers=True)
    ```

### 1.2.2 Obtener DataFrame por Separado
En algunos casos, puede ser útil manejar por separado la obtención de datos.

```python
# Obtener solo el DataFrame
df = pyspark_client.load_dataframe_from_table(table=ambiente.table_name, connection_type='batch') # ESPECIFICAR 'dms' o 'batch'

# Obtener nombres de columnas
column_names = df.columns

# Convertir datos a lista de tuplas
data_como_lista_de_tuplas = pyspark_client.dataframe_to_list_of_tuples(dataframe=df)
```

Estos métodos ofrecen diversas formas de cargar datos, ya sea para pequeñas o grandes cantidades, y permiten una gestión eficiente según el origen de los datos.
