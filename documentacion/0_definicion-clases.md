# Documentacion de clases 

## Documentación de la Clase GlueLogger

## Descripción General

`GlueLogger` es una clase en Python diseñada para mejorar el registro de logs en la consola de cloudwatch de AWS, siendo especialmente útil en trabajos de AWS Glue. Proporciona una forma estructurada de emitir mensajes de log con contexto adicional, como el nombre de la clase, el nombre de la función y el número de línea desde donde se generó el log.

## Ejemplo de Uso

```python
logger = GlueLogger({'JOB_NAME': 'TrabajoDeProcesamientoDeDatos'})
logger.info('Inicio del proceso')
# Más declaraciones de log...
logger.error('Ocurrió un error')
logger.exception('Se produjo una excepción')
```

La clase GlueLogger es particularmente útil para trabajos de AWS Glue, pero también puede ser utilizada en otros contextos.
El logger proporciona un formato estandarizado para los mensajes de log, facilitando su análisis e interpretación.



## Documentación de la Clase Ambiente

### Descripción General

La clase `Ambiente` está diseñada para manejar las variables de entorno y configuraciones necesarias en un trabajo, especialmente en el contexto de AWS. Su responsabilidad incluye la obtención y validación de secretos de AWS, parámetros del trabajo, y la configuración de conexiones a bases de datos.

### Responsabilidades

1. Obtener secretos de AWS y parámetros del trabajo.
2. Validar que todas las variables de entorno requeridas estén presentes.
3. Establecer las conexiones a bases de datos en variables para su fácil acceso.

### Uso

La clase debe ser instanciada en el `main` del programa. Ejemplo:

```python
ambiente = Ambiente(args=['JOB_NAME','date','table_name','interface_output'])
# Acceder a la librería:
libreria = ambiente.LIB
# Acceder a datos de conexión de la base de datos:
data_connect_batch = ambiente.data_connect_batch
```

### Extensión
La clase está abierta a extensiones para agregar más argumentos según sea necesario, como event_date, event_date2, etc.
para extenderla se debe hacer a traves de la instancia: **ambiente.event_date2 = args['event_date2']**

### Métodos Principales
__init__(self, args, logger: GlueLogger)
Constructor de la clase. Inicializa el logger, carga y valida las variables de entorno y establece las conexiones a la base de datos.

generate_name_routes_file(self, interface_name, file_extension='TXT', glue_route='FTP_OUTPUT')
Genera y establece la variable interface_routes con las rutas de las interfaces.

bucket_name(self)
Propiedad que obtiene el nombre del bucket de AWS desde las variables de entorno.

data_connect_batch(self)
Propiedad que obtiene las credenciales y detalles de conexión para la base de datos batch.

data_connect_dms(self)
Propiedad que obtiene las credenciales y detalles de conexión para la base de datos DMS.



## Documentación de la Clase DataProcessor

### Descripción General

La clase `DataProcessor` se encarga de procesar archivos y generar interfaces en AWS, como transformar filas de datos individualmente.

### Responsabilidades

1. Procesar archivos: Generar interfaces a partir de resultados de bases de datos y escribir en archivos.
2. Transformación de datos: Modificar y adaptar los datos fila por fila según sea necesario.

#### Uso

Esta clase debe ser instanciada con un objeto `Ambiente` y un logger del tipo `GlueLogger`. Ejemplo:

```python
data_processor = DataProcessor(ambiente: Ambiente, logger: GlueLogger)
```

### Métodos Principales
1. __init__(self, ambiente, logger: GlueLogger)
Constructor de la clase. Inicializa el logger y el objeto Ambiente.

2. generate_interface(self, db_results)
### Descripción
Este método toma los resultados de la base de datos y los escribe en un archivo, transformando y formateando cada fila según sea necesario. Gestiona la escritura de datos en un archivo temporal y maneja errores relacionados con la entrada/salida de archivos.
Genera una interfaz basada en los resultados de una base de datos.

   1. Parámetros
   2. db_results: Resultados de una consulta a la base de datos que se procesarán para generar la interfaz.


# FileManager

## Descripción General

`FileManager` es una clase diseñada para manejar diversas operaciones relacionadas con la lectura y escritura de archivos, especialmente enfocada en la interacción con AWS S3. Proporciona métodos para verificar la existencia de archivos en S3, leer su contenido, generar archivos vacíos y subir archivos a AWS. Además, incluye funcionalidades para procesar y validar los datos leídos.

## Métodos

### `__init__(self, logger=GlueLogger)`
Constructor de la clase. Inicializa el FileManager con un logger.

#### Parámetros
- `logger` (GlueLogger): Objeto de logger para registrar eventos y errores.

### `set_ambiente(self, ambiente: Ambiente)`
Establece el ambiente para el FileManager.

#### Parámetros
- `ambiente` (Ambiente): Objeto de ambiente con configuraciones específicas.

### `check_file_exists(self, bucket_name, filepath)`
Verifica si un archivo existe en un bucket de S3.

#### Parámetros
- `bucket_name` (str): Nombre del bucket en S3.
- `filepath` (str): Ruta del archivo en el bucket.

### `read_file_s3(self, bucket_name, filepath)`
Lee un archivo de un bucket S3.
Por defecto filepath es **filepath=self.ambiente.interface_routes['route_s3']**

#### Parámetros
- `bucket_name` (str): Nombre del bucket en S3. Se obtiene de ambiente.bucket_name
- `filepath` (str): Ruta del archivo en el bucket. debe ser: **self.ambiente.interface_routes['route_s3']**

### `generate_empty_file(self)`
Genera un archivo vacío.

### `upload_file_to_aws(self)`
Sube un archivo a AWS.

### `readFileValidate(self, file, contents)`
Valida y procesa el contenido de un archivo leído de S3.

#### Parámetros
- `file` (?): Archivo a procesar.
- `contents` (?): Contenido del archivo.

## Uso
Para usar `FileManager`, primero se debe crear una instancia de la clase y luego invocar sus métodos según sea necesario, pasando los parámetros adecuados. Asegúrese de que todas las dependencias y configuraciones (como `GlueLogger` y `Ambiente`) estén correctamente establecidas.

```python
logger = GlueLogger()
ambiente = Ambiente(...)  # Configurar correctamente
file_manager = FileManager(logger)
file_manager.set_ambiente(ambiente)
# Ejemplo de uso de un método
file_manager.read_file_s3(self.ambiente.bucket_name, self.ambiente.interface_routes['route_s3'])
```


# DatabaseManager

## Descripción General

`DatabaseManager` es una clase diseñada para facilitar la interacción con bases de datos, específicamente para operaciones de inserción masiva. La clase proporciona métodos para dividir datos en lotes (chunks), insertar o actualizar registros de forma masiva utilizando múltiples hilos y establecer conexiones con la base de datos.

## Métodos

### `__init__(self, logger=GlueLogger)`
Constructor de la clase. Inicializa `DatabaseManager` con un logger.

#### Parámetros
- `logger` (GlueLogger): Objeto de logger para registrar eventos y errores.

### `_worker(self, data_chunk: List[Tuple], query: str, data_connect: dict)`
Método privado utilizado para insertar un chunk de datos en la base de datos.

#### Parámetros
- `data_chunk` (List[Tuple]): Datos a insertar.
- `query` (str): Query SQL para la inserción.
- `data_connect` (dict): Diccionario con datos de conexión a la base de datos.

### `divide_in_chunks(self, data: List[Tuple], chunk_size: int)`
Divide los datos en chunks de un tamaño específico.

#### Parámetros
- `data` (List[Tuple]): Datos a dividir.
- `chunk_size` (int): Tamaño de cada chunk.

### `bulk_insert_or_update_with_threads(self, data: List[Tuple], query: str, data_connect: dict, chunk_size=15000, max_threads=100)`
Inserta o actualiza registros en la base de datos de forma masiva utilizando múltiples hilos.

#### Parámetros
- `data` (List[Tuple]): Datos a insertar o actualizar.
- `query` (str): Query SQL para la inserción o actualización.
- `data_connect` (dict): Diccionario con datos de conexión a la base de datos.
- `chunk_size` (int, opcional): Tamaño de cada chunk de datos.
- `max_threads` (int, opcional): Máximo número de hilos a utilizar.

### `connect_db(self, data_connect: dict)`
Establece una conexión con la base de datos.

#### Parámetros
- `data_connect` (dict): Diccionario con datos de conexión a la base de datos.

## Ejemplo de Uso

```python
# Crear una instancia de DatabaseManager
logger = GlueLogger()
db_manager = DatabaseManager(logger)

# Datos de conexión a la base de datos
data_connect_batch = ambiente.data_connect_batch
data_connect_dms = ambiente.data_connect_dms

# Datos a insertar y consulta SQL
datos = [(1, 'dato1'), (2, 'dato2')]  # Ejemplo de datos a insertar
consulta_sql = "INSERT INTO tabla (columna1, columna2) VALUES (%s, %s)"

# Uso del método para inserción masiva
db_manager.bulk_insert_or_update_with_threads(data=datos, query=consulta_sql, data_connect=data_connect_dms)

```


## ResponseManager

### Descripción
`ResponseManager` gestiona respuestas de procesos, registrando éxitos o errores en la funcion main.

### Métodos
- `__init__(self, job_name: str, logger: GlueLogger)`: Inicializa la clase con un nombre de trabajo y un logger.
- `success(self)`: Retorna una respuesta de éxito. 'FIN PROCESO {job_name} EXITOSO
- `error(self, error_message: str = "")`: Retorna una respuesta de error con un mensaje opcional. FIN PROCESO ERROR: mensaje de error



## PysparkClient

### Descripción
`PysparkClient` maneja operaciones relacionadas con PySpark y conexiones de base de datos.

### Métodos
- `__init__(self, ambiente: Ambiente, args, file_manager: FileManager, logger: GlueLogger)`: Inicializa el cliente con contexto Spark, Glue y sesión.
- `_build_jdbc_url(self, connection_info)`: Construye una URL JDBC.
- `_get_connection_properties(self, connection_type)`: Obtiene propiedades de conexión.
- `_set_connection_dms(self)`: Establece conexión DMS (mambu).
- `_set_connection_batch(self)`: Establece conexión Batch.
- `load_dataframe_from_table(self, table_name, connection_type)`: Carga datos de una tabla en un DataFrame.
- `load_dataframe_from_query(self, query: str, connection_type: str)`: Carga datos de una consulta SQL en un DataFrame.
- `dataframe_to_list_of_tuples(self, dataframe)`: Convierte un DataFrame en una lista de tuplas.
- `write_dataframe_to_table(self, df, table_name, connection_type, overwrite=True)`: Escribe un DataFrame en una tabla.
- `create_dataframe_from_datalist(self, data: List, column_names: List)` : Crea un df a partir de una lista de tuplas 
- 
### Uso
df = pyspark_client.load_dataframe_from_table("nombre_tabla", "dms")
df = pyspark_client.load_dataframe_from_query("SELECT * FROM tabla", "batch")
tuplas = pyspark_client.dataframe_to_list_of_tuples(df)
pyspark_client.write_dataframe_to_table(df, "tabla_destino", "dms", overwrite=True)
df = create_dataframe_from_datalist( data=[('valor 1', 'valor2'), ( 'valor 1', 'valor 2')], column_names= ['col1', 'col2'] )


## PysparkUtils

### Descripción
`PysparkUtils` proporciona utilidades estáticas para la manipulación y transformación de DataFrames en PySpark.

### Métodos y ejemplos

#### `replace_null_values_with_zero(df, list_of_columns: List[str])`
Reemplaza los valores nulos en las columnas especificadas con cero. Útil para columnas numéricas.
```python
df = PysparkUtils.replace_null_values_with_zero(df, ["columna1", "columna2"])
```

`replace_null_string_values_with_empty(df, list_of_columns: List[str]) `
Reemplaza los valores nulos en las columnas especificadas con un string vacío. Útil para columnas de tipo string.
```python
df = PysparkUtils.replace_null_string_values_with_empty(df, ["columna1", "columna2"])
```

`mover_columna_a_ultima_posicion(df, nombre_columna)`
```python
df = PysparkUtils.mover_columna_a_ultima_posicion(df, "nombre_columna")
```


## SQLQueryManager

### Descripción
`SQLQueryManager` es una clase diseñada para crear consultas SQL para operaciones masivas como inserciones, actualizaciones y 'upserts'.

### Métodos

#### `__init__(self, ambiente: Ambiente, logger=GlueLogger)`
Inicializa el gestor de consultas SQL con un ambiente y un logger.
```python
sql_query_manager = SQLQueryManager(ambiente, logger)
```

`query_insert_masivo(self, table_name: str, column_names: List[str], data: List[Tuple[Any, ...]])`
Función que devuelve la consulta SQL de inserción para una tabla validando que la cantidad de largo de datos sea igual al numero de columnas.
:param table_name: Nombre de la tabla en la base de datos.
:param column_names: Lista de nombres de columnas en la tabla.
:param data: Lista de tuplas, donde cada tupla contiene los valores a insertar en las columnas.
:return: La consulta SQL generada  en formato string.

**Ejemplo:**                               
        "SELECT id, name FROM table_name" --> [(1,  2),   (2, 3),]
        columns order: --------------> ['id', 'name']
**IMPORANTE:** 
        El orden de los datos deben coincidir con el orden de las columnas.
        Si los datos son un select de una base de datos van a venir: id, name


```python
query = sql_query_manager.query_insert_masivo("mi_tabla", ["columna1", "columna2"], [(1, "dato1"), (2, "dato2")])
```

`query_update_masivo(self, table_name: str, column_names: List[str], condition_column: str, data: List[Tuple[Any, ...]])`
Esta función crea una consulta SQL para actualizar múltiples registros en una base de datos.
Se basa en solo una condicion, si necesitas mas de una condicion se debe modificar la seccion del WHERE.

Es importante que en la data, en cada tupla de la lista,
el valor del WHERE, por ejemplo num_cta, se encuentre al final de la tupla.
ejemplo: [(valor_columna1, valor_columna2, ...., valor_num_cta), (...)]


Args:
    table_name (str): Nombre de la tabla en la base de datos.
    data: set de datos
    column_names (List[str]): Lista de nombres de columnas de la tabla a actualizar.
    condition_column (str): Nombre de la columna que se utiliza en la cláusula WHERE para la condición de actualización.

Returns:
    str: La consulta SQL generada para actualizar los registros en la base de datos.

Ejemplo:
    query = SQLQueryManager.query_update_masivo(table_name="mi_tabla",
                                                column_names=["monto", "interes", num_cta],
                                                condition_column="num_cta",
                                                data=[(monto1, interes1, num_cta_1), (monto2, interes2, num_cta_2)])
    # Salida: "UPDATE mi_tabla SET monto = %s, interes = %s, WHERE num_cta = %s;"

Genera una consulta SQL para actualizar múltiples registros en una tabla.

query = sql_query_manager.query_update_masivo("mi_tabla", ["columna1", "columna2"], "columna_condicion", [(1, "dato1", 3), (2, "dato2", 4)])



`query_upsert_masivo(self, table_name: str, column_names: List[str], unique_key: str, data: List[Tuple[Any, ...]])`

Crea una consulta SQL para realizar un 'upsert' (insertar o actualizar) en una tabla de MySQL.
Utiliza la sentencia INSERT INTO ... ON DUPLICATE KEY UPDATE ,
en donde toma la clave primaria de la tabla como campo a validar si el registro existe o no para su actualizacion
Args:
    table_name (str): Nombre de la tabla en la base de datos.
    column_names (List[str]): Lista de nombres de columnas de la tabla.
    unique_key (str): Nombre de la columna que es clave única o primaria.
    data (List[Tuple[Any, ...]]): Lista de tuplas con los datos a insertar o actualizar.

Returns:
    str: Consulta SQL para realizar el 'upsert'.

Ejemplo:
    # Suponiendo que 'num_cta' es la clave única y 'monto', 'interes' son los otros campos
    query = SQLQueryManager.query_upsert_masivo(table_name="mi_tabla",
                                                column_names["num_cta", "monto", "interes"],
                                                unique_key="num_cta",
                                                data=[(23451, "1000", 3500), (34565, "2300", 3444)])
    # La consulta resultante intentará insertar las filas, y si el 'num_cta' ya existe, actualizará 'monto' y 'interes'.

Return ejemplo:
    INSERT INTO mi_tabla (num_cta, monto, interes) VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE monto = VALUES(monto), interes = VALUES(interes);

1. Ejemplo:
```python
query = sql_query_manager.query_upsert_masivo("mi_tabla", ["num_cta", "monto", "interes"], "num_cta", [(23451, 1000, 3500), (34565, 2300, 3444)])
```


## DataManager

### Descripción
`DataManager` es una clase estática diseñada para facilitar operaciones de manejo de datos con PySpark y SQL, como obtener datos, insertarlos y actualizarlos masivamente.

### Métodos

#### `obtener_data_con_pyspark(pyspark_client: PysparkClient, data_connect_type: str, query: str=None, table_name: str=None)`
Obtiene los datos de la base de datos utilizando pyspark basado en la conexion de datos que se obtiene de la clase ambiente.
Retornara un dataframe, lista de nombre de columnas y datos en formato lista de tuplas para usar insercion masiva con mysql

Si se desea traer los datos de una tabla, declarar solo 'table_name', caso contrario declarar solo 'query'
Args:
    data_connect: string de tipo 'batch' o 'dms'
    query (str, optional): Para mysql es requerida la query.
    table_name (str, optional): Es requerida para pyspark si se desea ob

Returns:
    3 objetos
        1: Dataframe con los datos obtenidos
        2: Lista con nombre de columnas de la tabla [column1, column2, etc...]
        3: Los datos de la tabla en formato pymysql:  [(valor1, valo2, ...), (valo1, valo2, ...)]

Obtiene datos de la base de datos utilizando PySpark. Puede especificar una tabla o una consulta SQL.

##### Ejemplo de Uso
```python
df, column_names, db_data = DataManager.obtener_data_con_pyspark(pyspark_client, 'dms', query="SELECT * FROM mi_tabla")
```


`insertar_datos_masivos(sql_query_manager: SQLQueryManager, db_manager: DatabaseManager, data_connect: dict, column_names: List, data: List[Tuple], table_name: str)`
Inserta datos masivamente en una tabla SQL. Requiere una consulta SQL de inserción masiva, los datos y el nombre de la tabla.

```python
DataManager.insertar_datos_masivos(sql_query_manager=sql_query_manager,
                                   db_manager=db_manager,
                                   data_connect=ambiente.data_connect_batch,
                                   column_names=['col1', 'col2'],
                                   data=[(1, 'dato1'),(2, 'dato2')],
                                   table_name='mi_tabla') # ambiente.table_name
```



`actualizar_datos_masivos(sql_query_manager: SQLQueryManager, ambiente: Ambiente, db_manager: DatabaseManager, data_connect: dict, column_names: List[str], data: List[Tuple], condition_column: str, is_upsert: bool=True)`

Funcion que actualiza datos masivamente en una tabla sql.
Esta funcion construye la query de actualizacion o upsert basado en la data y nombre de columnas.
Si is_upsert es falso, entonces solo necesitas actualizar y deberas modificar el orden de data 
para que el valor para la columna de la sentencia where, quede en la ultima posicion.

Args:
    column_names:  Lista de nombre de columnas que se van a actualizar.
                    Se debe incluir la columna para la sentencia where
    data: Lista de tuplas con la data ordenada en el mismo orden que column names.
        el valor de la sentencia where debe ir al final de cada tupla
    condition_column: el nombre de la columna para la sentencia where
    is_upsert: Determina si la consulta sera un upsert (insertar o actualizar, o solo actualizar)

Ejemplo:
si la data viene asi:
    data = [(num_cta, valo2, valo2 )]
En donde 'num_cta' es la columna para la sentencia WHERE,
deberas modificar tu consulta SELECT en donde traes los datos para que num_cta quede asi:
    data = [(valo1, valo2, num_cta )]
   

Actualiza datos masivamente en una tabla SQL. Se puede elegir entre actualizar o realizar un 'upsert'.

### Ejemplo de uso
```python
DataManager.actualizar_datos_masivos(sql_query_manager=sql_query_manager,
                                     ambiente=ambiente,
                                     db_manager=db_manager,
                                     data_connect=data_connect, # ambiente.data_connect_batch
                                     column_names=['col1', 'col2', 'col_condicion'],
                                     data=[(1, 'dato1', 3), (2, 'dato2', 4)],
                                     condition_column='col_condicion',
                                     is_upsert=True) # o False si es solo actualizar registros
```


`generate_files_upload_interface(data, ambiente, data_processor, file_manager, logger)`
Genera archivos y los sube a AWS. En caso de no tener datos, genera un archivo vacío.

### Ejemplo de Uso
```python
DataManager.generate_files_upload_interface(data, ambiente, data_processor, file_manager, logger)
```



## Init

### Descripción
`Init` es una clase estática diseñada para inicializar componentes y configuraciones en un entorno AWS Glue, facilitando la carga de argumentos, la configuración de loggers y la preparación del ambiente de trabajo.

### Métodos

#### `load_args_from_aws(args: List)`
Carga los argumentos pasados a un script AWS Glue desde la línea de comandos.

##### Ejemplo de Uso
```python
args = Init.load_args_from_aws(['JOB_NAME', 'table_name'])
```


`inicializar_logger_y_response(args)`
Inicializa y retorna un logger y un gestor de respuestas basado en los argumentos proporcionados.

##### Ejemplo de uso
```python
logger, process_response = Init.inicializar_logger_y_response(args)
```

`inicializar_proceso(args, glue_logger)`
Inicializa y retorna componentes clave del proceso, incluyendo el ambiente, el gestor de base de datos, el gestor de consultas SQL, el procesador de datos, el gestor de archivos y el cliente de PySpark.

#### Ejemplo de uso:
```python
ambiente, db_manager, sql_query_manager, data_processor, pyspark_client, file_manager = Init.inicializar_proceso(args, glue_logger)
```



# Inicializacion de un job o proceso

## Función Main

### Descripción
La función `main` sirve como punto de entrada principal para un script de AWS Glue. Se encarga de inicializar el entorno, cargar y procesar datos, generar interfaces y manejar errores.

### Proceso General

1. **Cargar y Validar Argumentos**: Utiliza `Init.load_args_from_aws` para cargar los argumentos del trabajo de Glue.

2. **Inicializar Logger y Response**: Configura el logger y el gestor de respuestas para el proceso.

3. **Inicializar el Proceso**: Establece componentes clave como el ambiente, el gestor de base de datos, entre otros.

4. **Obtener y Ejecutar Queries SQL**: Carga y ejecuta consultas SQL necesarias para el proceso.

5. **Cargar Datos**: Carga los datos necesarios para el proceso utilizando PySpark.

6. **Mapear Funciones y Validaciones**: Realiza validaciones y limpia los datos utilizando funciones personalizadas.

7. **Transformar y Procesar Datos**: Aplica las transformaciones necesarias a los datos.

8. **Generar y Subir Interfaces**: Procesa y prepara los datos para su exportación o uso posterior.

9. **Manejo de Errores**: Captura y maneja los errores que puedan ocurrir durante el proceso.

### Ejemplo de Implementación
```python
def main():
    try:
        # ------------------------------------------------- 0 Cargar y validar secretos -----------------------------------------------------------------------
        args = Init.load_args_from_aws(args=['JOB_NAME', 'date', 'table_name', 'interface_output']) 
        logger, process_response = Init.inicializar_logger_y_response(args=args)
        ambiente, db_manager, sql_query_manager, data_processor, pyspark_client, file_manager = Init.inicializar_proceso(args, glue_logger=logger)
        
        # Configurar interface output:
        ambiente.interface_output_name = args['interface_output']
        ambiente.interface_routes_input = ambiente.generate_name_routes_file(interface_name= ambiente.interface_output_name,
                                                                             glue_route='FTP_OUTPUT')

        # -------------------------------------------------- 1 OBTENER QUERIES SQL -------------------------------------------------------------------------  
        logger.info('OBTENIENDO QUERIES DEL PROCESO...')    
        ## establece tus queries aqui        
        
        # -------------------------------------------------- 2 CARGAR DATOS ---------------------------------------------------------------------------------
        logger.info('CARGANDO DATOS DEL JOB...')  
        ## Carga tus datos aqui. Ejemplo    
        ## df = pyspark_client.load_dataframe_from_query(query=query_informacion, connection_type='batch') # Reemplazar query
        ## df.show()

        # Continuar con proceso...

        
    
    except ValueError as e:
        # Manejo de errores conocidos
        logger.exception("ERROR DE PROCESO")
        DataManager.generate_files_upload_interface(...)
        return process_response.error(error_message=str(e))

    except Exception as e:
        # Manejo de errores inesperados
        logger.exception("ERROR INESPERADO")
        DataManager.generate_files_upload_interface(...)
        return process_response.error(error_message=str(e))

    return process_response.success()

    
if __name__ == '__main__':
    main()
```

