# Plantilla para Lectura y Reutilización de Código

Esta plantilla está diseñada para facilitar la lectura y reutilización de código. A continuación, se describen las clases y directrices a seguir:

# Documentación del Script de PySpark en Formato Markdown

## Descripción General
Este script en Python utiliza PySpark para realizar operaciones de procesamiento de datos y manejo de bases de datos. Está diseñado para ser ejecutado en un entorno de AWS Glue, interactuando con AWS Secret Manager para la gestión de secretos, y utilizando PyMySQL para operaciones de base de datos.

## Clases y Funciones Principales

### Clase `Ambiente`
#### Descripción
- Responsable de obtener, validar y establecer variables de entorno y argumentos del job.
- Gestiona la conexión con bases de datos y AWS Secret Manager.

#### Métodos Principales
- `__init__(self, args)`: Constructor que inicializa la clase con argumentos específicos.
- `_load_env_vars(self)`: Carga variables desde AWS Secret Manager.
- `_validate_env_variables(self)`: Valida que todas las variables de entorno estén disponibles.
- `_generate_name_routes_file(self, file_extension='TXT')`: Genera rutas de archivo para la interfaz.

### Clase `DataProcessor`
#### Descripción
- Encargada de procesar archivos y transformar filas de datos.

#### Métodos Principales
- `__init__(self, ambiente)`: Constructor que inicializa la clase con un objeto `Ambiente`.
- `_format_db_row(self, db_row, event_date=None)`: Formatea los datos de una fila de la base de datos.
- `generate_interface(self, db_results, event_date)`: Genera un archivo de interfaz con los resultados de la base de datos.

### Clase `FileManager`
#### Descripción
- Maneja la lectura y escritura de archivos, y la subida de archivos a AWS.

#### Métodos Principales
- `generate_empty_file(self)`: Genera un archivo vacío.
- `upload_file_to_aws(self)`: Sube archivos a AWS.
- `readFileValidate(self, file, contents)`: Lee y valida el contenido de un archivo.

### Clase `DatabaseManager`
#### Descripción
- Gestiona la conexión con bases de datos y ejecuta operaciones de inserción masiva y consultas SQL.

#### Métodos Principales
- `bulk_insert_with_threads(self, data, query, data_connect, chunk_size=15000, max_threads=100)`: Inserta datos en la base de datos utilizando múltiples hilos.
- `connect_db(data_connect)`: Establece una conexión con la base de datos.
- `execute_SQL_select(query, data_connect)`: Ejecuta una consulta SELECT en la base de datos.

### Clase `ResponseManager`
#### Descripción
- Maneja las respuestas de éxito o error del proceso.

#### Métodos Principales
- `success(self)`: Retorna un mensaje de éxito.
- `error(self, error_message)`: Retorna un mensaje de error.

### Clase `SQLQueryManager`
#### Descripción
- Almacena y gestiona consultas SQL.

#### Métodos Principales
- `select_intereses_bigdata(table_name, event_date)`: Genera una consulta SELECT de ejemplo.
- `query_insert_masivo(table_name, column_names, data)`: Genera una consulta de inserción masiva.

### Clase `PysparkClient`
#### Descripción
- Cliente de PySpark para la carga y escritura de DataFrames.

#### Métodos Principales
- `load_dataframe_from_table(self, table_name, connection_type)`: Carga un DataFrame desde una tabla de base de datos.
- `write_dataframe_ignoring_duplicates(self, df, table_name, connection_type)`: Escribe un DataFrame en la base de datos ignorando duplicados.

## Funciones Principales del Script

### `inicializar_proceso(args)`
- Inicializa el proceso y valida las variables.
- Retorna instancias de las clases necesarias para el proceso.

### `insertar_datos_masivos(...)`
- Inserta datos masivamente en la base de datos.


### `actualizar_datos_masivos(...)`
- Actualiza datos masivamente en la base de datos.

### `generate_files_upload_interface(...)`
- Genera y sube archivos de interfaz.

### `obtener_data_con_pyspark(...)`
- Obtiene datos de la base de datos utilizando PySpark .

### `obtener_data_con_pymysql(...)`
- Obtiene datos de la base de datos utilizando pymysql .




### `main()`
- Función principal que ejecuta el proceso.
- Maneja excepciones y errores.

## Uso y Ejecución

1. **Configuración de AWS**: Asegurarse de que las credenciales y permisos de AWS estén configurados correctamente.
2. **Parámetros del Job**: Proporcionar los parámetros requeridos como `JOB_NAME`, `date`, `table_name`, y `interface_output`.
3. **Ejecución**: Ejecute el script en un entorno de AWS Glue o en un entorno local que tenga configurado PySpark y acceso a AWS.
4. **Logs**: Monitoree los logs para seguimiento y depuración.

## IMPORTANTE
1. Si el job tiene mas parametros como table_name2, date_2, se deben especificar en la clase ambiente y obtenerlos desde args.

2. Conexiones a bases de datos: 
- Una vez realizada una consulta a la base de datos, automaticamente se cerrara la conexion (DatabaseManager.execute_SQL_select())
- Cada consulta de tipo select, esta retornara los datos(lista de tuplas) y nombres de columnas (list)

3. Manejo de errores 

- Al extender clases y crear nuevas funciones, no se debe utiliza el siguiente formato para retornar errores:
    ```python
    return {'status': 400, 'code': '-1', 'body': 'NOK'}
    ```
- Se deben usar bloques `try-except` para manejar excepciones:
    ```python
    try:
        # tu codigo
    except Exception: 
        raise ValueError("Mensaje de error")
    ```
- Esto es importante ya que hay una clase encargada de manejar los mensajes de éxito o error que requiere levantar excepciones de esta manera.
- Automaticamente al levantar excepciones con ValueError('mensaje') la clase ResponseManager retornara:
  **{'status': 400, 'code': '-1', 'body': 'NOK'}**
- Si vas a crear una funcion, lo idea es crearla en una clase encargada de tu funcion y utilizar try except 

4. Generacion de interfaces:
- Se utiliza la funcion **generate_files_upload_interface()**
- Si se le pasa la lista de datos vacia, se llamara a **LIB.generate_empty_file()** y **LIB.upload_file_to_aws()**
- Si la lista tiene datos se utilizara **DatasProcesor.generate_interface(db_results=data)** y luego **LIB.upload_file_to_aws()**.
- Puedes modificar esta funcion para tu job.
- Si necesitas utilizar otras funciones de LIB para gestionar archivos, los puedes agregar a la clase **FileManager()** o **DataProcesor()**

5. Insercion de datos:
- Se utilizara la funcion **insertar_datos_masivos()** en donde debes pasarle una lista de columnas ordenadas en el mismo orden en el que vienen los datos
 o  de tu consulta select ( automaticamente al ejecutar **execute_SQL_select()**, obtienes los nombres de columnas y datos ordenados)
- Se utilizara la funcion para insertar datos de manera masiva con multi hilos, lo cual garantizara transacciones de insercion mas rapidas.
- La clase **SQLQueryManager** contiene la funcion **query_insert_masivo**, la cual validara que los datos y columnas coincidan en longitud.
6. Manejo de duplicados:
- La insercion masiva de datos utiliza INSERT IGNORE INTO ...lo cual ignorara si un registro ya se encuentra en la base de datos.


## Ejemplo de uso para main()

```python
def main():

    try:
        # ------------------------------------------------- 0 Cargar y validar secretos -----------------------------------------------------------------------
 
        args = getResolvedOptions(sys.argv, ['JOB_NAME','date','table_name','interface_output']) # ESPECIFICA aca nuevas variables de args
        ambiente, db_manager, sql_query_manager, data_processor, process_response, pyspark_client, file_manager = inicializar_proceso(args)


        # -------------------------------------------------- 1 OBTENER QUERIES SQL -------------------------------------------------------------------------
        ## Declara tus queries sql como funciones en la clase SQLQueryManager con el decorador @staticmethod y utilizalas de esta manera:
        query_1 = sql_query_manager.select_intereses_bigdata(table_name=ambiente.table_name, event_date=ambiente.event_date)


        # -------------------------------------------------- 2 CARGAR DATOS ---------------------------------------------------------------------------------
        # Puedes cargar datos con PYMYSQL el cual retornara data y column_names, y tambien puedes cargar datos con PYSPARK

        # CON PYMYSQL
        data, column_names = obtener_data_con_pymysql(data_connect=ambiente.data_connect_batch, # ESPECIFICAR data_connect_batch o data_connect_dms
                                                      query=query_1)
        # CON PYSPARK                                            
        df, column_names, data_pyspark = obtener_data_con_pyspark(pyspark_client=pyspark_client,
                                                                  data_connect_type='batch', # ESPECIFICAR tipo de conexion 'batch' o 'dms'
                                                                  table_name=ambiente.table_name) # ESPECIFICAR NOMBRE DE TABLA


        ## ------------------------------------------------- 3 TRANSFORMACION DE DATOS -----------------------------------------------------------------------
        df = df.withColumn("monto", lit("1000")).withColumn("nuevo_monto", lit("2000")) 
        # Si son pocos registros puedes utilizar la transformacion de manera convencional utilizando:
        data_processor.generate_interface(db_results=data, event_date=ambiente.date)


        ## ----------------------------------------------- 4 INSERTAR DATOS O ACTUALIZAR -----------------------------------------------------------------------------
        # Se realiza a traves de pymysql con hilos, y se insertan los datos de forma masiva con los datos divididos en trozos de 15000 y 100 threads
        if data and column_names: # especifica donde tienes los datos y nombre de columnas
            insertar_datos_masivos(sql_query_manager,
                                    ambiente,
                                    db_manager,
                                    ambiente.data_connect_batch, # ESPECIFICA TIPO DE CONEXION ambiente.data_connect_batch o ambiente.data_connect_dms
                                    column_names, # ESPECIFICA column_names
                                    data) # ESPECIFICA DATOS COMO LISTA DE TUPLAS

            actualizar_datos_masivos(sql_query_manager=sql_query_manager,
                                    ambiente=ambiente,
                                    db_manager=db_manager,
                                    data_connect =ambiente.data_connect_batch, # ESPECIFICA TIPO DE CONEXION ambiente.data_connect_batch o ambiente.data_connect_dms
                                    column_names =column_names, # ESPECIFICA column_names []
                                    data=data,
                                    condition_column = "num_cta" # especifica el nombre de la columna de la sentencia WHERE
                                    is_upsert=False)  # especifica si necesitas insertar o actualizar los registros
                                  

        # 4. ------------------------------------------------ 5 GENERAR INTERFACE ------------------------------------------------------------------------------
        # Si no hay datos, la funcion se encargara de generar un archivo vacio y subirlo a AWS.

        generate_files_upload_interface(data=data, # ESPECIFICAR LA DATA EN FORMATO LISTA DE TUPLAS
                                        ambiente=ambiente,
                                        data_processor=data_processor,
                                        file_manager=file_manager)
        
        return process_response.success()
    
    except ValueError as e:
        # Errores del codigo

        """"
            Si hay un error en cualquier parte del codigo que utiliza: 
            (recuerda siempre utilizar este bloque para manejar errores y levantarlos con raise ValueError() en tus funciones)

            try:
                tu codigo...
            except Exception as e:
                raise ValueError(f"[NombreClase.NombreFuncion] ERROR: {str(e)}")

            se capturara aca y retornara:
            {'status': 400, 'code': '-1', 'body': 'NOK'}
        """
        print(str(e))
        return process_response.error(error_message=str(e))

    except Exception as e:
        # Errores inesperados
        print(f'Error inesperado: {str(e)}')
        return process_response.error(error_message=str(e))

```