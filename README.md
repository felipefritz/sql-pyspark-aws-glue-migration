# Plantilla para Lectura y Reutilización de Código

Esta plantilla está diseñada para facilitar la lectura y reutilización de código. A continuación, se describen las clases y directrices a seguir:

## Configuracion general:

1. Usar el archivo index.py como referencia para realizar un nuevo Job
   1. Copiar el contenido de **index.py** y tambien lo que esta en **terraform/glue.tf** y pegarlo en tu proyecto
2. Reemplazar la linea 4 en los archivos de variables de terraform (terraform/variables_desa.tfvars, etc..)
   1. Usar este archivo: **"--extra-py-files" = "s3://sftp-ctabeneficio-dev/glue/LIBRARY/index_pyspark.py"**

3. El archivo template_base.py contiene un main mas limpio en el cual puedes adaptar el job a tu manera apoyandote en los archivos de /documentacion.
4. El archivo index.py es una template con codigo que se utiliza para jobs de lecutra de tablas, insertar o actualizar tablas sql y generar interface.

## Archivo index_pysppark.py
5. Este archivo es el que contiene todo el codigo que esta en bucket de S3 al cual hace referencia tu proyecto ( Ya no es Index.py)


# Documentación del Script de PySpark

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


### Clase `ResponseManager`
#### Descripción
- Maneja las respuestas de éxito o error del proceso.

#### Métodos Principales
- `success(self)`: Retorna un mensaje de éxito.
- `error(self, error_message)`: Retorna un mensaje de error.

### Clase `SQLQueryManager`
#### Descripción
- Almacena y gestiona consultas SQL.


### Clase `PysparkClient`
#### Descripción
- Cliente de PySpark para la carga y escritura de DataFrames.



### `main()`
- Función principal que ejecuta el proceso.
- Maneja excepciones y errores.

## Uso y Ejecución

1. **Configuración de AWS**: Asegurarse de que las credenciales y permisos de AWS estén configurados correctamente.
2. **Parámetros del Job**: Proporcionar los parámetros requeridos como `JOB_NAME`, `date`, `table_name`, y `interface_output`.
3. **Ejecución**: Ejecute el script en un entorno de AWS Glue o en un entorno local que tenga configurado PySpark y acceso a AWS.
4. **Logs**: Monitoree los logs para seguimiento y depuración.

## IMPORTANTE
1. Si el job tiene mas parametros como table_name2, date_2, se deben especificar como ambiente.parametro2 = args['parametros_2"].

2. Conexiones a bases de datos: 
- Una vez realizada una consulta a la base de datos, automaticamente se cerrara la conexion (DatabaseManager.execute_SQL_select())
- Cada consulta de tipo select, esta retornara los datos(lista de tuplas) y nombres de columnas (list)

1. Manejo de errores 

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
from index_pyspark import Init, DataManager, PysparkUtils

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType


def main():
    
    try:
        logger.info('INICIALIZANDO VARIABLES DE ENTORNO Y CONFIGURANDO AMBIENTES...')
        
        args = Init.load_args_from_aws(args=['JOB_NAME', 'date', 'table_name', 'interface_output']) 
        logger, process_response = Init.inicializar_logger_y_response(args=args)
        ambiente, db_manager, sql_query_manager, data_processor, pyspark_client, file_manager = Init.inicializar_proceso(args, glue_logger=logger)
        
        # Configurar interface output:
        ambiente.interface_output_name = args['interface_output']
        ambiente.interface_routes_input = ambiente.generate_name_routes_file(interface_name= ambiente.interface_output_name,
                                                                             glue_route='FTP_OUTPUT')
        
        
        logger.info('OBTENIENDO QUERIES DEL PROCESO...')    
        # Logica de establecer o llamar queries (fuera del main)   
        query_ejemplo = f"select * from {ambiente.table_name}"    
        
        
        logger.info('CARGANDO DATOS DEL JOB...')  
        ## Logica para cargar tablas, ejemplo:    
        ## df = pyspark_client.load_dataframe_from_query(query=query_ejemplo, connection_type='batch') # Reemplazar query
        ## df.show()


        logger.info('MAPEANDO FUNCIONES LIB A PYSPARK...')    
        # Logica Convertir funciones de Lib o funciones de python a pyspark...
        valida_monto = udf(ambiente.LIB.validateAmount, StringType())        
        validate_string = udf(ambiente.LIB.validateEmptyValue, StringType())
        

        logger.info('VALIDANDO Y LIMPIANDO DATOS...')      
         # Logica de limpiar o validar dataframes...


        logger.info('TRANSFORMANDO Y PROCESANDO DATOS...')      
        # Logica de transformar datos...
        
        
        
        # LAS SECCIONES 1 Y 2 SON OPCIONALES SI NECESITAS INSERTAR O ACTUALIZAR UNA TABLA. 
        # 1.
        # logger.info('INSERTANDO DATOS DESDE PYSPARK A SQL...')
        
        
        #  DataManager.insertar_datos_masivos(sql_query_manager,
        #                                    db_manager,
        #                                    data_connect=ambiente.data_connect_batch,  # REEMPLAZAR SI SE INSERTA EN DMS(MAMBU) O BATCH
        #                                    column_names=df.columns,                   # REEMPLAZAR DATAFRAME O COLUMN NAMES COMO LISTA...
        #                                    table_name=ambiente.table_name           # REEMPLAZAR CON NOMBRE DE TABLA A INSERTAR REGISTROS
        #                                    )
        
        
        
        # 2.
        # logger.info('ACTUALIZANDO DATOS DESDE PYSPARK A SQL...')
        
        #  DataManager.actualizar_datos_masivos(sql_query_manager=sql_query_manager,
        #                              ambiente=ambiente,
        #                              db_manager=db_manager,
        #                              data_connect=ambiente.data_connect_batch,  # REEMPLAZAR SI SE INSERTA EN DMS(MAMBU) O BATCH
        #                              table_name=ambiente.table_name             # REEMPLAZAR CON NOMBRE DE TABLA A ACTUALIZAR
        #                              column_names=['col1', 'col2', 'col_condicion'],
        #                              data=[(1, 'dato1', 3), (2, 'dato2', 4)],
        #                              condition_column='col_condicion',          # EJEMPLO DE 'num_cta_mambu'
        #                              is_upsert=True)                            # o False si es solo actualizar registros


        logger.info('GENERANDO INTERFACE...')    
        # Logica de generar interface... (descomentar lineas siguientes y reemplazar variables si es necesario)
        # data_as_list_of_tuples = []  
        # data_as_list_of_tuples = pyspark_client.dataframe_to_list_of_tuples(dataframe=df)
        
        # DataManager.insertar_datos_masivos(sql_query_manager, db_manager, data_connect=ambiente.data_connect_batch, column_names=df, table_name=ambiente.table_name, column)
        
        # DataManager.generate_files_upload_interface(data=data_as_list_of_tuples, # ESPECIFICAR LA DATA EN FORMATO LISTA DE TUPLAS
        #                                 ambiente=ambiente,
        #                                 data_processor=data_processor,
        #                                 file_manager=file_manager,
        #                                 logger=logger)
    
    except ValueError as e:
        # Errores del codigo
        
        logger.exception(f"ERROR DE PROCESO")
        
        # Si falla se genera archivo vacio
        DataManager.generate_files_upload_interface(data=[], # ESPECIFICAR LA DATA EN FORMATO LISTA DE TUPLAS
                                        ambiente=ambiente,
                                        data_processor=data_processor,
                                        file_manager=file_manager,
                                        logger=logger)
        return process_response.error(error_message=str(e))

    except Exception as e:
        # Errores inesperados
        logger.exception(f"ERROR INTESPERADO")
        # Si falla se genera archivo vacio
        DataManager.generate_files_upload_interface(data=[], # ESPECIFICAR LA DATA EN FORMATO LISTA DE TUPLAS
                                        ambiente=ambiente,
                                        data_processor=data_processor,
                                        file_manager=file_manager,
                                        logger=logger)
        
    return process_response.success()
        
        
if __name__ == '__main__':
    main()

```