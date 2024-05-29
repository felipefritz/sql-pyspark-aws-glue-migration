import sys
import logging
import inspect
import pymysql
from typing import Tuple, List, Any
from concurrent.futures import ThreadPoolExecutor

from pyspark.sql.types import StringType, FloatType
from pyspark.sql.functions import col,  lit, coalesce
from pyspark.sql.types import  IntegerType, DoubleType, LongType, FloatType

from awsglue.transforms import *
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

from utils import Library


class GlueLogger:
    
    def __init__(self, args):
        job_name = args['JOB_NAME'] if 'JOB_NAME' in args else 'UnknownJob'
        self.logger = logging.getLogger(job_name)
        self.logger.setLevel(logging.INFO)

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        self.logger.addHandler(handler)

    def info(self, message):
        caller_frame = inspect.stack()[1]
        func_name = caller_frame[3]
        line_num = caller_frame[2]
        caller_locals = caller_frame[0].f_locals
        caller_class = caller_locals.get('self', None)
        caller_class_name = caller_class.__class__.__name__ if caller_class else None
        log_message = f"Class: {caller_class_name} - Function: {func_name} - Line: {line_num}  - {message}"
        self.logger.info(log_message)

    def error(self, message):
        caller_frame = inspect.stack()[1]
        func_name = caller_frame[3]
        line_num = caller_frame[2]
        caller_locals = caller_frame[0].f_locals
        caller_class = caller_locals.get('self', None)
        caller_class_name = caller_class.__class__.__name__ if caller_class else None
        log_message = f"Function: {func_name} - Line: {line_num} - Class: {caller_class_name} - {message}"
        self.logger.error(log_message)

    def exception(self, message):
        caller_frame = inspect.stack()[1]
        func_name = caller_frame[3]
        line_num = caller_frame[2]
        caller_locals = caller_frame[0].f_locals
        caller_class = caller_locals.get('self', None)
        caller_class_name = caller_class.__class__.__name__ if caller_class else None
        log_message = f"Function: {func_name} - Line: {line_num} - Class: {caller_class_name} - {message}"
        self.logger.exception(log_message)


class Ambiente:

    def __init__(self, args, logger: GlueLogger):
        self.log = logger
        self.args = args
        self.LIB = Library()
        self.env_vars = self._load_env_vars() # Obtiene variables de LIB.secretManager
        self._validate_env_variables() # Valida que todas las variables esten disponibles
        self.job_name = args['JOB_NAME']
        self.event_date = args['date']
        self.table_name = args['table_name']

        
        self._validate_event_date()
                
    def _validate_event_date(self):
        self.log.info(f"Validando event_date {self.event_date} ...")

        resp =self.LIB.validateEvent(self.event_date)
        if not resp:
            raise ValueError('Event date is not valid')
        self.log.info(f" event_date {self.event_date} validado")
        return resp
        
    def _load_env_vars(self):
        """Carga variables desde AWS secret manager y las retorna como diccionario.

          Puedes agregar mas variables si lo requieres
        """
        try:
            self.log.info(f"Cargando variables de entorno desde AWS secretManager")
            values = {'BECH': self.LIB.secretManager("batch/datos_bech"),
                    'API_MAMBU': self.LIB.secretManager("batch/datos_mambu"),
                    'BECH_PRODUCT': self.LIB.secretManager("batch/datos_glue_bancoestado"),
                    'GLUE_CONNECT': self.LIB.secretManager("batch/datos_glue_connections"),
                    'GLUE_CUSTOM': self.LIB.secretManager("batch/datos_glue_customfields"),
                    'GLUE_ROUTES': self.LIB.secretManager("batch/datos_glue_awsrutes"),
                    'GLUE_CHANNELS': self.LIB.secretManager("batch/datos_glue_channels"),
                    'GLUE_BANCOESTADO': self.LIB.secretManager("batch/datos_glue_bancoestado"),
                    'PTS': self.LIB.secretManager("batch/datos_pts")}
            return values
        except Exception as e:
            raise ValueError(f"Error [Ambiente._load_env_vars]: {str(e)}")
    
    def _validate_env_variables(self):
        """Valida que todas las variables de entorno de aws no esten vacias al instanciar la clase"""
        self.log.info("Validando variables de entorno AWS...")
        missing_vars = [key for key, value in self.env_vars.items() if value is None]
        self.log.info("Variables de entorno AWS validas")
        if missing_vars:
            self.log.info(f"Error [Ambiente._validate_env_variables]: Missing or invalid AWS environment variables")
            raise ValueError(f"Error [Ambiente._validate_env_variables] Missing or invalid AWS environment variables: {str(missing_vars)}")
    
    def generate_name_routes_file(self, interface_name, file_extension='TXT', glue_route='FTP_OUTPUT'):
        """
          Establece la variable interface_routes con las rutas de la interfaz.
          
          Anteriormente:
                resp_routes_output = LIB.generateNameRoutesFile(BECH['AMB_NAME'],
                BECH['FILE_PATH'], GLUE_ROUTES['FTP_OUTPUT'], interface_output, 'TXT')

        Args:
            file_extension (str, optional): _description_. Defaults to 'TXT'.
            glue_route: Especifica la ruta. Por ejemplo: FTP_OUTPUT, FTP_INPUT
        """

        try:
            self.log.info(f"Generando name routes file")
            routes = self.LIB.generateNameRoutesFile(self.env_vars['BECH']['AMB_NAME'],
                                                                    self.env_vars['BECH']['FILE_PATH'],
                                                                    self.env_vars['GLUE_ROUTES'][glue_route],
                                                                    interface_name,
                                                                    file_extension) 
            self.log.info(f'Ambiente: Ruta archivo: {str(routes)}')
            return routes

        except Exception as e:
            raise ValueError(f"Error [Ambiente.generate_name_routes_file]: {str(e)}")
    
    @property
    def bucket_name(self):
        """Funcion que obtiene el nombre del bucket desde las variables de entorno de aws"""

        # multi región, sólo en producción
        if self.env_vars['BECH']['AMB_NAME'] == 'PRODUCCION':
            BUCKET_NAME = self.env_vars['BECH']['BUCKET_NAME_MRAP']
            prefix = "MRAP        :"
        else:
            BUCKET_NAME = self.env_vars['BECH']['BUCKET_NAME']
            prefix = "BUCKET_NAME :"
            
        self.log.info(f"accede al {str(prefix)}  {str(BUCKET_NAME)}")
        return BUCKET_NAME
     
    @property
    def data_connect_batch(self):
        """Obtiene variables desde AWS  para la conexion a la base de datos batch"""
        
        self.log.info("Obteniendo datos de conexion batch...")
        try:
            data_connect = {'DB_HOST': self.env_vars['GLUE_CONNECT']['DB_HOST_DMS'],
                            'DB_USER': self.env_vars['GLUE_CONNECT']['DB_USER_DMS'],
                            'DB_PASSWORD': self.env_vars['GLUE_CONNECT']['DB_PASSWORD_DMS'],
                            'DB_NAME': self.env_vars['GLUE_CONNECT']['DB_NAME_DMS_BECH']}
            formatted_data_connect = ', '.join([f"{key}: {value}" for key, value in data_connect.items() if key not in  ['DB_PASSWORD', 'DB_USER']])
            self.log.info(f'data_connect batch------------> {formatted_data_connect}')
            return data_connect
        except Exception as e:
            raise ValueError(f'Error [Ambiente.data_connect_batch]: {str(e)}')
    
    @property
    def data_connect_dms(self):
        """Obtiene variables desde AWS  para la conexion a la base de datos dms"""
        try:
            self.log.info("Obteniendo datos de conexion dms")
            data_connect = {'DB_HOST': self.env_vars['GLUE_CONNECT']['DB_HOST_DMS'],
                            'DB_USER': self.env_vars['GLUE_CONNECT']['DB_USER_DMS'],
                            'DB_PASSWORD': self.env_vars['GLUE_CONNECT']['DB_PASSWORD_DMS'],
                            'DB_NAME': self.env_vars['GLUE_CONNECT']['DB_NAME_DMS'] }
            formatted_data_connect = ', '.join([f"{key}: {value}" for key, value in data_connect.items() if key not in  ['DB_PASSWORD', 'DB_USER']])
            self.log.info(f'data_connect dms------------> {formatted_data_connect}')
            return data_connect
        except Exception as e:
            raise ValueError(f'Error [Ambiente.data_connect_dms]: {str(e)}')

        
class DataProcessor():

    def __init__(self, ambiente, logger: GlueLogger):
        self.log = logger
        self.ambiente = ambiente

    def generate_interface(self, db_results):

        self.log.info(f"Generando la interfaz en: {str(self.ambiente.interface_routes['route_tmp'])}")
        try:
            with open(self.ambiente.interface_routes['route_tmp'], "w") as f:
                self.log.info(f" routes: {self.ambiente.interface_routes} abierto")
                for count, row in enumerate(db_results, start=1):
                    salida = ';'.join(['0' if x in (None, '') else str(x) for x in row]) + ';'
                    
                    if count < len(db_results):
                        f.write(salida + '\r\n')
                    else:
                        f.write(salida)
                        
            self.log.info(f"Total registros procesados: {str(len(db_results))}")
            self.log.info(f"Fin de la generación de interfaz en: {str(self.ambiente.interface_routes['route_tmp'])}")
            return True
        
        except (IOError, NameError) as e:
            self.log.exception(f"Hubo un problema al generar la interfaz en: {self.ambiente.interface_routes.get('route_tmp', 'ruta desconocida')}")
            raise ValueError("Error [DataProcessor.generateInterface]: " + str(e))

    
class FileManager:
    """Clase Encargada de manejar funciones relacionadas a lectura y escritura de archivos"""
    def __init__(self, logger=GlueLogger):
        self.log = logger

    def set_ambiente(self, ambiente: Ambiente):
        self.ambiente = ambiente
        return self
    
    def check_file_exists(self, bucket_name, filepath):
        self.log.info("Validando archivo de entrada {filepath}")
        file_exists = self.ambiente.LIB.checkFileExistance(bucket_name, filepath)
        if file_exists:
          self.log.info("Archivo de entrada {filepath} existe")
          return True
        else:
          raise ValueError("[FileManager.check_file_exists] Error al obtener el archivo de entrada")
    
    def read_file_s3(self, bucket_name, filepath):
        self.check_file_exists(bucket_name=self.ambiente.bucket_name,
                               filepath=self.ambiente.interface_routes['route_s3'])
        
        file = self.ambiente.LIB.readFileS3(bucket_name, filepath)
        if not file:
          raise ValueError("[FileManager.read_file_s3] No se pudo cargar el archivo de entrada desde s3 o el archivo no existe")
        elif len(file) == 0:
          raise ValueError(f"[FileManager.read_file_s3] No se encontraron registros en el archivo de entrada {filepath}")
        return file
        
    def generate_empty_file(self): 
        try:
            self.log.info("[FileManager.generate_empty_file]: Generando archivo vacio")
            response = self.ambiente.LIB.generateEmptyFile(self.ambiente.interface_routes['route_tmp'])  
            if response:
                self.log.info('[FileManager.upload_file_to_aws]: Archivo vacio generado con éxito')
                return response
            else:
                raise ValueError('Problemas al subir el archivo vacio a AWS')
        except Exception as e:
            raise ValueError(f"Error [FileManager.generate_empty_file] {str(e)}")
           
    def upload_file_to_aws(self):
        """Funcion que sube archivos a AWS."""
        try:
            self.log.info('[FileManager.upload_file_to_aws]: Subiendo archivo a AWS')
            response = self.ambiente.LIB.uploadFileAWS(self.ambiente.interface_routes['route_tmp'],
                                                       self.ambiente.interface_routes['name_file'],
                                                       self.ambiente.bucket_name,
                                                       self.ambiente.interface_routes['route_s3'])
            if response:
                self.log.info('[FileManager.upload_file_to_aws]: Archivo subido con éxito')
                return response
            else:
                raise ValueError('Problemas al subir el archivo a AWS')
        except Exception as e:
            raise ValueError(f'Error [FileManager.upload_file_to_aws]: {str(e)}' )

    def readFileValidate (self, file, contents):
        """ Función que permite recorrer data obtenida de un archivo s3 """
        count = 0
        data_file = []
        try:
            if contents == '':
                self.log.info("lectura exitosa de " + str(count) + " registros, del archivo " + str(file))
                return data_file
            else:
                for line in contents:
                    linex = line.split(";")
                    count = count + 1
                    
                    if count == 1:
                        self.log.info('elimina cabecera')
                        pass
                    else:
                        data_file.append(linex)

                count = count - 1 # elimina 1 que es el count de la cabecera

                self.log.info("lectura exitosa de " + str(count) + " registros, del archivo " + str(file))
            
            return data_file
        except (IOError, NameError) as e:
            self.log.exception("problemas al abrir el archivo INPUT o NO EXISTE " + str(file))
            raise ValueError('FileManager.readFileValidate ERROR: ' + str(e))

    
class DatabaseManager:

    def __init__(self, logger=GlueLogger):
        self.log = logger
        
    def _worker(self, data_chunk: List[Tuple], query: str, data_connect: dict):
        """" 
        Metodo privado que se ejecuta en bulk_insert_with_threads que crea una conexion por cada insercion
        luego ejecuta executermany que permite insertar varios registros a la vez basado en una query
        """
        connection =self.connect_db(data_connect=data_connect)
        if connection:
            try:
                cursor = connection.cursor()
                cursor.executemany(query, data_chunk)
                connection.commit()
                self.log.info(f"Chunk insertado con éxito: {len(data_chunk)} registros.")
                
            except pymysql.err.IntegrityError as ine:
                raise ValueError(f"[ DBManager.worker]  pymysql.err.IntegrityError: {str(ine)}")

            except Exception as e:
                connection.rollback()
                raise ValueError(f"[ DBManager.worker] Error: {str(e)}")
            finally:
                connection.close()

    def divide_in_chunks(self, data: List[Tuple], chunk_size: int):
        """ Funcion que divide los datos en chunks o muestra de datos de un tamaño específico. """
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]
        
    def bulk_insert_or_update_with_threads(self, data: List[Tuple], query: str, data_connect: dict, chunk_size=15000, max_threads=100):

        self.log.info("<------- Insertando registros masivamente con hilos ------->")
        try:
            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                futures = [executor.submit(self._worker, chunk, query, data_connect) for chunk in self.divide_in_chunks(data, chunk_size)]
                for future in futures:
                    future.result()  
            return True
        except Exception as e:
            raise ValueError(f"[DatabaseManager.bulk_insert_or_update_with_threads] Error: {str(e)}")

    def connect_db(self, data_connect: dict):
        try:
            connection = pymysql.connect(
                host=data_connect['DB_HOST'],
                user=data_connect['DB_USER'],
                passwd=data_connect['DB_PASSWORD'],
                database=data_connect['DB_NAME']
            )
            self.log.info("Conexion exitosa a BD : " + str(data_connect['DB_NAME']))
            return connection
        except Exception as e:
            raise ValueError("Error [DatabaseManager.connectDB]: " + str(e))


class ResponseManager:
    def __init__(self, job_name: str, logger: GlueLogger):
        self.default_message = f'FIN PROCESO {job_name}'
        self.log = logger

    def success(self):
        self.log.info(f'{self.default_message} EXITOSO')
        return {'status': 200, 'code': 0, 'body': "OK"}
    
    def error(self, error_message: str = ""):
        self.log.exception(f'{self.default_message} ERROR: {error_message}')
        return {'status': 400, 'code': '-1', 'body': 'NOK'}


class PysparkClient:
    
    def __init__(self, ambiente: Ambiente, args, file_manager: FileManager, logger: GlueLogger):
        self.log = logger
        self.spark_context = SparkContext()
        self.glue_context = GlueContext(self.spark_context)
        self.spark_session = self.glue_context.spark_session
        self.job = Job(self.glue_context)
        self.job.init(ambiente.job_name, args)
        self.ambiente = ambiente
        self.jdbc_url = ""
        self.dms_connection_properties = None
        self.batch_connection_properties = None
        self.file_manager = file_manager

    def _build_jdbc_url(self, connection_info):
        """
        Retorna un string de conexion para pyspark a traves del driver mysql
        """
        port = connection_info.get('DB_PORT', '3306')
        return f"jdbc:mysql://{connection_info['DB_HOST']}:{port}/{connection_info['DB_NAME']}?zeroDateTimeBehavior=convertToNull"
    
    def _get_connection_properties(self, connection_type):
        """
        Obtiene las propiedades de conexión basadas en el tipo de conexión.

        :param connection_type: Tipo de conexión ('dms' o 'batch').
        :return: Propiedades de conexión.
        """
        if connection_type == 'dms':
            self._set_connection_dms()
            return self.dms_connection_properties
        elif connection_type == 'batch':
            self._set_connection_batch()
            return self.batch_connection_properties
        else:
            raise ValueError("Tipo de conexión desconocido: debe ser 'dms' o 'batch'")
    
    def _set_connection_dms(self):
        """
        Establece los datos de conexion basado en los valores del ambiente para la base de datos dms
        """
        try:
            connection_info = self.ambiente.data_connect_dms
            jdbc_url = self._build_jdbc_url(connection_info)
            self.dms_connection_properties = {
                "url": jdbc_url,
                "user": connection_info['DB_USER'],
                "password": connection_info['DB_PASSWORD'],
            }
        except Exception as e:
            raise ValueError(f"[PysparkClient.set_connection_dms] Could not set connection to dms: {str(e)}")

    def _set_connection_batch(self):
        """
        Establece los datos de conexion basado en los valores del ambiente para la base de datos batch
        """
        try:
            connection_info = self.ambiente.data_connect_batch
            jdbc_url = self._build_jdbc_url(connection_info)
            self.batch_connection_properties = {
                "url": jdbc_url,
                "user": connection_info['DB_USER'],
                "password": connection_info['DB_PASSWORD'],
            }
        except Exception as e:
            raise ValueError(f"[PysparkClient._set_connection_batch] Could not set connection to batch: {str(e)}")
          
    def load_dataframe_from_table(self, table_name, connection_type):
        """ 
          Carga los datos desde una tabla de la base de datos en un dataframe.
          Se debe especificar el tipo de base a la cual conectarse.
          Args:
            :param table_name: nombre de la tabla de la base de datos
            :param connection_type: Puede ser de tipo dms o batch
          return: Pyspark dataframe object        
        """
        try:
            self.log.info(f"GENERANDO DATAFRAME DESDE TABLA ------> {table_name}")

            connection_properties = self._get_connection_properties(connection_type)
            df = self.spark_session.read \
                .format("jdbc") \
                .option("url", connection_properties['url']) \
                .option("dbtable", table_name) \
                .option("user", connection_properties['user']) \
                .option("password", connection_properties['password']) \
                .option("numPartitions", "10") \
                .load()

            self.log.info(f"Dataframe generado con éxito ------> {table_name}")
            return df

        except Exception as e:
            raise ValueError(f"[PysparkClient.dataframe_from_table] Error al cargar DataFrame desde la tabla {table_name}: {str(e)}")

    def load_dataframe_from_query(self, query: str, connection_type: str):
        """ 
        Carga los datos desde una query SQL de la base de datos en un dataframe.
        Se debe especificar el tipo de base a la cual conectarse.
            Args:
                :param table_name: nombre de la tabla de la base de datos
                :param connection_type: Puede ser de tipo dms o batch
            return: Pyspark dataframe object        
        """
        self.log.info(f"GENERANDO DATAFRAME DESDE QUERY ")

        try:
            connection_properties = self._get_connection_properties(connection_type)
            df = self.spark_session.read \
                .format("jdbc") \
                .option("url", connection_properties['url']) \
                .option("user", connection_properties['user']) \
                .option("password", connection_properties['password']) \
                .option("dbtable", f"({query}) AS t") \
                .option("numPartitions", "10") \
                .load()
            self.log.info(f"Dataframe generado con éxito ")
            return df
        
        except Exception as e:
            raise ValueError(f"[PysparkClient.dataframe_from_query] Error al cargar DataFrame desde la consulta: {str(e)}")
    
    def dataframe_to_list_of_tuples(self, dataframe):
        """
            Transforma los datos de un dataframe de pyspark a una lista de tuplas,
            el cual es el mismo formato que una consulta SQL de pymysql, con el fin de que 
            se puedan leer y insertar con bulk insert de pymysql.
        """
        try:
            self.log.info("[PysparkClient.dataframe_to_list_of_tuples] Convirtiendo dataframe a lista de tuplas...")
            collected_data = dataframe.collect()
            data_as_tuples = [tuple(row) for row in collected_data]
            return data_as_tuples
        except Exception as e:
            raise ValueError(f"[PysparkClient.dataframe_to_list_of_tuples] Failed to convert {str(e)}")
    
    def write_dataframe_to_table(self, df, table_name, connection_type, overwrite=True):
        """
        Escribe un DataFrame en una tabla de la base de datos, con la opción de sobrescribir la tabla existente.
        Args:
            :param df: DataFrame de PySpark a escribir en la base de datos.
            :param table_name: Nombre de la tabla en la base de datos donde se escribirá el DataFrame.
            :param connection_type: Tipo de conexión ('dms' o 'batch').
            :param overwrite: Booleano que indica si se debe sobrescribir la tabla existente (True por defecto).
        """
        try:
            self.log.info(f"ESCRIBIENDO DATAFRAME EN LA TABLA ------> {table_name}")

            # Obtener las propiedades de conexión
            connection_properties = self._get_connection_properties(connection_type)

            # Modo de guardado
            mode = 'overwrite' if overwrite else 'append'

            # Escribir el DataFrame en la base de datos
            df.write \
            .format("jdbc") \
            .option("url", connection_properties['url']) \
            .option("dbtable", table_name) \
            .option("user", connection_properties['user']) \
            .option("password", connection_properties['password']) \
            .mode(mode) \
            .save()

            self.log.info(f"DataFrame escrito con éxito en la tabla ------> {table_name}")

        except Exception as e:
            raise ValueError(f"[PysparkClient.write_dataframe_to_table] Error al escribir DataFrame en la tabla {table_name}: {str(e)}")

    def create_dataframe_from_datalist(self, data: List, column_names: List):
        try:
            df = self.spark_session.createDataFrame(data, column_names)
            return df
        except Exception as e:
            self.logger.exception(f'Error al crear el dataframe: {str(e)}')
            raise ValueError(f'Error al crear el dataframe: {str(e)}')
    
    
class PysparkUtils:
    
    @staticmethod
    def replace_null_values_with_zero(df, list_of_columns: List[str]):
        try:
            for column in list_of_columns:
                if isinstance(df.schema[column].dataType, (IntegerType, DoubleType, LongType, FloatType)):
                    df = df.withColumn(column, coalesce(col(column), lit(0)))
            return df
        except Exception as e:
            raise ValueError(f"[PysparkUtils.convert_null_to_zero ERROR:{str(e)}")
    
    @staticmethod
    def replace_null_string_values_with_empty(df, list_of_columns: List[str]):
        try:
            for column in list_of_columns:
                if isinstance(df.schema[column].dataType, (StringType)):
                    df = df.withColumn(column, coalesce(col(column), lit(" ")))
            return df
        except Exception as e:
            raise ValueError(f"[PysparkUtils.replace_null_string_values_with_empty ERROR:{str(e)}")
    
    @staticmethod
    def mover_columna_a_ultima_posicion(df, nombre_columna):
      """ 
          Mueve una columna a la ultima posicion del dataframe con el fin de ser utilizado en update_masivo
      """
      try:
        columnas = [c for c in df.columns if c != nombre_columna]
        df = df.select(columnas + [col(nombre_columna)])
        return df
      except Exception as e:
        raise ValueError(f"[PysparkClient.mover_columna_a_ultima_posicion] Error al mover la columna: {str(e)}")
    

class SQLQueryManager:
    def __init__(self, ambiente: Ambiente, logger=GlueLogger):
        self.ambiente = ambiente
        self.log = logger

    def __validate_length_columns_equal_to_data(data, column_names):
        try:
            if not data:
                raise ValueError("[SQLQueryManager.__validate_length_columns_equal_to_data] --> La lista de datos está vacía")
            if not column_names:
                raise ValueError("[SQLQueryManager.__validate_length_columns_equal_to_data] --> La lista de nombres de columnas está vacía")
            if len(data[0]) != len(column_names):
                raise ValueError("[SQLQueryManager.__validate_length_columns_equal_to_data] --> La longitud de las filas de datos no coincide con el número de columnas")  
        except Exception as e:
            raise ValueError(f"[SQLQueryManager.__validate_length_columns_equal_to_data] --> Error {str(e)}")  
        
    def query_insert_masivo(self, table_name: str, column_names: List[str], data: List[Tuple[Any, ...]]) -> str:
       
        # Verificar que los datos tengan la misma cantidad de elementos que la lista de nombres de columnas
        try:
            SQLQueryManager.__validate_length_columns_equal_to_data(data=data, column_names=column_names)

            # Unir los nombres de las columnas para la consulta
            joined_column_names = ', '.join(column_names) # ej: id, name

            # Crear los marcadores de posición para los valores (%s)
            placeholders = ', '.join(['%s'] * len(column_names)) # ej: %s, %s, %s, %s, %s... etc
            # Construir la consulta SQL
            query = f"INSERT IGNORE INTO {table_name} ({joined_column_names}) VALUES ({placeholders});"
        
            self.log.info(f'[SQLQueryManager.query_insert_masivo] ---> {query}')  
            return query
        except Exception as e:
            raise ValueError(f"[SQLQueryManager.query_insert_masivo] --> Error {str(e)}")  
        
    def query_update_masivo(self, table_name: str, column_names: List[str], condition_column: str, data: List[Tuple[Any, ...]]) -> str:

        try:
            SQLQueryManager.__validate_length_columns_equal_to_data(data=data, column_names=column_names)
            
            # Se excluye la columna de condición de la cláusula SET
            set_columns = [col for col in column_names if col != condition_column]

            # Construir la parte de SET de la consulta SQL
            set_clause = ', '.join([f"{col} = %s" for col in set_columns])
            
            # Construir la consulta SQL con una cláusula WHERE dinámica
            query = f"UPDATE {table_name} SET {set_clause} WHERE {condition_column} = %s;"
            self.log.info(f'[SQLQueryManager.query_update_masivo] ---> {query}')
            return query
        except Exception as e:
            raise ValueError(f"[SQLQueryManager.query_insert_masivo] --> Error {str(e)}")  

    def query_upsert_masivo(self, table_name: str, column_names: List[str], unique_key: str, data: List[Tuple[Any, ...]]) -> str:
    
        try:
            # Verificar que los datos tengan la misma cantidad de elementos que la lista de nombres de columnas
            SQLQueryManager.__validate_length_columns_equal_to_data(data=data, column_names=column_names)

            # Unir los nombres de las columnas para la consulta
            joined_column_names = ', '.join(column_names)

            # Crear los marcadores de posición para los valores (%s)
            placeholders = ', '.join(['%s'] * len(column_names))

            # Crear la parte de la consulta para el INSERT
            insert_query = f"INSERT INTO {table_name} ({joined_column_names}) VALUES ({placeholders})"

            # Crear la parte de la consulta para el ON DUPLICATE KEY UPDATE
            update_parts = ', '.join([f"{col} = VALUES({col})" for col in column_names if col != unique_key])
            upsert_query = f"{insert_query} ON DUPLICATE KEY UPDATE {update_parts};"

            self.log.info(f'[SQLQueryManager.query_upsert_masivo] ---> {upsert_query}')  
            return upsert_query
        except Exception as e:
            raise ValueError(f"[SQLQueryManager.query_insert_masivo] --> Error {str(e)}")  


class DataManager:

    @staticmethod
    def obtener_data_con_pyspark(pyspark_client: PysparkClient, data_connect_type: str, query: str=None, table_name: str=None):
        try:
            if table_name:
                df = pyspark_client.load_dataframe_from_table(table_name=table_name, connection_type=data_connect_type)
                                                                
            elif query:
                df = pyspark_client.load_dataframe_from_query(query=query, connection_type=data_connect_type)   
            else:
                raise ValueError(f"obtener_data_con_pyspark ERROR: Se debe especificar query o table_name") 
            
            column_names = df.columns
            db_data = pyspark_client.dataframe_to_list_of_tuples(dataframe=df)
            return df, column_names, db_data
        
        except Exception as e:
            raise ValueError(f"obtener_data_con_pyspark ERROR: {str(e)}")      

    @staticmethod
    def insertar_datos_masivos(sql_query_manager: SQLQueryManager, db_manager: DatabaseManager,
                            data_connect: dict, column_names:List, data: List[Tuple], table_name: str):
        
        if not column_names or not data:
            raise ValueError('insertar_datos_masivos ERROR: No hay datos para insertar.')

        try:
            query_insert_masivo = sql_query_manager.query_insert_masivo(table_name=table_name,
                                                                        column_names=column_names,
                                                                        data=data)
            db_manager.bulk_insert_or_update_with_threads(data=data,
                                                         query=query_insert_masivo,
                                                         data_connect=data_connect)
        except Exception as e:
            raise ValueError(F'insertar_datos_masivos ERROR: {str(e)}')

    @staticmethod
    def actualizar_datos_masivos(sql_query_manager: SQLQueryManager, ambiente: Ambiente, db_manager: DatabaseManager, table_name: str,
                                data_connect: dict, column_names: List[str], data: List[Tuple], condition_column: str, is_upsert: bool= True ):
        
        """
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
                is_upsert: Determina si la consulta sera un upsert (insertar, sino actualizar)

            Ejemplo:
            si la data viene asi:
                data = [(num_cta, valo2, valo2 )]
            En donde 'num_cta' es la columna para la sentencia WHERE,
            deberas modificar tu consulta SELECT en donde traes los datos para que num_cta quede asi:
                data = [(valo1, valo2, num_cta )]
        """    
        if not column_names or not data:
            raise ValueError('actualizar_datos_masivos ERROR: Faltan datos o condiciones para la actualización.')
        try:
            if is_upsert:
                query_update = sql_query_manager.query_upsert_masivo(
                    table_name=table_name, 
                    unique_key=condition_column,
                    column_names=column_names, 
                    data=data)
                
            elif not is_upsert:
                query_update = sql_query_manager.query_update_masivo(
                    table_name=table_name, 
                    condition_column=condition_column,
                    column_names=column_names, 
                    data=data)
            
            db_manager.bulk_insert_or_update_with_threads(data=data, 
                                                        query=query_update, 
                                                        data_connect=data_connect) 
                                                        
        except Exception as e:
            raise ValueError(f'actualizar_datos_masivos ERROR: {str(e)}')

    @staticmethod
    def generate_files_upload_interface(data, ambiente, data_processor, file_manager, logger):
        """Genera interface si vienen datos, si no vienen datos, se genera un archivo vacio y luego se sube a AWS
            si ocurre un error al subir el archivo, se procedera a volver a intentar crear un archivo vacio y luego subirlo a AWS
        """
        try:
            if not data:
                file_manager.generate_empty_file()
            else:
                data_processor.generate_interface(db_results=data)
            file_manager.upload_file_to_aws()

        except Exception as e:
            logger.info('generate_files_upload_interface Error: al subir el archivo, generando un archivo vacío: ', str(e))
            file_manager.generate_empty_file()
            file_manager.upload_file_to_aws()


class Init:
    
    @staticmethod
    def load_args_from_aws(args: List):
        
        try:
            args = getResolvedOptions(sys.argv, args) 
            return args
        except Exception as e:
            raise ValueError(f'Error loading args {str(e)}')

    @staticmethod
    def inicializar_logger_y_response(args):
        try:
            logger = GlueLogger(args=args)
            process_response = ResponseManager(logger=logger, job_name=args.get('JOB_NAME', 'Unknown'))
            return logger, process_response
        except Exception as e:
            # Manejo de excepciones durante la inicialización
            logger.info(f"Error al inicializar logger y response: {e}")
            return None, None
    
    @staticmethod
    def inicializar_proceso(args, glue_logger):
        glue_logger.info('<--------- INICIALIZANDO PROCESO Y VALIDANDO VARIABLES: -------->')
        try:
            ambiente = Ambiente(args=args, logger=glue_logger)
            db_manager = DatabaseManager(logger=glue_logger)
            sql_query_manager = SQLQueryManager(ambiente=ambiente, logger=glue_logger)
            data_processor = DataProcessor(ambiente=ambiente, logger=glue_logger)
            file_manager = FileManager(logger=glue_logger).set_ambiente(ambiente=ambiente)
            pyspark_client = PysparkClient(ambiente=ambiente, args=args, file_manager=file_manager, logger=glue_logger)
            return ambiente, db_manager, sql_query_manager, data_processor, pyspark_client, file_manager
        except Exception as e:
            glue_logger.exception(e)
            raise ValueError(f"Error al inicializar el proceso: {str(e)}")
        