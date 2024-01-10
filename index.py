import sys
import calendar
import pymysql
from typing import Tuple, List, Any
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor

from pyspark.sql.types import StringType, FloatType
from pyspark.sql.functions import col, when, lit, udf, sum, avg, date_add, date_sub, datediff, date_format, current_date
from pyspark.sql.types import DateType, TimestampType

from awsglue.transforms import *
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

from Index import Library


class Ambiente:
    """
        Clase encargada de obtener, validar y establecer las variables
        de entorno obtenidas desde AWS y los argumentos del job
        
        1. Responsabilidad de clase:
            - Obtiene secretos de AWS, párametros del job y establece LIB.
            - Valida que estén todas las variables de entorno requeridas
            - Establece en variable los datos de conexión a bases de datos (data_connect_)
        2. Uso: Se debe instanciar en el main.
            ejemplo:
                ambiente = Ambiente(args=['JOB_NAME','date','table_name','interface_output'])
                1. acceder a librería
                    ambiente.LIB
                2. Acceder a datos de conexión base de datos:
                    data_connect_batch = ambiente.data_connect_batch
        3. Clase abierta a extension en caso de que se requiera agregar mas argumentos como
        event_date, event_date2, etc...
                
    """
    def __init__(self, args):
        print("Instanciando datos de ambiente...")
        self.args = args
        self.LIB = Library()
        self.env_vars = self._load_env_vars() # Obtiene variables de LIB.secretManager
        self._validate_env_variables() # Valida que todas las variables esten disponibles
        self.job_name = args['JOB_NAME']
        self.event_date = args['date']
        self.table_name = args['table_name']

        # Si necesitas un output file:
        self.interface_output_name = args['interface_output']
        self.interface_routes = self.generate_name_routes_file()
        
        # Si necesitas un input:
        # self.interface_input_name = args['input_file']
        # self.interface_routes_input = self.generate_name_routes_file(glue_route='FTP_INPUT')
        self._validate_event_date()
                
    def _validate_event_date(self):
        print(f"Validando event_date {self.event_date} ...")

        resp =self.LIB.validateEvent(self.event_date)
        if not resp:
            raise ValueError('Event date is not valid')
        print(f" event_date {self.event_date} validado")
        return resp
        
    def _load_env_vars(self):
        """Carga variables desde AWS secret manager y las retorna como diccionario.

          Puedes agregar mas variables si lo requieres
        """
        try:
            print(f"Cargando variables de entorno desde AWS secretManager")
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
            print(f"Error [Ambiente._load_env_vars]: {str(e)}")
            raise ValueError(f"Error [Ambiente._load_env_vars]: {str(e)}")
    
    def _validate_env_variables(self):
        """Valida que todas las variables de entorno de aws no esten vacias al instanciar la clase"""
        print("Validando variables de entorno AWS...")
        missing_vars = [key for key, value in self.env_vars.items() if value is None]
        print("Variables de entorno AWS validas")
        if missing_vars:
            print(f"Error [Ambiente._validate_env_variables]: Missing or invalid AWS environment variables")
            raise ValueError(f"Error [Ambiente._validate_env_variables] Missing or invalid AWS environment variables: {str(missing_vars)}")
    
    def generate_name_routes_file(self, file_extension='TXT', glue_route='FTP_OUTPUT'):
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
            print(f"Generando name routes file")
            routes = self.LIB.generateNameRoutesFile(self.env_vars['BECH']['AMB_NAME'],
                                                                    self.env_vars['BECH']['FILE_PATH'],
                                                                    self.env_vars['GLUE_ROUTES'][glue_route],
                                                                    self.interface_output_name,
                                                                    file_extension) 
            print(f'Ambiente: Ruta archivo: {str(routes)}')

            return routes

        except Exception as e:
            print(f"Error [Ambiente.generate_name_routes_file]: {str(e)}")
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
            
        print("accede al " + str(prefix) + str(BUCKET_NAME))
        return BUCKET_NAME
     
    @property
    def data_connect_batch(self):
        """Obtiene variables desde AWS  para la conexion a la base de datos batch"""
        
        print("Obteniendo datos de conexion batch...")
        try:
            data_connect = {'DB_HOST': self.env_vars['GLUE_CONNECT']['DB_HOST_DMS'],
                            'DB_USER': self.env_vars['GLUE_CONNECT']['DB_USER_DMS'],
                            'DB_PASSWORD': self.env_vars['GLUE_CONNECT']['DB_PASSWORD_DMS'],
                            'DB_NAME': self.env_vars['GLUE_CONNECT']['DB_NAME_DMS_BECH']}
            formatted_data_connect = ', '.join([f"{key}: {value}" for key, value in data_connect.items()])
            print('data_connect batch------------> ', formatted_data_connect)
            return data_connect
        except Exception as e:
            raise ValueError(f'Error [Ambiente.data_connect_batch]: {str(e)}')
    
    @property
    def data_connect_dms(self):
        """Obtiene variables desde AWS  para la conexion a la base de datos dms"""
        try:
            print("Obteniendo datos de conexion dms")
            data_connect = {'DB_HOST': self.env_vars['GLUE_CONNECT']['DB_HOST_DMS'],
                            'DB_USER': self.env_vars['GLUE_CONNECT']['DB_USER_DMS'],
                            'DB_PASSWORD': self.env_vars['GLUE_CONNECT']['DB_PASSWORD_DMS'],
                            'DB_NAME': self.env_vars['GLUE_CONNECT']['DB_NAME_DMS'] }
            formatted_data_connect = ', '.join([f"{key}: {value}" for key, value in data_connect.items()])
            print('data_connect dms------------> ', formatted_data_connect)
            return data_connect
        except Exception as e:
            raise ValueError(f'Error [Ambiente.data_connect_dms]: {str(e)}')

        
class DataProcessor():
    """"Clase encargada de procesar archivos como generar la interface en AWS
        o transformar filas de datos uno a uno.
    """
    def __init__(self, ambiente):
        print("Instanciando DataProcessor...")
        self.ambiente = ambiente

    def _format_db_row(self, db_row, event_date=None):
        """
        Función que transforma los datos de una fila desde una consulta a la base de datos
        Args:
            row (tuple): tupla de registros de la base de datos.
            event_date (str): Fecha en formato AAAAMMDD del evento.
        Returns (tuple): Tupla con los registros transformados y ordenados para generar la interfaz.
        Error: Retorna error y es capturado en main().
        """
        try:
            # VALIDACIONES DE LOS CAMPOS
            # TODO
            
            export_structure = {
                # ACA DEBE IR LA DATA COMO DICCIONARIO 
                # TODO

            }
            
            # Convertir valores del diccionario en tupla ordenada
            return tuple(export_structure.values())
        except Exception as e:
            raise ValueError(f'Error [DataProcessor.format_db_row]: {str(e)}')

    def generate_interface(self, db_results, event_date):
        """
        Escribe resultados formateados en un archivo, manejando rutas y excepciones.
        
        Parámetros:
        - db_results (list[tuple]): Resultados de una consulta de base de datos a escribir en el archivo .
        - routes (dict): Contiene rutas, se espera una clave 'route_tmp'.
        - load: No utilizado en la función.
        - event_date (string AAAAMMDD): Fecha recibida desde la api.
        
        Retorna:
        - bool: True si la escritura es exitosa, None si hay una excepción o error.
        
        Nota:
        """
        print(f"Generando la interfaz en: {str(self.ambiente.interface_routes['route_tmp'])}")
        try:
            with open(self.ambiente.interface_routes['route_tmp'], "w") as f:
                print(f" routes: {self.ambiente.interface_routes} abierto")
                for count, row in enumerate(db_results, start=1):
    
                    # Procesar la fila de la base de datos ( viene en formato de tupla)
                    #formatted_row = self._format_db_row(db_row=row, event_date=event_date)
                    #if not formatted_row:
                    #    return
                    # Convertir cada valor de la tupla a cadena y reemplazar con 0
                    salida = ';'.join(['0' if x in (None, '') else str(x) for x in row]) + ';'
                    
                    if count < len(db_results):
                        f.write(salida + '\r\n')
                    else:
                        f.write(salida)
                        
            print(f"Total registros procesados: {str(len(db_results))}")
            print(f"Fin de la generación de interfaz en: {str(self.ambiente.interface_routes['route_tmp'])}")
            return True
        
        except (IOError, NameError) as e:
            print(f"Hubo un problema al generar la interfaz Test en: {self.ambiente.interface_routes.get('route_tmp', 'ruta desconocida')}")
            print("Error [DataProcessor.generateInterface]: " +  str(e))
            raise ValueError("Error [DataProcessor.generateInterface]: " + str(e))

    
class FileManager:
    """Clase Encargada de manejar funciones relacionadas a lectura y escritura de archivos"""
    def __init__(self):
        print("Instanciando FileManager...")

    def set_ambiente(self, ambiente: Ambiente):
        self.ambiente = ambiente
        return self
    
    def check_file_exists(self, bucket_name, filepath):
        print("Validando archivo de entrada {filepath}")
        file_exists = self.ambiente.LIB.checkFileExistance(bucket_name, filepath)
        if file_exists:
          print("Archivo de entrada {filepath} existe")
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
            print("[FileManager.generate_empty_file]: Generando archivo vacio")
            response = self.ambiente.LIB.generateEmptyFile(self.ambiente.interface_routes['route_tmp'])  
            if response:
                print('[FileManager.upload_file_to_aws]: Archivo vacio generado con éxito')
                return response
            else:
                print('Problemas al subir el archivo vacio a AWS')
                raise ValueError('Problemas al subir el archivo vacio a AWS')
        except Exception as e:
            print(f"Error [FileManager.generate_empty_file]: { str(e)}")
            raise ValueError(f"Error [FileManager.generate_empty_file] {str(e)}")
           
    def upload_file_to_aws(self):
        """Funcion que sube archivos a AWS."""
        try:
            print('[FileManager.upload_file_to_aws]: Subiendo archivo a AWS')

            response = self.ambiente.LIB.uploadFileAWS(self.ambiente.interface_routes['route_tmp'],
                                                       self.ambiente.interface_routes['name_file'],
                                                       self.ambiente.bucket_name,
                                                       self.ambiente.interface_routes['route_s3'])
            if response:
                print('[FileManager.upload_file_to_aws]: Archivo subido con éxito')
                return response
            else:
                raise ValueError('Problemas al subir el archivo a AWS')
        except Exception as e:
            print('Error [FileManager.upload_file_to_aws]:', str(e))
            raise ValueError(f'Error [FileManager.upload_file_to_aws]: {str(e)}' )

    def readFileValidate (self, file, contents):
        """ Función que permite recorrer data obtenida de un archivo s3 """

        count = 0
        data_file = []

        try:
            if contents == '':
                print("lectura exitosa de " + str(count) + " registros, del archivo " + str(file))
                return data_file
            else:
                for line in contents:
                    linex = line.split(";")
                    count = count + 1
                    
                    if count == 1:
                        print('elimina cabecera')
                        pass
                    else:
                        data_file.append(linex)

                count = count - 1 # elimina 1 que es el count de la cabecera

                print("lectura exitosa de " + str(count) + " registros, del archivo " + str(file))
            
            return data_file
        except (IOError, NameError) as e:
            print('error: ' + str(e))
            print("problemas al abrir el archivo INPUT o NO EXISTE " + str(file))
            raise ValueError('FileManager.readFileValidate ERROR: ' + str(e))

    
class DatabaseManager:
    """
        Clase encargada de gestionar conexion a base de datos,
        ejecutar commits, insertar datos masivos
    """
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
                print(f"Chunk insertado con éxito: {len(data_chunk)} registros.")
                
            except pymysql.err.IntegrityError as ine:
                print(f" [ DBManager.worker] Error de integridad: {ine}")
                raise ValueError(f"[ DBManager.worker] Error: {str(ine)}")

            except Exception as e:
                connection.rollback()
                print(f"[ DatabaseManager.worker] Error detallado:", e.__class__, ":", e)

                raise ValueError(f"[ DBManager.worker] Error: {str(e)}")
            finally:
                connection.close()

    def divide_in_chunks(self, data: List[Tuple], chunk_size: int):
        """ Funcion que divide los datos en chunks o muestra de datos de un tamaño específico. """
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]
        
    def bulk_insert_or_update_with_threads(self, data: List[Tuple], query: str, data_connect: dict, chunk_size=15000, max_threads=100):
        """Funcion que inserta varios registros a la vez con hilos a una base de datos
           la query se debe generar utilizando SQLQueryManager.query_insert_masivo() o query_update_masivo()
        """
        print("<------- Insertando registros masivamente con hilos ------->")
        try:
            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                futures = [executor.submit(self._worker, chunk, query, data_connect) for chunk in self.divide_in_chunks(data, chunk_size)]
                for future in futures:
                    future.result()  
            return True
        except Exception as e:
            raise ValueError(f"[DatabaseManager.bulk_insert_or_update_in_chunks] Error: {str(e)}")

    def bulk_insert_or_update_in_chunks(self, data: List[Tuple], query: str, conne, chunk_size=15000):
        print("<------- Insertando registros masivamente sin hilos ------->")

        for chunk in self.divide_in_chunks(data, chunk_size):
            try:
                cursor = conne.cursor()
                cursor.executemany(query, chunk)
                conne.commit()
            except pymysql.err.InterfaceError as ie:
                print(f"Error de interfaz en la base de datos: {ie}")
                conne.ping(reconnect=True)
                cursor = conne.cursor()
                # Reintenta insercion
                cursor.executemany(query, chunk)
                conne.commit()
            except pymysql.err.IntegrityError:
                print(f"Error campop duplicado: {ie}")
                continue
            
            except Exception as e:
                print("Error detallado:", e.__class__, ":", e)
                print('Error en bulk insert => ' + str(e))
                conne.rollback()
                raise ValueError(f"[DatabaseManager.bulk_insert_or_update_in_chunks] Error: {str(e)}")
        return True

    @staticmethod
    def connect_db(data_connect: dict):
        try:
            connection = pymysql.connect(
                host=data_connect['DB_HOST'],
                user=data_connect['DB_USER'],
                passwd=data_connect['DB_PASSWORD'],
                database=data_connect['DB_NAME']
            )
            print("Conexion exitosa a BD : " + str(data_connect['DB_NAME']))
            return connection
        except Exception as e:
            raise ValueError("Error [DatabaseManager.connectDB]: " + str(e))

    @staticmethod
    def execute_SQL_select(query: str, data_connect: dict):
        """ 
            Función que ejecuta una consulta a la base de datos de tipo SELECT
            y devuelve los datos junto con los nombres de las columnas 
            
            Return: 
                0: List[Tuple()]: Lista de tuplas con los datos de la base de datos
                1: List: Lista con los nombre de las columnas de la consulta slq ordenadas
        """

        try:
            connection = DatabaseManager.connect_db(data_connect=data_connect)
            print("Query de ejecución " + str(query))
            cursor = connection.cursor()
            cursor.execute(query)
            column_names = [column[0] for column in cursor.description]

            db_response = cursor.fetchall()
            print('<------------ REGISTROS OBTENIDOS DE LA BASE DE DATOS CON PYMYSQL ---------------->')
            print(f"Imprimiendo primeros 100 registros ----> {str(db_response[:100])}")
            if len(db_response) == 0:
                print(f'no se encontró información en la base de datos con la query {query}.')
                connection.close()
                return [], []

            connection.close()
            return db_response, column_names

        except Exception as e:
            if connection:
                connection.close()
            print('Error [DBManager.execute_SQL_select] => ' + str(e))
            raise ValueError("Error [DatabaseManager.execute_SQL_select]: " + str(e))

    @staticmethod
    def execute_SQL_commit (query: str, data_connect: dict):
        """ Función que ejecuta una consulta a la base de datos de tipo insert, update o delete para un solo registro"""
        try:
            connection = DatabaseManager.connect_db(data_connect=data_connect)
            print("Query de ejecución " + str(query))
            cursor = connection.cursor()
            cursor.execute(query)
            connection.commit()
            db_response = cursor.fetchall()
            
            print(f" db response ----> {str(db_response)}")
            connection.close()
            return db_response
        
        except Exception as e:
            connection.close()
            print('Error [execute_SQL_commit] => ' + str(e))
            raise ValueError("Error [DatabaseManager.execute_SQL_commit]: " + str(e))


class ResponseManager:
    def __init__(self, job_name: str):
        self.default_message = f'FIN PROCESO {job_name}'

    def success(self):
        print(f'{self.default_message} EXITOSO')
        return {'status': 200, 'code': 0, 'body': "OK"}
    
    def error(self, error_message: str = ""):
        print(f'{self.default_message} ERROR: {error_message}')
        return {'status': 400, 'code': '-1', 'body': 'NOK'}


class PysparkClient:
    
    def __init__(self, ambiente: Ambiente, args, file_manager: FileManager):
        print("Initializing Pyspark client")
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
    
    def _transformar_columnas_fecha(self, df):
        """ Examina cada campo en el esquema del DataFrame y si tiene fecha invalida al transforma"""
        try:
          for campo in df.schema.fields:
              if isinstance(campo.dataType, (DateType, TimestampType)):
                  df = df.withColumn(campo.name, when(col(campo.name).isNull() | (col(campo.name) < "1000-01-01"), "1900-01-01").otherwise(col(campo.name)))
          return df
        except Exception as e:
            raise ValueError(f"[PysparkClient.mover_columna_a_ultima_posicion] Error al transformar a fecha: {str(e)}")
      
    def mover_columna_a_ultima_posicion(self, nombre_columna):
      """ 
          Mueve una columna a la ultima posicion del dataframe con el fin de ser utilizado en update_masivo
      """
      try:
        columnas = [c for c in df.columns if c != nombre_columna]
        df = df.select(columnas + [col(nombre_columna)])
        return df
      except Exception as e:
        raise ValueError(f"[PysparkClient.mover_columna_a_ultima_posicion] Error al mover la columna: {str(e)}")
          
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
            print(f"GENERANDO DATAFRAME DESDE TABLA ------> {table_name}")

            connection_properties = self._get_connection_properties(connection_type)
            df = self.spark_session.read \
                .format("jdbc") \
                .option("url", connection_properties['url']) \
                .option("dbtable", table_name) \
                .option("user", connection_properties['user']) \
                .option("password", connection_properties['password']) \
                .option("numPartitions", "10") \
                .load()

            print(f"Dataframe generado con éxito ------> {table_name}")
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
        print(f"GENERANDO DATAFRAME DESDE QUERY ------> {query}")

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
            print(f"Dataframe generado con éxito ------> {query} ")
            return df
        
        except Exception as e:
            raise ValueError(f"[PysparkClient.dataframe_from_query] Error al cargar DataFrame desde la consulta: {str(e)}")

    def load_dataframe_from_txt_file(self, s3_path, has_headers=True):
        """
        Funcion que lee un archivo .txt desde S3 y lo carga como un dataframe.
        'has_headers' especifica si el archivo incluye nombre de columnas en la primera linea
        """
        print(f"CARGANDO DATAFRAME DESDE ARCHIVO DESDE S3 ------> {s3_path}")
        try:
          if self.file_manager.check_file_exists(bucket_name=self.ambiente.bucket_name, 
                                                 filepath=s3_path):
            s3_path = f's3://{self.ambiente.bucket_name}/{s3_path}'
            df = self.spark_session.read.option("delimiter", ";").csv(s3_path, header=has_headers)
            return df
          else:
            raise ValueError(f"[PysparkClient.load_dataframe_from_txt_file] El archvivo : {s3_path} no existe")

        except Exception as e:
            raise ValueError(f"[PysparkClient.load_dataframe_from_txt_file] Error al cargar DataFrame desde el archivo: {str(e)}")

    def dataframe_to_list_of_tuples(self, dataframe):
        """
            Transforma los datos de un dataframe de pyspark a una lista de tuplas,
            el cual es el mismo formato que una consulta SQL de pymysql, con el fin de que 
            se puedan leer y insertar con bulk insert de pymysql.
        """
        try:
            print("[PysparkClient.dataframe_to_list_of_tuples] Convirtiendo dataframe a lista de tuplas...")
            collected_data = dataframe.collect()
            data_as_tuples = [tuple(row) for row in collected_data]
            return data_as_tuples
        except Exception as e:
            print("[PysparkClient.dataframe_to_list_of_tuples] Error al convertir el dataframe a lista tuplas")
            raise ValueError(f"[PysparkClient.dataframe_to_list_of_tuples] Failed to convert {str(e)}")
        
        
class ValidatorManager:
    """ Clase para validar todo tipo de variable relacionada a reglas de negocio"""
    pass


class Utils:
    """Clase con funciones para se reutilizadas en distintos jobs"""
    
    @staticmethod
    def _es_dia_habil(fecha: datetime.date, feriados: List[datetime.date]) -> bool:
        """
        Valida si una fecha es un día hábil o inhabil
        """
        if fecha.weekday() >= 5: 
            return False
        if fecha in feriados:
            return False
        return True
    
    @staticmethod
    def agregar_dias_habiles(fecha_inicial: datetime.date, dias: int, feriados: List[datetime.date]) -> datetime.date:
        """
        Agrega un número especificado de días hábiles a una fecha dada.

        Esta función toma una fecha inicial y agrega un número específico de días hábiles, 
        teniendo en cuenta los días feriados proporcionados. Un día hábil se define como 
        cualquier día que no sea sábado, domingo o un día feriado especificado.

        Parámetros:
        - fecha_inicial (datetime.date): La fecha desde la cual comenzar a contar.
        - dias (int): El número de días hábiles a agregar.
        - feriados (List[datetime.date]): Una lista de fechas que deben considerarse como días no hábiles.

        Retorna:
        - datetime.date: La fecha resultante después de agregar los días hábiles.

        Ejemplo de uso:
            fecha_resultante = Utils.agregar_dias_habiles(date(2021, 1, 1), 5, [date(2021, 1, 6)])
            # Esto agregará 5 días hábiles a partir del 1 de enero de 2021, excluyendo el 6 de enero de 2021 si es feriado.
        """
        fecha = fecha_inicial - timedelta(days=1)   
        while dias > 0:
            fecha += timedelta(days=1)
            if Utils._es_dia_habil(fecha, feriados):
                dias -= 1
        return fecha

    @staticmethod
    def dias_habiles_entre(fecha_inicio: datetime.date, fecha_fin: datetime.date, feriados: List[datetime.date]):
        """
        Funcion que calcula los dias habiles que hay entre 2 fechas
        No se consideran dias feriados ni sabado y domingo (solo dias habiles)
        Args: 
            fecha_inicio: Fecha desde, en formato datetime.date
            fecha_fin: Fecha hasta, en formato datetime.date
            feriados: Listado de objetos datetime.date
        Retuns (int):  Cantidad de dias habiles entre 2 fechas
        """
        dias = (fecha_fin - fecha_inicio).days
        dias_habiles = 0
        
        for i in range(dias + 1):  # Añadir 1 para incluir también el último día
            fecha = fecha_inicio + timedelta(days=i)
            
            if Utils._es_dia_habil(fecha, feriados):
                dias_habiles += 1
        
        return dias_habiles


class SQLQueryManager:
    """ 
        Clase que tiene como responsabilidad almacenar 
        todas las query de tipo string que se requieran para el proceso.
        Deben ser metodos estaticos en donde se deben acceder : SQLQueryManager.nombre_query()
    """
    
    @staticmethod
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
        
    @staticmethod
    def query_holidays ():
        """ Función que contiene la query para obtener los feriados """

        return  f"""SELECT DATE_FORMAT(CONCAT(YEAR, '-', MONTHOFYEAR, '-', DAYOFMONTH),'%Y-%m-%d')
                      AS holidays,NAME AS name
                      FROM holiday;
                """
    
    @staticmethod
    def query_insert_masivo(table_name: str, column_names: List[str], data: List[Tuple[Any, ...]]) -> str:
        """ 
          Función que devuelve la consulta SQL de inserción para una tabla validando que la cantidad de largo de datos sea igual al numero de columnas.
          
          :param table_name: Nombre de la tabla en la base de datos.
          :param column_names: Lista de nombres de columnas en la tabla.
          :param data: Lista de tuplas, donde cada tupla contiene los valores a insertar en las columnas.
          :return: La consulta SQL generada  en formato string.

          Ejemplo:                               
                  "SELECT id, name FROM table_name" --> [(1,  2),   (2, 3),]
                  columns order: --------------> ['id', 'name']
          IMPORANTE: 
                  El orden de los datos deben coincidir con el orden de las columnas.
                  Si los datos son un select de una base de datos van a venir: id, name


        """
        # Verificar que los datos tengan la misma cantidad de elementos que la lista de nombres de columnas
        try:
            SQLQueryManager.__validate_length_columns_equal_to_data(data=data, column_names=column_names)

            # Unir los nombres de las columnas para la consulta
            joined_column_names = ', '.join(column_names) # ej: id, name

            # Crear los marcadores de posición para los valores (%s)
            placeholders = ', '.join(['%s'] * len(column_names)) # ej: %s, %s, %s, %s, %s... etc
            # Construir la consulta SQL
            query = f"INSERT IGNORE INTO {table_name} ({joined_column_names}) VALUES ({placeholders});"
        
            print(f'[SQLQueryManager.query_insert_masivo] ---> {query}')  
            return query
        except Exception as e:
            raise ValueError(f"[SQLQueryManager.query_insert_masivo] --> Error {str(e)}")  
        
    @staticmethod
    def query_update_masivo(table_name: str, column_names: List[str], condition_column: str, data: List[Tuple[Any, ...]]) -> str:
        """
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
        """
        try:
            SQLQueryManager.__validate_length_columns_equal_to_data(data=data, column_names=column_names)
            
            # Se excluye la columna de condición de la cláusula SET
            set_columns = [col for col in column_names if col != condition_column]

            # Construir la parte de SET de la consulta SQL
            set_clause = ', '.join([f"{col} = %s" for col in set_columns])
            
            # Construir la consulta SQL con una cláusula WHERE dinámica
            query = f"UPDATE {table_name} SET {set_clause} WHERE {condition_column} = %s;"
            print(f'[SQLQueryManager.query_update_masivo] ---> {query}')
            return query
        except Exception as e:
            raise ValueError(f"[SQLQueryManager.query_insert_masivo] --> Error {str(e)}")  


    @staticmethod
    def query_upsert_masivo(table_name: str, column_names: List[str], unique_key: str, data: List[Tuple[Any, ...]]) -> str:
        """
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


        """
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

            print(f'[SQLQueryManager.query_upsert_masivo] ---> {upsert_query}')  
            return upsert_query
        except Exception as e:
            raise ValueError(f"[SQLQueryManager.query_insert_masivo] --> Error {str(e)}")  


class JobFunction():
    """Clase encargada de manejar las funciones de logica de negocio. 
        Las funciones pueden ser de tipo static para acceder como: JobFunction.nombre_funcion()
    """
    
    def __init__(self, ambiente: Ambiente = None, query_manager: SQLQueryManager = None):
        """Si la funcion se manejara en esta clase como privada y requiere datos del ambiente,
            puedes acceder a estos datos desde la misma clase. Por defecto no requiere ambiente ni querymanager.
            
            Si la funcion requiere acceder a cualquier otra clase, debes extender
            la clase en el metodo init para aceptar esas clases
        """
        self.ambiente = ambiente
    
    # Aca funciones del job con el decorador "@staticmethod"
    
    
def inicializar_proceso(args):
    print('<--------- INICIALIZANDO PROCESO Y VALIDANDO VARIABLES: -------->')
    ambiente = Ambiente(args=args)
    db_manager = DatabaseManager()
    sql_query_manager = SQLQueryManager()
    data_processor = DataProcessor(ambiente=ambiente)
    file_manager = FileManager().set_ambiente(ambiente=ambiente)
    pyspark_client = PysparkClient(ambiente=ambiente, args=args, file_manager=file_manager)
    job_functions = JobFunction()
    process_response =ResponseManager(job_name=ambiente.job_name)
    return ambiente, db_manager, sql_query_manager, data_processor, process_response, pyspark_client, file_manager, job_functions


def obtener_data_con_pymysql(data_connect: dict, query: str=None):
               
    """Obtiene los datos de la base de datos basado en los datos de conexion declarados.
         
    Args:
        data_connect: diccionario con los datos de conexion obtenidos de la clase Ambiente()
        query (str, optional): Para mysql es requerida la query.

    Returns:
        2 listas: 
            lista1: Lista de tuplas con los registros de la base de datos: [(valor1, valo2, ...), (valo1, valo2, ...)]
            lista2: Lista con los nombres de las columnas ordenadas consulta select de la query: [column1, column2, etc...]
    """
    print('<--------- OBTENIENDO DATOS: -------->')
    try:
        db_manager = DatabaseManager()
        db_data, column_names = db_manager.execute_SQL_select(query=query, data_connect=data_connect)                                                    
        print(f'<--------- COLUMN NAMES MYSQL: --------> \n {column_names}')
        return db_data, column_names
          
    except Exception as e:
        raise ValueError(f"obtener_data_con_pymysql ERROR: {str(e)}")


def obtener_data_con_pyspark(pyspark_client: PysparkClient, data_connect_type: str, query: str=None, table_name: str=None):

    """Obtiene los datos de la base de datos utilizando pyspark basado en la conexion de datos que se obtiene de la clase ambiente.
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
    """
    print('<--------- OBTENIENDO DATOS DESDE PYSPARK: -------->')                     
    try:
        if table_name:
            df = pyspark_client.load_dataframe_from_table(table_name=table_name, connection_type=data_connect_type)
                                                            
        elif query:
            df = pyspark_client.load_dataframe_from_query(query=query, connection_type=data_connect_type)   
        else:
            raise ValueError(f"obtener_data_con_pyspark ERROR: Se debe especificar query o table_name") 
           
        column_names = df.columns
        db_data = pyspark_client.dataframe_to_list_of_tuples(dataframe=df)

        print(f'<--------- COLUMN NAMES PYSPARK: --------> \n {column_names}')
        print('<--------- DF FROM TABLE: -------->')
        df.show()
        print('<--------- DATA PYSPARK (PRIMEROS 100 REGISTROS): -------->')
        print(db_data[:100])

        return df, column_names, db_data
    except Exception as e:
        raise ValueError(f"obtener_data_con_pyspark ERROR: {str(e)}")      


def insertar_datos_masivos(sql_query_manager: SQLQueryManager, db_manager: DatabaseManager,
                           data_connect: dict, column_names:List, data: List[Tuple], table_name: str):
    
  print('<--------- INSERTANDO DATOS MASIVOS: -------->')
  print(data_connect)
  print(column_names)
  print(data)
  print(table_name)

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


def actualizar_datos_masivos(sql_query_manager: SQLQueryManager, ambiente: Ambiente, db_manager: DatabaseManager,
                             data_connect: dict, column_names: List[str], data: List[Tuple], condition_column: str, is_upsert: bool= True):
    
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

    print('<--------- ACTUALIZANDO DATOS MASIVOS: -------->')
    
    if not column_names or not data:
        raise ValueError('actualizar_datos_masivos ERROR: Faltan datos o condiciones para la actualización.')

    try:
        
        if is_upsert:
            query_update = sql_query_manager.query_upsert_masivo(
                table_name=ambiente.table_name, 
                unique_key=condition_column,
                column_names=column_names, 
                data=data)
            
        elif not is_upsert:
            query_update = sql_query_manager.query_update_masivo(
                table_name=ambiente.table_name, 
                condition_column=condition_column,
                column_names=column_names, 
                data=data)
          
        db_manager.bulk_insert_or_update_with_threads(data=data, 
                                                      query=query_update, 
                                                      data_connect=data_connect) 
                                                      
    except Exception as e:
        raise ValueError(f'actualizar_datos_masivos ERROR: {str(e)}')


def generate_files_upload_interface(data, ambiente, data_processor, file_manager):
    """Genera interface si vienen datos, si no vienen datos, se genera un archivo vacio y luego se sube a AWS
        si ocurre un error al subir el archivo, se procedera a volver a intentar crear un archivo vacio y luego subirlo a AWS
    """
    print('<--------- GENERANDO INTERFACE: -------->')
    try:
        if not data:
            file_manager.generate_empty_file()
        else:
            data_processor.generate_interface(db_results=data, event_date=ambiente.event_date)
        file_manager.upload_file_to_aws()

    except Exception as e:
        print('generate_files_upload_interface Error: al subir el archivo, generando un archivo vacío: ', str(e))
        file_manager.generate_empty_file()
        file_manager.upload_file_to_aws()


def main():
    """
        En este docstring la idea es comentar lo que hace el job junto con el requerimiento:
    
        Este Job tiene la finalidad de calcular los montos xxx. Para calcular los montos se utiliza la funcion 
        dentro de la clase JobFunctions.calcular_montos(monto, otro_valor).
        Se utiliza la query que esta en SQLQueryManager.query_obtiene_montos(table_name), esta query va a la batch y hace join con mambu, etc.
        
        Se deben obtener los datos para luego procesarlos y generar un archivo de salida ( o insertarlos en una tabla y generar un archivo de salida vacio)
        ETC...
        
        link de historia de usuario JIRA: https://...
    """
    try:
        # ------------------------------------------------- 0 Cargar y validar secretos -----------------------------------------------------------------------
        args = getResolvedOptions(sys.argv, ['JOB_NAME', 'date', 'table_name', 'table_name_proy', 'interface_output']) # ESPECIFICA aca nuevas variables de args
        ambiente, db_manager, sql_query_manager, data_processor, process_response, pyspark_client, file_manager, job_functions = inicializar_proceso(args)
        
        # Declara aca mas atributos de la clase ambiente asociados a los args del job glue como por ejemplo:
        # ambiente.table_name_2 = args['table_name_2']        

        # -------------------------------------------------- 1 OBTENER QUERIES SQL -------------------------------------------------------------------------
        
        
        
        # -------------------------------------------------- 2 CARGAR DATOS ---------------------------------------------------------------------------------

        
        
        ## ------------------------------------------------- 3 TRANSFORMACION DE DATOS -----------------------------------------------------------------------
 
 
 
        ## ------------------------------------------------- 4 INSERTAR DATOS -----------------------------------------------------------------------------
        # insertar_datos_masivos(sql_query_manager,
        #                         db_manager,
        #                         ambiente.data_connect_batch, 
        #                         column_names, 
        #                         data_as_list_of_tuples,
        #                         ambiente.table_name_proy) 

        # 4. ------------------------------------------------ 5 GENERAR INTERFACE ------------------------------------------------------------------------------

        # generate_files_upload_interface(data=data_as_list_of_tuples, # ESPECIFICAR LA DATA EN FORMATO LISTA DE TUPLAS
        #                                 ambiente=ambiente,
        #                                 data_processor=data_processor,
        #                                 file_manager=file_manager)
        
        return process_response.success()
    
    except ValueError as e:
        # Errores del codigo
        print(str(e))
        return process_response.error(error_message=str(e))

    except Exception as e:
        # Errores inesperados
        print(f'Error inesperado: {str(e)}')
        return process_response.error(error_message=str(e))
        
        
if __name__ == '__main__':
    main()

