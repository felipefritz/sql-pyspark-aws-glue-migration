import sys
import logging
import inspect
import os
import boto3
import json
import base64
import datetime
import calendar
import decimal

from os import remove
from datetime import timedelta
from itertools import cycle

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

from botocore.exceptions import ClientError
from botocore.exceptions import NoCredentialsError


class Library:
  """ Clase de librerías de conexión AWS S3, RDS MySQL, SecretManager """

  def __init__ (self):
    """ Función constructor """


  ####################################
  ######## CÓDIGO CONSOLA AWS ########
  ####################################

  def secretManager (self, secrets):
    """ Función manager de conexión para obtener los secret de AWS """
    
    try:
      if secrets == '':
        print('Debe enviar la ruta del secreto')
        return False

      session = boto3.session.Session()
      
      client = session.client(service_name = 'secretsmanager')
  
      get_secret_value_response = client.get_secret_value(SecretId = secrets)
  
      if 'SecretString' in get_secret_value_response:
        secret = json.loads(get_secret_value_response['SecretString'])
      else:
        secret = json.loads(base64.b64decode(get_secret_value_response['SecretBinary']))
  
      return secret
    except ClientError as e:
      if e.response['Error']['Code'] == 'DecryptionFailureException':
        print('Error DecryptionFailureException [secretManager]: ' + str(e))
        return False
      elif e.response['Error']['Code'] == 'InternalServiceErrorException':
        print('Error InternalServiceErrorException [secretManager]: ' + str(e))
        return False
      elif e.response['Error']['Code'] == 'InvalidParameterException':
        print('Error InvalidParameterException [secretManager]: ' + str(e))
        return False
      elif e.response['Error']['Code'] == 'InvalidRequestException':
        print('Error InvalidRequestException [secretManager]: ' + str(e))
        return False
      elif e.response['Error']['Code'] == 'ResourceNotFoundException':
        print('Error ResourceNotFoundException [secretManager]: ' + str(e))
        return False


  def uploadFileAWS (self, route_tmp, file_name, bucket, route_s3):
    """ Función que permite la subida de un archivo a S3 AWS """
    
    try:
      if route_tmp == '':
        print('Debe enviar una ruta temporal donde se encuentra su archivo')
        return False
      
      if file_name == '':
        print('Debe enviar el nombre del archivo que se debe subir')
        return False

      if bucket == '':
        print('Debe enviar el nombre del bucket')
        return False
      
      if route_s3 == '':
        print('Debe enviar la ruta donde se almacenará el archivo')
        return False
      
      s3 = boto3.resource('s3')

      print("Inicia carga a S3")
      print("Ruta archivo origen =>", route_tmp)
      print("Ruta archivo destino =>", route_s3)

      # upload file in tmp to bucket
      s3.meta.client.upload_file(route_tmp, bucket, route_s3)

      print("Finaliza carga a S3")

      return True
    except OSError as e:
      print('Error [uploadFileAWS]: ' + str(e))
      print("El archivo no se encontro")
      print("Finaliza carga a S3")
      return False
    except NoCredentialsError:
      print("Credenciales no disponibles [uploadFileAWS]")
      print("Finaliza carga a S3")
      return False


  def checkFileExistance (self, bucket, path):
    """ Función que permite verificar si existe un archivo en la ruta en S3 
    @bucket = nombre del bucket
    @path = ruta/archivo 
    retorna True or False
    """

    try:
      print('validación de existencia del archivo en S3')

      s3 = boto3.client('s3')

      results = s3.list_objects(Bucket = bucket, Prefix = path)

      if ('Contents' in results):
        print('El archivo ' + path  + ' existe en el bucket')
        return True
      else:
        print('El archivo ' + path  + ' NO existe en el bucket')
        return False
    except (IOError, NameError) as e:
      print('error: ' + str(e))
      print('Ha ocurrido un error al buscar el archivo en: ' + str(path))
      return False
  
  # deprecado, se eliminará, usar readFileS3
  def read_file (self, bucket_name, path): 
    """Funcion que permite leer un archivo desde S3, 
    retorna una lista

    Args:
        bucket_name (_type_): nombre del bucket
        path (_type_): ruta INPUT/archivo 

    Returns:
        Lista [(file1),(file2),(file3)]
    """

    lista = []
    s3 = boto3.client('s3')  
    fileobj = s3.get_object(Bucket=bucket_name, Key=path) 

    filedata = fileobj['Body'].read()
    contents = filedata.decode('utf-8')
    for line in contents.splitlines():
        lista.append(line)
    return lista


  def readFileS3 (self, bucket_name, path): 
    """ Funcion que permite leer un archivo desde S3, retorna una lista
    Args:
      bucket_name (_type_): nombre del bucket
      path (_type_): ruta INPUT/archivo 

    Returns:
      Lista [(file1),(file2),(file3)]
    """

    if bucket_name in ("","None", None):
      print('Debe enviar el nombre del bucket')
      return False

    if path in ("","None", None):
      print('Debe enviar el nombre del bucket')
      return False

    try:
      s3 = boto3.client('s3')  

      lista = []
      fileobj = s3.get_object(Bucket=bucket_name, Key=path) 

      filedata = fileobj['Body'].read()
      contents = filedata.decode('utf-8')

      for line in contents.splitlines():
        lista.append(line)

      return lista
    except (IOError, NameError) as e:
      print('error: ' + str(e))
      print('Ha ocurrido un error al leer el archivo en: ' + str(path))
      return False

    
  def readFileJsonS3 (self, bucket_name, path):
    """Funcion que permite leer un archivo desde S3
    Args:
    bucket_name (_type_): nombre del bucket
    path (_type_): ruta INPUT/archivo  
      
    Returns:json"""

    if bucket_name in ("","None", None):
      print('Debe enviar el nombre del bucket')
      return False

    if path in ("","None", None):
      print('Debe enviar el nombre del bucket')
      return False

    try:
      s3 = boto3.client('s3')

      fileobj = s3.get_object(Bucket=bucket_name, Key=path)
      
      filedata = fileobj['Body'].read().decode('utf-8')
      json_text = json.loads(filedata)

      return json_text
    except (IOError, NameError) as e:
      print('error: ' + str(e))
      print('ha ocurrido un error al leer el archivo json en: ' + str(path))
      return False


  def checkEmptyFile (self, bucket, s3_file):
    """ Función que verifica si el archivo en s3 esta vacio.
    @bucket: Nombre del bucket en s3.
    @ s3_file: Ruta de ubicación del archivo. 
    """

    try:
      s3 = boto3.client('s3')

      fileobj = s3.get_object(Bucket=bucket, Key=s3_file) 
      filedata = fileobj['Body'].read()

      if len(filedata) == 0:
        print("Archivo " + str(s3_file) + " vacio")
        return False
      else:
        print("Archivo de entrada " + str(s3_file) + " no esta vacio. Continúa proceso")
        return True 
    except:
      print("Problemas al validar si el Archivo " + str(s3_file) + " esta vacio")
      return False


  def deleteFileAWS (self, bucket_name, interface_obj):
    """Función que elimina un archivo desde S3.
    @bucket_name: Nombre del bucket en S3.
    @interface_obj: Objeto de salida de funcion generateNameRoutesFile
    {'route_tmp', 'route_s3', 'name_file'}
    """

    s3 = boto3.client('s3')

    print(f"Inicio eliminacion de interfaz en S3")

    key_s3 = interface_obj['route_s3'] #+ interface_obj['name_file']
    bucket_s3 = bucket_name
    file_name = interface_obj['name_file']

    try:
      resp = s3.delete_object(Bucket=bucket_s3, Key=key_s3)
      print("Se ha eliminado el archivo antiguo: " + str(resp))
      return True
    except Exception as ex:
      print("Error al eliminar el archivo" + str(file_name) + " en ruta " + str(bucket_s3))
      print("ERROR: " + str(ex))
      return False


  ####################################
  ######### CÓDIGO UTILIDAD ##########
  ####################################

  def validateEvent (self, event):
    """ Función utils valida evento recibido (fecha) """

    if event == '' and type(event) != str:
      print("Parámetro fecha vacío ( " + event + " )")
      return False
    else:
      try:
        if len(event) == 8:
          # "20220301" se obtiene fecha
          year = event[0:4]
          month = event[4:6]
          day = event[6:8]

          date_event = year + '-' + month + '-' + day

          datetime.datetime.strptime(date_event, '%Y-%m-%d')

          return date_event
        elif len(event) == 16:
          # "2022030120220331" se obtiene fecha inicio y fecha término
          date_tmp = []
          list_date = []

          date_init = event[0:8]
          date_finish = event[8:16]

          date_tmp.append(date_init)
          date_tmp.append(date_finish)

          # valida ambas fechas enviadas
          for d in date_tmp:
            year = d[0:4]
            month = d[4:6]
            day = d[6:8]

            date = year + '-' + month + '-' + day

            datetime.datetime.strptime(date, '%Y-%m-%d')

            list_date.append(date)

          return list_date
        else:
          print('el largo del string de fechas no corresponde a los correctos (8,16)')
          return False
      except ValueError:
        print("Formato incorrecto [validateEvent] " + str(event))
        return False

  
  def generateNameRoutesFile (self, stage, file_path, route_bucket_s3, interface_name, extension):
    """ Función que genera el archivo que será subido a S3 """

    try:
      if stage == '':
        print('Debe enviar el ambiente')
        return False

      if file_path == '':
        print('Debe enviar el file path donde se creará el archivo (tmp)')
        return False

      if route_bucket_s3 == '':
        print('Debe enviar la ruta donde requiere cargar el archivo en S3')
        return False

      if interface_name == '':
        print('Debe enviar el nombre de la interfaz o del archivo')
        return False

      if extension == '':
        print('Debe enviar una extensión en Mayúscula (TXT, CSV)')
        return False
      else:
        if extension == 'TXT':
          ext = '.' + str(extension)
        elif extension == 'CSV':
          ext = '.' + str(extension)
        elif extension == 'TCL':
          ext = '.' + str(extension)
        elif extension == 'json':
          ext = '.' + str(extension)

        else:
          ext = '.TXT'
      
      file_name = interface_name + ext

      if file_name[0:3] in ('CBC','CTE','AHO'):
        if stage == 'DESARROLLO':
          initial = 'D'
        elif stage == 'QA':
          initial = 'T'
        else:
          initial = 'P'

        route_tmp = (f"{ file_path }{ initial }{ file_name }")      # /tmp/PNAMEFILE.TXT
        route_s3 = (f"{ route_bucket_s3 }{ initial }{ file_name }") # glue/CBC/FTP/OUTPUT/PNAMEFILE.TXT
        name_file = (f"{ initial }{ file_name }")                   # PNAMEFILE.TXT
      else:
        route_tmp = (f"{ file_path }{ file_name }")                 # /tmp/NAMEFILE.TXT
        route_s3 = (f"{ route_bucket_s3 }{ file_name }")            # glue/CBC/FTP/OUTPUT/NAMEFILE.TXT
        name_file = (f"{ file_name }")                              # NAMEFILE.TXT

      response = {
        'route_tmp': route_tmp,
        'route_s3': route_s3,
        'name_file': name_file
      }

      return response
    except (IOError, NameError) as e:
      print("Error [generateNameRoutesFile]: " + str(e))
      return False

  
  def validateEmptyValue (self, data, len_output, data_type):
    """ Función que permite validar un campo vacío dando largo correspondiente a cada campo validados """

    try:
      if len_output == '':
        print('Debe enviar el largo del campo')
        return False

      if data_type == '':
        print('Debe enviar el tipo de dato a validar (STR - INT)')
        return False

      if data_type == 'INT':
        try:
          if data == None or data == 'None' or data == '':
            data_output = "0".zfill(len_output)
          else:
            verification = int(data)
            verify_replace = str(verification).replace("-","")
            data_output = str(verify_replace).zfill(len_output)
          data_output = data_output[0:len_output]
        except ValueError:
          print('el dato no es un entero: ' + str(data))
          return "0".zfill(len_output)
      elif data_type == 'STR':
        if data == None or data == 'None' or data == '':
          data_output = (" ").ljust(len_output)
        else:
          data_output = str(data).ljust(len_output)
        data_output = data_output[0:len_output]
      else:
        print('Debe enviar un tipo de dato válido (STR - INT)')
        return False

      return data_output
    except (IOError, NameError) as e:
      print("Error [validateEmptyValue]: " + str(e))
      return False


  def validateAmount (self, amount, len_output):
    """ Función que permite validar el monto enviado """

    try:
      if len_output == '':
        print('Debe enviar el largo del campo')
        return False
      
      if amount == None or amount == 'None' or amount == '':
        return "0".zfill(len_output)
      else:
        if str(amount) == '0E-10':
          return "0".zfill(len_output)
        else:
          amount_output = str(amount).replace("-","")
          amount_output = amount_output.split('.')
          verification = int(amount_output[0])
          amount_output = str(verification).zfill(len_output)
          amount_output = amount_output[0:len_output]

          return amount_output
    except (IOError, NameError) as e:
      print("Error [validateAmount]: " + str(e))
      return "0".zfill(len_output)


  def validateNumberChannel (self, channel, len_output, list_channel):
    """ Función que permite validar el número de canal """

    if len_output == '':
      print('Debe enviar el largo que requiere el valor')
      return False

    if list_channel == () or len(list_channel) == 0:
      print('Debe enviar canales de búsqueda')
      return False

    try:
      if channel == '' or channel == None or channel == 'None':
        return { 'channel_code': str('999').zfill(len_output), 'channel_name': str('').ljust(len_output) }

      v = list_channel[channel]
      value = v.split(",")
      channel_code = value[0]
      channel_name = value[1]
      channel_code = str(channel_code).zfill(len_output)

      return { 'channel_code': channel_code, 'channel_name': channel_name }
    except (KeyError, IOError, NameError) as e:
      print("Error [validateNumberChannel]: " + str(e))
      print("Canal no mapeado en Secrets Manager " + str(channel))
      channel_code = str('999').zfill(len_output)
      channel_name = str(channel).replace("_","").upper().ljust(len_output)
      return { 'channel_code': channel_code, 'channel_name': channel_name }


  def convertFormatDate (self, event, flag):
    """ Función que permite convertir y validar una fecha según formato """

    try:
      # convierte fecha de YYYYMMDD a YYYY-MM-DD
      if flag == 1: 
        if (str(event) == '' or str(event) == 'NoneType' or str(event) == 'None'):
          print("Parámetro fecha vacío ( " + str(event) + " ) - flag " + str(flag))
          return "0000-00-00"
        else:
          try:
            ev = str(event)
            year = ev[0:4]
            month = ev[4:6]
            day = ev[6:8]

            date_event = year + '-' + month + '-' + day
            datetime.datetime.strptime(date_event, '%Y-%m-%d')

            return date_event
          except ValueError:
            print("Formato incorrecto " + str(event))
            return "0000-00-00"

      # convierte fecha de YYYY-MM-DD/YYYY-MM-DD HH:MM:SS a DDMMYYYY
      elif flag == 2:
        if (str(event) == '' or str(event) == 'NoneType' or str(event) == 'None'):
          print("Parámetro fecha vacío ( " + str(event) + " ) - flag " + str(flag))
          return "00000000"
        else:
          try:
            ev = str(event)
            year = ev[0:4]
            month = ev[5:7]
            day = ev[8:10]

            date = day + '' + month + '' + year
            datetime.datetime.strptime(date, '%d%m%Y')

            date = date[0:8]

            return date
          except ValueError:
            return "00000000"

      # convierte fecha de YYYYMMDD a DD-MM-YYYY
      elif flag == 3: 
        if (str(event) == '' or str(event) == 'NoneType' or str(event) == 'None'):
          print("Parámetro fecha vacío ( " + str(event) + " ) - flag " + str(flag))
          return "00-00-0000"
        else:
          try:
            ev = str(event)
            year = ev[0:4]
            month = ev[4:6]
            day = ev[6:8]

            date_event = day + '-' + month + '-' + year
            datetime.datetime.strptime(date_event, '%Y-%m-%d')

            return date_event
          except ValueError:
            print("Formato incorrecto " + str(event))
            return "00-00-0000"

      # convierte fecha de YYYY-MM-DD/YYYY-MM-DD HH:MM:SS a YYYYMMDD
      elif flag == 4:
        try:
          if (str(event) == '' or str(event) == 'NoneType' or str(event) == 'None'):
            print("Parámetro fecha vacío ( " + str(event) + " ) - flag " + str(flag))
            return "00000000"
          else:
            ev = str(event)
            year = ev[0:4]
            month = ev[5:7]
            day = ev[8:10]

            date = year + '' + month + '' + day
            datetime.datetime.strptime(date, '%Y%m%d')

            date = date[0:8]

            return date
        except ValueError:
          return "00000000"
    
      # no es ningún flag
      else:
        return False
    except (IOError, NameError) as e:
      print("Error [convertFormatDate]: " + str(e))
      return False


  def convertFormatDate2 (self, event, flag):
    """ Función que permite convertir y validar una fecha según formato """
    try:
      if event == '':
        print('Debe enviar fecha')
        return False
      if flag == '':
        print('Debe enviar flag')
        return False
      if flag == 1:
        format_date = "%Y-%m-%d"
      elif flag == 2:
        format_date = "%d%m%Y"
      elif flag == 3:
        format_date = "%Y%m%d"
      else:
        format_date = "%Y/%m/%d"
      if event != None or event != 'None':
        date = datetime.datetime.strptime(event, '%Y-%m-%d').strftime(format_date)
      else: 
        event = "00000000"
        year = event[0:4]
        month = event[4:6]
        day = event[6:8]
        date = format_date.replace("%Y",year).replace("%m",month).replace("%d",day)
      return date
    except (ValueError) as e:
      print("ValueError convertFormatDate2 | Formato fecha incorrecto = ", event, str(e))
      return False


  def generateEmptyFile (self, routes):
    """ Función que genera un archivo vacío """

    try:
      print("Generando la interfaz en: " + str(routes))

      with open(routes, "w") as q:
        interface_output = ''
        q.write(interface_output)
      q.close()

      print("Fin de la generacion interfaz en: " + str(routes))

      return True
    except (IOError, NameError) as e:
      print("Error [generateEmptyFile]: " + str(e))
      return False


  def convertDateOld (self, date, format):
    """ Función que convierte una fecha seteada 00000000 a 19000101 """

    try:
      if format == 'y':
        if date == '00000000':
          resp_date = '19000101'
        else:
          resp_date = str(date)
      elif format == 'd':
        if date == '00000000':
          resp_date = '01011900'
        else:
          resp_date = str(date)
      else:
        resp_date = str(date)
      
      return resp_date
    except (IOError, NameError) as e:
      print("Error [convertDateOld]: " + str(e))

      if format == 'y':
        return '19000101'
      elif format == 'd':
        return '01011900'
      else:
        return False

  
  def validateHours (self, hours):
    """ Función que permite validar una hora dando formato HHMMSS """
    
    try:
      if (hours == None or hours == 'None' or hours == '' or hours == '  :  :  '):
        return "0".zfill(6)

      if len(str(hours)) > 8: # por formato yyyy-mm-dd hh:mm:ss
        str_hours = str(hours)
        spl_hours = str_hours.split(' ')
        time = str(spl_hours[1]).replace(":","")
      else: # por formato hh:mm:ss
        time = str(hours).replace(":","")
      time = time[0:6]

      return time
    except (IOError, NameError) as e:
      print("Error [convertDateOld]: " + str(e))
      return "0".zfill(6)


  def validateReverse (self, data, trx_rel, len_output):
    """ Función que permite validar marca reversa * y modo D o R 
    @data =  valor estado reversa 
    @trx_rel = valor transaccion relacionada
    return * o vacío , D o R como json.
    """

    try:
      if len_output == '':
        print('Debe enviar el largo del campo')
        return False

      if data == None or data == 'None' or data == '' or str(data) == 'NoneType' or str(data).lower() == 'null':
        data_reversa = " ".ljust(len_output)
        data_mode = " ".ljust(len_output)
      else:
        res_trx_rel = ''
        if trx_rel == None or trx_rel == '' or trx_rel == 'None' or str(trx_rel) == 'NoneType' or str(trx_rel).lower() == 'null':
          res_trx_rel = '0'
        else:
          res_trx_rel = str(trx_rel)

        if data == "1": # marca reversa = 1
          if int(res_trx_rel) == 0: # no tiene trx relacionada es D
            data_reversa = str("*").ljust(len_output)
            data_mode = str("D").ljust(len_output)
          elif int(res_trx_rel) > 0: # si tiene trx relacionada es R
            data_reversa = str("*").ljust(len_output)
            data_mode = str("R").ljust(len_output)
        else: # marca reversa = 0 o None
          data_reversa = str(" ").ljust(len_output)
          data_mode = str("D").ljust(len_output)
        
      data_reversa = data_reversa[0:len_output]
      data_mode = data_mode[0:len_output]

      return { 'data_reversa': data_reversa, 'data_mode': data_mode }
    except (IOError, NameError) as e:
      print("Error [validateReverse]: " + str(e))
      return False


  def validateIdTerminalSpecial (self, data, len_output):
    """ Función que permite validar el id terminal """
  
    try:
      if len_output == '':
        print('Debe enviar el largo del campo [validateIdTerminalSpecial]')
        return False

      if data == '':
        print('Debe enviar un id de terminal [validateIdTerminalSpecial]')
        return False

      if data == None or data == 'None':
        id_terminal = "0".rjust(4,"0")
      elif len(data) < 4: 
        id_terminal = str(data).rjust(4,"0")
      else:
        id_terminal = str(data)

      id_terminal = id_terminal.ljust(len_output,"0")
      id_terminal = id_terminal[0:len_output]

      return id_terminal
    except (IOError, NameError) as e:
      print("Error [validateIdTerminalSpecial]: " + str(e))
      return False


  def validateRut (self, rut, len_output):
    """ Función que valida rut separando rut y dígito verificador """

    try:
      if len_output == None or len_output == 'None' or len_output == '':
        return { 'rut': ("0").zfill(8), 'dv': '0' }

      if rut == None or rut == 'None' or rut == '':
        return { 'rut': ("0").zfill(len_output), 'dv': '0' }
   
      rutcli = str(rut).replace(".","").replace(",","").replace("-","")
      dvcli = rutcli
      largorut = len(rutcli)
      rut = (rutcli[0:(largorut-1)])
      dv = (dvcli[(largorut-1):(largorut)])

      return { 'rut': str(rut).zfill(len_output), 'dv': dv }
    except (IOError, NameError) as e:
      print("Error [validateRut]: " + str(e))
      return { 'rut': ("0").zfill(len_output), 'dv': '0' }


  def validateStateTrx (self, state, len_output):
    """ Función que valida el estado de una cuenta """

    try:
      if state == None or state == 'None' or state == '':
        print('Debe enviar el estado para que sea validado')
        return False

      if len_output == None or len_output == 'None' or len_output == '':
        print('Debe enviar el largo del campo')
        return False

      if state == 'ACTIVE':
        statex = ('ACTIVA').ljust(len_output)
      elif state == 'LOCKED':
        statex = ('BLOQUEADA').ljust(len_output)
      else:
        statex = (' ').ljust(len_output)

      return statex
    except (IOError, NameError) as e:
      print("Error [validateStateTrx]: " + str(e))
      return False

  
  def validateDateNowAndNextHoliday (self, next_day, list_holidays={}):
    """ Función que valida si el día actual es feriado, sí lo es, continua validando el siguiente """

    try:
      if next_day == None or next_day == 'None' or next_day == '':
        print('Debe enviar fecha a validar')
        return False

      # recorre si el día actual es feriado y el siguiente y así sucesivamente
      if next_day in list_holidays:
        while next_day in list_holidays:
          date_week = datetime.datetime.strptime(next_day, "%Y%m%d")
          day_week = date_week.weekday() # lunes = 0, martes = 1,  miercoles = 2, jueves = 3, viernes = 4
          day_next = timedelta(days = 1)
          d = ''

          if day_week == 4:
            d = "Viernes"
            day_next = timedelta(days = 3)
          elif day_week == 3:
            d = "Jueves"
            day_next = timedelta(days = 1)
          elif day_week == 2:
            d = "Miercoles"
            day_next = timedelta(days = 1)
          elif day_week == 1:
            d = "Martes"
            day_next = timedelta(days = 1)
          elif day_week == 0:
            d = "Lunes"
            day_next = timedelta(days = 1)
          elif day_week == 5:
            d = "Sábado"
            day_next = timedelta(days = 1)
          elif day_week == 6:
            d = "Domingo"
            day_next = timedelta(days = 2)

          print("R: SI ES FERIADO " + str(next_day) + ' y es día ' + str(d))

          execution_date = date_week + day_next
          execution_date = str(execution_date).split(" ")
          execution_date = execution_date[0].replace("-","")
        next_day = execution_date
      else:
        date_now = datetime.datetime.strptime(next_day, "%Y%m%d")

        if date_now.weekday() == 6: # domingo
          diff = 2
        elif date_now.weekday() == 5: # sábado
          diff = 1
        else: # otro día
          diff = 1
        
        # calculando dia hábil siguinte 
        date = (date_now + timedelta(days=diff)).strftime("%Y%m%d")
        print("día hábil = " + str(date) + " ES FERIADO?")

        if date in list_holidays:
          # sí es feriado 
          while date in list_holidays:
            print("R: SI ES FERIADO " + str(date)) 

            # obtiene diferencia de días
            test_date = datetime.datetime.strptime(date, "%Y%m%d")

            if test_date.weekday() == 0: # lunes
              diff = 3
            if test_date.weekday() == 6: # domingo
              diff = 2
            elif test_date.weekday() == 5: # sábado
              diff = 1
            else: # otro día
              diff = 1

            date = (test_date + timedelta(days=diff)).strftime("%Y%m%d")
        
        next_day = date

      resp_holiday = next_day

      print("R: NO ES FERIADO " + str(resp_holiday))

      return resp_holiday
    except (IOError, NameError) as e:
      print("Error [validateDateNowAndNextHoliday]: " + str(e))
      return False


  def validateDateNowAndPreviousHoliday (self, previous_day, list_holidays={}):
    """ Función que valida si el día actual es feriado, sí lo es, continua validando el anterior """

    try:
      if previous_day == None or previous_day == 'None' or previous_day == '':
        print('Debe enviar fecha a validar')
        return False

      if previous_day in list_holidays:
        # recorre si el día actual es feriado y el siguiente y así sucesivamente
        while previous_day in list_holidays:
          date_week = datetime.datetime.strptime(previous_day, "%Y%m%d")
          day_week = date_week.weekday() # lunes = 0, martes = 1,  miercoles = 2, jueves = 3, viernes = 4, sábado = 5, domingo = 6
          day_prev = timedelta(days = 1)
          d = ''

          if day_week == 4:
            d = "Viernes"
            day_prev = timedelta(days = 1)
          elif day_week == 3:
            d = "Jueves"
            day_prev = timedelta(days = 1)
          elif day_week == 2:
            d = "Miercoles"
            day_prev = timedelta(days = 1)
          elif day_week == 1:
            d = "Martes"
            day_prev = timedelta(days = 1)
          elif day_week == 0:
            d = "Lunes"
            day_prev = timedelta(days = 3)
          elif day_week == 5:
            d = "Sábado"
            day_prev = timedelta(days = 1)
          elif day_week == 6:
            d = "Domingo"
            day_prev = timedelta(days = 2)

          print("R: SI ES FERIADO " + str(previous_day))

          execution_date = date_week - day_prev
          execution_date = str(execution_date).split(" ")
          execution_date = execution_date[0].replace("-","")
          previous_day = execution_date

        resp_holiday = previous_day
      else:
        date_now = datetime.datetime.strptime(previous_day, "%Y%m%d")

        if date_now.weekday() == 6: # domingo
          diff = 2
        elif date_now.weekday() == 5: # sábado
          diff = 1
        else : # otro día
          diff = 0
        
        # calculando dia hábil anterior 
        date = (date_now - timedelta(days=diff)).strftime("%Y%m%d")
        print("día hábil = " + str(date) + " ES FERIADO?")

        # sí está feriado 
        while date in list_holidays:
          print("R: SI ES FERIADO " + str(date)) 

          # obtiene diferencia de días
          test_date = datetime.datetime.strptime(date, "%Y%m%d")

          if test_date.weekday() == 0: # lunes
            diff = 3
          elif test_date.weekday() == 6: # domingo
            diff = 2
          elif test_date.weekday() == 5: # sábado
            diff = 1
          else : # otro día
            diff = 1

          date = (test_date - timedelta(days=diff)).strftime("%Y%m%d")

        previous_day = date

      resp_holiday = previous_day
      
      print("R: NO ES FERIADO " + str(resp_holiday))

      return resp_holiday
    except (IOError, NameError) as e:
      print("Error [validateDateNowAndPreviousHoliday]: " + str(e))
      return False

  
  def deleteFileTmp (self, interface, file_path):
    """ Función que elimina archivo temporal existente """

    try:
      # valida si existe archivo temporal
      if os.path.exists(file_path) and os.path.exists(interface):
        print('Existe el archivo temporal ' + str(interface) + ' se procede a eliminar')
        remove(interface)
      else:
        print('No existe el archivo temporal ' + str(interface))
      
      return True
    except (IOError, NameError) as e:
      print("Error [deleteFileTmp]: " + str(e))
      return False


  def validateTrxType (self, type_trx, len_output):
    """ Función que permite validar el tipo de transacción asignando C/A o vacío según corresponda """

    try:
      if type_trx == None or type_trx == 'None' or type_trx == '':
        print('Debe enviar un tipo de transacción a validar (WITHDRAWAL/DEPOSIT)')
        return False

      if len_output == None or len_output == 'None' or len_output == '':
        print('Debe enviar el largo del campo')
        return False

      if str(type_trx) == 'WITHDRAWAL':
        trx_type = "C".ljust(len_output)
      elif str(type_trx) == 'DEPOSIT':
        trx_type = "A".ljust(len_output)
      else:
        trx_type = " ".ljust(len_output)

      return trx_type
    except (IOError, NameError) as e:
      print("Error [validateTrxType]: " + str(e))
      return False


  def previousBusinessDay (self, date, holidays):
    """ funcion que devuelve día hábil anterior
    @date en formato = AAAAMMDD
    @holidays = lista de feriados desde tabla holiday mambu 
    return día hábil anterior en formato = AAAAMMDD """

    try:
      if date == None or date == 'None' or date == '':
        print('Debe enviar fecha a validar [previousBusinessDay]')
        return False
        
      if len(holidays) == 0 or holidays == {} or holidays == []:
        print('Debe enviar un listado de fechas feriados [previousBusinessDay]')
        return False

      test_date = datetime.datetime.strptime(date, "%Y%m%d")

      # imprimiendo fecha de entrada
      print("Today: " + calendar.day_name[test_date.weekday()] +" "+  str(test_date)[0:10])

      # obtiene diferencia de dias
      if test_date.weekday() == 0:   # LUNES
        diff = 3
      elif test_date.weekday() == 6: # DOMINGO
        diff = 2
      else :                         # OTRO DIA
        diff = 1

      # calculando dia hábil anterior 
      date = (test_date - timedelta(days=diff)).strftime("%Y%m%d")
      print("hábil anterior = ", str(date), " ES FERIADO ? ")

      if date in holidays:
        # SI es feriado 
        while date in holidays:
          print(date, " SI ES FERIADO")  

          # obtiene diferencia de días
          test_date = datetime.datetime.strptime(date, "%Y%m%d")

          if test_date.weekday() == 0:   # LUNES
            diff = 3
          elif test_date.weekday() == 6: # DOMINGO
            diff = 2
          else :                         # OTRO DIA
            diff = 1
          date = (test_date - timedelta(days=diff)).strftime("%Y%m%d")

      print(date, " NO ES FERIADO")   
      return date
    except (IOError, NameError) as e:
      print("Error [previousBusinessDay]: " + str(e))
      return False


  def previousBusinessDayPlus1Day (self, date, holidays):
    """ funcion que devuelve día hábil anterior + 1 dia
    @date en formato = AAAAMMDD 
    @holidays = lista de feriados desde tabla holiday mambu 
    return día hábil anterior + 1 dia en formato = AAAAMMDD """

    try:
      if date == None or date == 'None' or date == '':
        print('Debe enviar fecha a validar [previousBusinessDayPlus1Day]')
        return False

      if len(holidays) == 0 or holidays == {} or holidays == []:
        print('Debe enviar un listado de fechas feriados [previousBusinessDayPlus1Day]')
        return False

      test_date = datetime.datetime.strptime(date, "%Y%m%d")

      # imprimiendo fecha de entrada
      print("Today : " + calendar.day_name[test_date.weekday()] +" "+  str(test_date)[0:10])
      print("Holidays List : " + str(holidays))

      # Obtiene diferencia de dias
      if test_date.weekday() == 0:   # LUNES
        diff = 3
      elif test_date.weekday() == 6: # DOMINGO
        diff = 2
      else :                         # OTRO DIA
        diff = 1

      # calculando día hábil anterior 
      date = (test_date - timedelta(days=diff)).strftime("%Y%m%d")
      print("Hábil anterior = ", str(date), " ES FERIADO ? ")

      if date in holidays:
        # SI es feriado 
        while date in holidays:
          print(date, " SI ES FERIADO")

          # obtiene diferencia de días
          test_date = datetime.datetime.strptime(date, "%Y%m%d")

          if test_date.weekday() == 0:   # LUNES
            diff = 3
          elif test_date.weekday() == 6: # DOMINGO
            diff = 2
          else :                         # OTRO DIA
            diff = 1
          date = (test_date - timedelta(days=diff)).strftime("%Y%m%d")

      print(date, " NO ES FERIADO")   
      new_date = datetime.datetime.strptime(date, "%Y%m%d")
      date = (new_date + timedelta(days=1)).strftime("%Y%m%d")

      return date
    except (IOError, NameError) as e:
      print("Error [previousBusinessDayPlus1Day]: " + str(e))
      return False


  def validateAmountNetoIVA (self, amount, len_output_amount, len_output_neto, len_output_IVA):
    """ Función que permite calcular el monto NETO e IVA, en función del amount enviado 
    @amount = monto bruto
    @len_output_amount = largo del monto bruto
    @len_output_neto = largo del monto neto
    @len_output_IVA = largo del monto IVA
    return monto bruto, monto neto y monto IVA """

    try:
      if len_output_amount == '':
        print('Debe enviar el largo del monto [validateAmountNetoIVA]')
        return False

      if len_output_neto == '':
        print('Debe enviar el largo del monto neto [validateAmountNetoIVA]')
        return False

      if len_output_IVA == '':
        print('Debe enviar el largo del monto IVA [validateAmountNetoIVA]')
        return False

      if amount == None or amount == 'None' or amount == '':
        amount = "0".zfill(len_output_amount)
        amount_neto = "0".zfill(len_output_neto)
        amount_IVA = "0".zfill(len_output_IVA)
      else:
        porc_IVA = "19"
        amount = str(amount).replace("-","")
        amount = amount.split(".")
        amount = int(amount[0])
        amount_neto = round(int(amount)/float("1."+str(porc_IVA)))
        amount_IVA = int(amount) - int(amount_neto)
        amount = str(amount).zfill(len_output_amount)
        amount_neto = str(amount_neto).zfill(len_output_neto)
        amount_IVA = str(amount_IVA).zfill(len_output_IVA)

      amount = amount[0:len_output_amount]
      amount_neto = amount_neto[0:len_output_neto]
      amount_IVA = amount_IVA[0:len_output_IVA]

      return { 'amount': amount, 'amount_neto': amount_neto, 'amount_IVA': amount_IVA }
    except (IOError, NameError) as e:
      print("Error [validateAmountNetoIVA]: " + str(e))
      return False
  
  
  def validateMovementGloss (self, cod_trx, len_output):
    """ Función que permite validar la glosa de movimiento """

    try:
      if len_output == '':
        print('Debe enviar el largo del campo [validateMovementGloss]')
        return False

      if cod_trx == None or cod_trx == 'None' or cod_trx == '':
        movement_gloss = "0"
      if (cod_trx) == '0596':
        movement_gloss = 'TRANSACCION DE GIRO'
      elif (cod_trx) == '0196':
        movement_gloss = 'TRAN DE DEP EN EFECTIVO'
      elif (cod_trx) == '0296':
        movement_gloss = 'TRAN DEP DOC MISMO BANCO'
      elif (cod_trx) == '0306':
        movement_gloss = 'TRAN ABONO POR TRASPASO'
      elif (cod_trx) == ("0670") or (cod_trx) == ("0640"):
        movement_gloss = "TRANSACCION DE CARGO"
      elif (cod_trx) == ("0380") or (cod_trx) == ("0340"):
        movement_gloss = "TRANSACCION DE ABONO"
      elif (cod_trx) == ("8390"):
        movement_gloss = "TRANSACCION RECAUDACION Y PAGO"
      elif (cod_trx) == ("0647"):
        movement_gloss = "TRANSACCION DE CARGO POR COMISION"
      else:
        movement_gloss = "0"

      movement_gloss = str(movement_gloss[0:len_output]).ljust(len_output)

      return movement_gloss
    except (IOError, NameError) as e:
      print("Error [validateMovementGloss]: " + str(e))
      return False


  def validateProductGloss (self, code, len_output_glo, len_output_sgm):
    """ Función que permite validar la glosa del producto y subsegmento """

    try:
      if len_output_glo == '':
        print('Debe enviar el largo del campo glosa [validateProductGloss]')
        return False

      if len_output_sgm == '':
        print('Debe enviar el largo del campo segmento [validateProductGloss]')
        return False
      
      sub_sgm = "SINDA" # valida código producto, asigna glosa

      if code == '' or code == None or code == 'None':
        glosa_prod = ""
        sub_sgm = ""
      elif code == "CCV00001":
        glosa_prod = "CUENTA BENEFICIO"
        sub_sgm = "PERSO"
      elif code == "CCV00002":
        glosa_prod = "CUENTA RUT MAMBU"
        sub_sgm = "PERSO"
      elif code == "CCV00003":
        glosa_prod = "CUENTA PYME COMPRAQUI"
        sub_sgm = "EMPRE"
      else:
        glosa_prod = ""
        sub_sgm = ""

      return { 'product_gloss': str(glosa_prod).ljust(len_output_glo), 'sub_segment': str(sub_sgm).ljust(len_output_sgm) }
    except (IOError, NameError) as e:
      print("Error [validateProductGloss]: " + str(e))
      return False


  def generateDvRut (self, rut):
    """ Función que permite validar rut sin digito verificador 
    @rut = parametro de entrada rut en formato rut sin digito verificador ni puntos, ni guión 
    @return {"rut": rut_value,"dv": dv_value} """

    if rut == "" or rut == None:
      print('debe enviar un rut')
      return False
    try:
      reversed_digits = map(int, reversed(str(rut)))
      factors = cycle(range(2, 8))
      s = sum(d * f for d, f in zip(reversed_digits, factors))

      if str((-s) % 11) == "10":
        dv = "K"
      else:
        dv = str((-s) % 11)
      return { "rut": str(rut), "dv": dv }
    except:
      print("rut no es valido " + str(rut))
      return False

      
  def getValueSign (self, value):
    """ Método que retorna el signo de un valor
    @value: parámetro de entrada correspondiente al valor a evaluar.
    """

    try:
      if float(value) >= 0:
        sig_val = '+'
      elif float(value) < 0:
        sig_val = '-'
      else:
        sig_val = ' '

      return sig_val
    except(IOError, NameError) as e:
      print ("Error [getValueSign]: " + str(e))
      return False


  def getAccountState (self, state_account, output_length):
    """ Evalua el estado de la cuenta y retorna un estado con largo y formato (Usado en SIGIR)
    @state_account: estado de la cuenta
    @output_length: largo de salida
    """

    try:
      if state_account == 'ACTIVE':
        state = 'A'.ljust(output_length, ' ')
      elif state_account == 'LOCKED':
        state = 'A'.ljust(output_length, ' ')
      elif state_account == 'CLOSED':
        state = 'C'.ljust(output_length, ' ')
      else:
        state = ' '.ljust(output_length, ' ')

      return state
    except(IOError, NameError) as e:
      print("Error [getAccountState]" + str(e))
      return False


  def getAvailableBalance (self, block_bal, total_bal):
    """ Calcula el saldo disponible para una cuenta 
    @block_bal: Saldo bloqueado en la cuenta.
    @tota_bal: Saldo total en la cuenta.
    """

    try:
      if isinstance(block_bal, decimal.Decimal) == True and isinstance(total_bal, decimal.Decimal) == True:
        if (float(block_bal) <= float(total_bal)):
          avl_bal = total_bal - block_bal
          return avl_bal
        else:
          print("saldo bloqueado " + str(block_bal) + " debe ser menor que el total " + str(total_bal))
          return 0
      else:
        print("los valores no son validos. Tipo: " + str(type(block_bal)))
        return 0
    except (IOError, NameError) as e:
      print("Error [getAvailableBalance]: " + str(e))


  def formatFloat (self, value, precs, length):
    """ Formatea un número a tipo float en valor absoluto y retorna como string.
    @value: Número a evaluar.
    @presicion: Cantidad de decimales.
    @length: Largo del campo llenado con campos a las izquierda. """

    try:
      if value == None or value == '':
        value = 0
      else:
        value = abs(float(value))

      d = f"{{:.{precs}f}}"
      a = str(f'{d}'.format(round(value, precs))).rjust(length,'0')

      return a
    except (IOError, NameError) as e:
      print("Error [formatFloat]: " + str(e))


  def getCodproduct (self,idProduct,idAccount):

    """ Forma el codigo del producto a partir del codigo de Familia de productos (primeros cinco digitos) y los primeros tres digitos de la cuenta.
    @idProduct: Id de la familia de productos
    @idAccount: Numero de la cuenta Mambu.
    """

    try:
      if idProduct == None or idProduct == '':
        product= "0".rjust(5,"0")
      else:
        product = str(idProduct[0:5])

      if idAccount == None or idAccount == '':
        subProduct = "0".rjust(5,"0")
      else:
        subProduct = str(idAccount[0:3])
      
      prodctEnd = product + subProduct

      return prodctEnd
    except (IOError, NameError) as e:
      print("Error [getCodproduct]: " + str(e))
      return False


  def validateCity (self, cod_comuna, len_output_city_code, len_output_city_name, list_city):
    """ Función que permite validar comunas 
    @cod_comuna = viene desde mambu en limpio, ejemplo 1, 43, 103
    @len_output_code_city = largo del codigo de comuna
    @len_output_city_name = largo de nombre de la comuna x cod_comuna
    @list_city = secreto batch/datos_glue_comunas
    """
    
    if len_output_city_code == '':
      print('Debe enviar el largo que requiere el valor len_output_city_code')
      return False

    if len_output_city_name == '':
      print('Debe enviar el largo que requiere el valor len_output_city_name')
      return False

    if list_city == () or len(list_city) == 0:
      print('Debe enviar comunas de búsqueda')
      return False

    try:
      city_name = list_city[cod_comuna]

      city_code = cod_comuna.zfill(len_output_city_code)
      city_name = str(city_name).ljust(len_output_city_name)
      city_name = city_name[0:len_output_city_name]
      
      return { 'city_code': city_code, 'city_name': city_name }
    except (KeyError, IOError, NameError) as e:
      print("Error [validateCity]: " + str(e))
      print("Comuna no mapeado en Secrets Manager " + str(cod_comuna))

      city_code = "0".zfill(len_output_city_code)
      city_name = " ".ljust(len_output_city_name)
      
      return { 'city_code': city_code, 'city_name': city_name }


  def getTypeAccount (self, prod_id, tutor, len_output):   
    """ Función que genera un código identificador del producto cerrado para 
    el proceso de cierre de cuentas.
    @prod_id = Codigo de producto.
    @tutor = rut de tutor asociado a la cuenta.
    """

    if prod_id == '':
      print('Debe enviar el largo que requiere el valor prod_id')
      return False

    if tutor == '':
      print('Debe enviar el largo que requiere el valor tutor')
      return False
    
    if len_output == '':
      print('Debe enviar el largo que requiere el valor len_output')
      return False
   
    try:
      if prod_id == "CCT00000":
        type_account = '1'
      elif prod_id == "LCR00100":
        type_account = '2'
      elif tutor != None and tutor.isnumeric() == True:
        type_account = '3'
      elif prod_id == "AHO00001":
        type_account = '7'
      elif prod_id == "CCV00002" or type_account == "CCV00000":
        type_account = '5'
      else:
        type_account = '0'
          
      return type_account.zfill(len_output)
    except(IOError, NameError) as e:
      print("Error [getTypeAccount]: " + str(e))
      return False
    

  def accountingDate (self, date_process, list_holidays):
    """ método que obtiene el último día del més hábil """

    try:
      if date_process == '':
        print('Debe enviar una fecha')
        return False
      
      if list_holidays == '':
        print('Debe enviar un listado de feriados')
        return False
      
      if len(list_holidays) == 0:
        print('Debe enviar un listado de feriados con información')
        return False
    
      year_str = str(date_process)[0:4]
      month_str = str(date_process)[4:6]
      year = int(year_str)
      month = int(month_str)

      last_day_month = datetime.date(year + int(month / 12), (month % 12) + 1, 1) - datetime.timedelta(days=1)
      last_day_month = str(last_day_month) # obtiene último día del mes

      last_day_month = str(last_day_month).replace("-", "")

      if last_day_month in list_holidays:
        while last_day_month in list_holidays:
          date_week = datetime.datetime.strptime(last_day_month, "%Y%m%d")
          day_week = date_week.weekday() # lunes = 0, martes = 1,  miercoles = 2, jueves = 3, viernes = 4, sábado = 5, domingo = 6
          day_prev = timedelta(days = 1)

          if day_week == 4:
            d = "VIERNES"
            day_prev = timedelta(days = 1)
          elif day_week == 3:
            d = "JUEVES"
            day_prev = timedelta(days = 1)
          elif day_week == 2:
            d = "MIÉRCOLES"
            day_prev = timedelta(days = 1)
          elif day_week == 1:
            d = "MARTES"
            day_prev = timedelta(days = 1)
          elif day_week == 0:
            d = "LUNES"
            day_prev = timedelta(days = 3)
          elif day_week == 5:
            d = "SÁBADO"
            day_prev = timedelta(days = 1)
          elif day_week == 6:
            d = "DOMINGO"
            day_prev = timedelta(days = 2)

          execution_date = date_week - day_prev
          execution_date = str(execution_date).split(" ")
          execution_date = execution_date[0].replace("-","")
          last_day_month = execution_date

        # obtiene diferencia de días
        test_date = datetime.datetime.strptime(last_day_month, "%Y%m%d")
        if test_date.weekday() == 0: # lunes
          diff = 3
        elif test_date.weekday() == 6: # domingo
          diff = 2
        elif test_date.weekday() == 5: # sábado
          diff = 1
        else : # otro día
          diff = 0

        last_day_month = (test_date - timedelta(days = diff)).strftime("%Y%m%d")
    
        resp_holiday = last_day_month
      else:
        last_day_month = datetime.datetime.strptime(last_day_month, "%Y%m%d")

        if last_day_month.weekday() == 6: # domingo
          diff = 2
        elif last_day_month.weekday() == 5: # sábado
          diff = 1
        else : # otro día
          diff = 0

        last_day_month = (last_day_month - timedelta(days = diff)).strftime("%Y%m%d")

        if last_day_month in list_holidays:
          while last_day_month in list_holidays:

            date_week = datetime.datetime.strptime(last_day_month, "%Y%m%d")
            day_week = date_week.weekday() # lunes = 0, martes = 1,  miercoles = 2, jueves = 3, viernes = 4, sábado = 5, domingo = 6
            day_prev = timedelta(days = 1)

            if day_week == 4:
              d = "VIERNES"
              day_prev = timedelta(days = 1)
            elif day_week == 3:
              d = "JUEVES"
              day_prev = timedelta(days = 1)
            elif day_week == 2:
              d = "MIÉRCOLES"
              day_prev = timedelta(days = 1)
            elif day_week == 1:
              d = "MARTES"
              day_prev = timedelta(days = 1)
            elif day_week == 0:
              d = "LUNES"
              day_prev = timedelta(days = 3)
            elif day_week == 5:
              d = "SÁBADO"
              day_prev = timedelta(days = 1)
            elif day_week == 6:
              d = "DOMINGO"
              day_prev = timedelta(days = 2)

            execution_date = date_week - day_prev
            execution_date = str(execution_date).split(" ")
            execution_date = execution_date[0].replace("-","")
            last_day_month = execution_date

          resp_holiday = str(last_day_month)
        else:
          last_day_month = datetime.datetime.strptime(last_day_month, "%Y%m%d")

          if last_day_month.weekday() == 6: # domingo
            diff = 2
          elif last_day_month.weekday() == 5: # sábado
            diff = 1
          else : # otro día
            diff = 0

          last_day_month = (last_day_month - timedelta(days = diff)).strftime("%Y%m%d")
          resp_holiday = str(last_day_month)

      return str(resp_holiday)
    except Exception as e:
      print('error [accountingDate]: ', e)
      return False


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
        