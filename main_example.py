from index_pyspark import Init, DataManager, PysparkUtils

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType


def main():
    
    try:
        
        args = Init.load_args_from_aws(args=['JOB_NAME', 'date', 'table_name', 'interface_output']) 
        logger, process_response = Init.inicializar_logger_y_response(args=args)
        
        logger.info('INICIALIZANDO VARIABLES DE ENTORNO Y CONFIGURANDO AMBIENTES...')

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
        return process_response.error(error_message=str(e))
        
    return process_response.success()
        
        
if __name__ == '__main__':
    main()