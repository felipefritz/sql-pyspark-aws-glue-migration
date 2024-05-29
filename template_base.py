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
        
      # Resto del codigo...
      
      
    except ValueError as e:        
        logger.exception(f"ERROR DE PROCESO")
        return process_response.error(error_message=str(e))

    except Exception as e:
        # Errores inesperados
        logger.exception(f"ERROR INTESPERADO")
        return process_response.error(error_message=str(e))
        
    return process_response.success()
        
        
if __name__ == '__main__':
    main()