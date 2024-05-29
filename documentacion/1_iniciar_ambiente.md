# Inicializar ambiente y variables


1. Agregar el siguiente bloque de codigo dentro del try de la funcion main del job
2. **IMPORTANTE** Agregar las variables de entorno de tus archivos de variables de entorno de terraform. (las que defines en terraform/variables_desa.tfvars, etc...)
3. Deben estar estas clases importadas en tu main:
4. **from index_pyspark import Init, DataManager, PysparkUtils**
   
## USO DE LIB

1. La libreria de Index.py Library() se obtiene de ambiente.LIB

validar_monto = ambiente.LIB.validate_amount(argumentos_que_recibe)


```python
# ------------------------------------------------- 0 Cargar y validar secretos -----------------------------------------------------------------------



args = Init.load_args_from_aws(args=['JOB_NAME', 'date', 'table_name', 'interface_output'])   # VARIABLES DE ENTORNO AQUI

logger, process_response = Init.inicializar_logger_y_response(args=args)
logger.info('INICIALIZANDO PROCESO...')    

ambiente, db_manager, sql_query_manager, data_processor, pyspark_client, file_manager = Init.inicializar_proceso(args, glue_logger=logger)

# Configurar interface output:
ambiente.interface_output_name = args['interface_output']
ambiente.interface_routes_output = ambiente.generate_name_routes_file(interface_name= ambiente.interface_output_name,
                                                                      glue_route='FTP_OUTPUT')


# SOLO SI ES NECESARIO LEER UN ARCHIVO DE ENTRADA:
ambiente.interface_input_name = args['interface_input'] # Debe estar interface_input en tus variables de entorno
ambiente.interface_routes_input = ambiente.generate_name_routes_file(interface_name= ambiente.interface_output_name,
                                                                      glue_route='FTP_INPUT')
```
