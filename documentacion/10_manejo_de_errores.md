# Manejo de errores de un job

La funcion main utiliza try: except para controlar los errores que puedan surgir en el proceso.
Cada funcion del job debe usar el bloque try except y levantar una exception de tipo raise V**alueError("mensajede error")** con el fin de controlar todos los errores que puedan surgir

## Ejemplo de uso:



```python
def funcion_ejemplo_job():
  try:
    # logica
    
  except Exception as e:
    raise ValueError(f"Ocurrio un error en funcion_ejemplo_job: {str(e)}")
  return

def main():
  try:
    # LOGICA DEL PROCESO
    funcion_ejemplo_job() # si hay un error aca, entrara en el bloque except ValueError as e de mas abajo.
    pass

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
```