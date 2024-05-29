## Metodos y proceso para generar una interface en S3:


```python
data_as_list_of_tuples = []
data_as_list_of_tuples = pyspark_client.dataframe_to_list_of_tuples(dataframe=df)
DataManager.generate_files_upload_interface(data=data_as_list_of_tuples, # ESPECIFICAR LA DATA EN FORMATO LISTA DE TUPLAS
                                            ambiente=ambiente,
                                            data_processor=data_processor,
                                            file_manager=file_manager,
                                            logger=logger)
```