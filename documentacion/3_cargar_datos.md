# Carga de datos desde mysql con pyspark

### Funciones disponibles:
1. **pyspark_client.load_dataframe_from_query(query=query_ejemplo, connection_type='batch')**
2. **pyspark_client.load_dataframe_from_table(table_name=ambiente.table_name, connection_type='batch')**


### Argumentos necesarios
Las funciones de cargar datos a pyspark reciben el argumente de connection_type, este argumento puede ser:
1. **connection_type='batch'**: Para conectarse a la base de datos batch
2. **connection_type='dms'**: Para conectarse a la base de datos dms o mambu


#### Usos
1. Agrega el logger en la funcion **main** del job, debajo de la seccion de queries o donde estimes conveniente

```python
logger.info('CARGANDO DATOS DEL JOB...')  
```

2. Cargar datos desde una query sql en formato string:

```python
df = pyspark_client.load_dataframe_from_query(query=query_ejemplo, connection_type='batch') # Reemplazar query
```

3. Cargar datos desde una tabla sql en formato string:
- **IMPORTATE**: 
```python

df = pyspark_client.load_dataframe_from_table(table_name=ambiente.table_name, connection_type='batch') # Reemplazar la tabla
```

4. Cargar datos desde una lista de tuplas:
   Este caso es útil para carga datos desde un archivo de texto y crear un dataframe de pyspark. Debes especificar las columnas para que los datos y las columnas encajen en el mismo orden.
```python
df = pyspark_client.create_dataframe_from_datalist( data=[('valor 1', 'valor2'), ( 'valor 1', 'valor 2')], column_names= ['col1', 'col2'] )
```