# Sugerencia para creacion de queries SQL


## Definicion de queries:


1. Si la query es muy extensa la puedes definir en una funcion fuera del metodo main y luego llamarla en la seccion de obtencion de queries

#### Ejemplo: 

```python
def query_principal(table_name):
  return f"SELECT * FROM {ambiente.table_name}" 
```

2. Si la funcion es corta la puedes agregar en la seccion de queries del proceso

```python
query_ejemplo = f"select * from {ambiente.table_name}"
```

3. Agrega este bloque de codigo debajo de la seccion de inicializar ambiente de la funcion **main** ( si es query simple o desde funcion o las que necesites)

```python

logger.info('OBTENIENDO QUERIES DEL PROCESO...')    

query_ejemplo = f"select * from {ambiente.table_name}"
query_desde_funcion = query_principal(table_name=ambiente.table_name) # Se accede a table name a traves de la clase ambiente o desde args['table_name']

```




