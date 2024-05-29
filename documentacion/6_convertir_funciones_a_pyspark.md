# Como convertir funciones de python a Pyspark
A veces queremos convertir funciones clasicas de python a pyspark para procesar columnas de un dataframe.
Esto se logra utilizando el metodo udf de pyspark, en donde recibe el nombre de la funcion y el tipo de dato que retorna.

1. Se deben importar los metodos desde:

```python
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType # importar mas tipos de datos
```

## Procesar funciones de la libreria Index.py --> Libbrary()
1. **valida_monto = udf(ambiente.LIB.validateAmount, StringType())**        
2. **validate_string = udf(ambiente.LIB.validateEmptyValue, StringType())**

