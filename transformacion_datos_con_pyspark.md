
# Transformación de Datos con PySpark
## Documentación con ejemplos de PySpark: [https://sparkbyexamples.com/pyspark/](https://sparkbyexamples.com/pyspark/)

1. **Filtrar con Múltiples Condiciones**
   ```python
   df = df.where((df["column_name"] == "valor") & (df["column_name2"] == "valor"))
   ```

2. **Agregar o Modificar Columnas**
   ```python
   df = df.withColumn("column_name", lit("valor")).withColumn("otra_columna", lit("otro valor"))
   ```

3. **Join entre dos DataFrames**
   ```python
   df_joined = df1.join(df2, df1["columna_relacion_df1"] == df2["columna_relacion_df2"], "inner")
   ```

4. **Modificar Valores de una Columna Basándose en Condiciones**
   ```python
   df = df.withColumn("column_name",
                      when(df.column_name == "tiene_este_valor", "nuevo_valor")
                      .when(df.column_name.isNull(), "")
                      .otherwise(df.column_name))
   ```

5. **Funciones Definidas por el Usuario (UDF)**
   ```python
   def sumar(a, b):
       return a + b

   suma_udf = udf(sumar, IntegerType())
   df = df.withColumn("suma", suma_udf(col("column_name_1"), col("column_name_2")))
   ```

6. **Agrupar y Calcular Sumas y Promedios**
   ```python
   df_agrupado = df.groupBy("categoria").agg(sum("ventas").alias("total_ventas"), avg("precio").alias("precio_promedio"))
   ```

7. **Ordenar Datos**
   ```python
   df_ordenado = df.orderBy(df.fecha.desc())
   ```

8. **Convertir String a Fecha**
   ```python
   df = df.withColumn("fecha", to_date(df.fecha_string, "yyyy-MM-dd"))
   ```

9. **Extraer Componentes de la Fecha**
   ```python
   df = df.withColumn("año", year(df.fecha))
   df = df.withColumn("mes", month(df.fecha))
   df = df.withColumn("dia", dayofmonth(df.fecha))
   ```

10. **Sumar Días a una Fecha**
    ```python
    df = df.withColumn("fecha_mas_10", date_add(df.fecha, 10))
    ```

11. **Concatenar valores en columna al final de un string**
    ```python
      from pyspark.sql.functions import concat, lit

      # Usar concat para agregar '000' al final de los valores de la columna
      df = df.withColumn("columna", concat(df["columna"], lit("000")))
    ```

12 **Concatenar valores al inicio de un string con pyspark**
   ```python
      from pyspark.sql.functions import format_string

      # Usar format_string para anteponer '000' a los valores de la columna
      df_modificado = df.withColumn("columna", format_string("000%s", "columna"))
   ```