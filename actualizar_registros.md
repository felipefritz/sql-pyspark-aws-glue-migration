# Actualización de Datos Masivos

La función `actualizar_datos_masivos()` permite actualizar grandes cantidades de datos en una base de datos. Puede manejar actualizaciones con una única condición.

## 0. Uso de la funcion: 
**def actualizar_datos_masivos(sql_query_manager: SQLQueryManager, ambiente: Ambiente, db_manager: DatabaseManager, data_connect: dict, column_names: List[str], data: List[Tuple], condition_column: str, is_upsert: bool= True):**
- Esta funcion construye la query de actualizacion o upsert basado en la data y nombre de columnas.
Si is_upsert es falso, entonces solo necesitas actualizar y deberas modificar el orden de data 
para que el valor para la columna de la sentencia where, quede en la ultima posicion.

- Ejemplo:

si la data viene asi:
**data = [(num_cta, valo2, valo2 )]**
- En donde **'num_cta'** es la columna para la sentencia **WHERE**,
deberas **modificar tu consulta SELECT** en donde traes los datos para que num_cta quede asi:
**data = [(valo1, valo2, num_cta )]**

- Si necesitas usar una query de actualizacion propia, debes crearla en SQLQueryManager 
 y luego modificar la funcion **actualizar_datos_masivos()** para utilizar tu query y luego si no quieres actualizar de manera masiva,
 puedes utilizar db_manager.execute_SQL_commit() para ejecutar el update de tus datos 1 a 1 utilizando un bucle for.



### 1 Creacion de  Queries internamente en la funcion actualizar_datos_masivos():
Se  utiliza la clase SQLQueryManager para usar estas queries. El objetivo es construir dinamicamente las queries de update de manera dinamica.
Ambas funciones para armar las queries reciben una lista de nombre de columnas, lista de tuplas con la data a actualizar


### 1.1  **Query update masivo**
**SQLQueryManager.query_update_masivo(table_name: str, column_names: List[str], condition_column: str, data: List[Tuple[Any, ...]]) -> str)**
Esta función crea una consulta SQL para actualizar múltiples registros en una base de datos.
Se basa en solo una condicion, si necesitas mas de una condicion se debe modificar la seccion del WHERE.
Es **importante** que en la data, en cada tupla de la lista, **el valor del WHERE, por ejemplo num_cta, se encuentre al final de la tupla** . [(valor_columna1, valor_columna2, ...., valor_num_cta), (...)]

Ejemplo:
```python
query = SQLQueryManager.query_update_masivo(table_name="mi_tabla",
                                            column_names=["monto", "interes", "num_cta"],
                                            condition_column="num_cta",
                                            data=[(monto1, interes1, num_cta_1), (monto2, interes2, num_cta_2)])
# Salida: "UPDATE mi_tabla SET monto = %s, interes = %s, WHERE num_cta = %s;"
```

## 2. **Query upsert (insert on duplcate key update)**
### 2.1 **def query_upsert_masivo(table_name: str, column_names: List[str], unique_key: str, data: List[Tuple[Any, ...]]) -> str:**
Crea una consulta SQL para realizar un 'upsert' (insertar o actualizar) en una tabla de MySQL.
Utiliza la sentencia **INSERT INTO ... ON DUPLICATE KEY UPDATE**,

en donde toma la clave primaria de la tabla como campo a validar si el registro existe o no para su actualizacion.

Ejemplo:

Suponiendo que 'num_cta' es la clave única y 'monto', 'interes' son los otros campos

```python
query = SQLQueryManager.query_upsert_masivo(table_name="mi_tabla",
                                            column_names["num_cta", "monto", "interes"],
                                            unique_key="num_cta",
                                            data=[(23451, "1000", 3500), (34565, "2300", 3444)])
```
La consulta resultante intentará insertar las filas, y si el 'num_cta' ya existe, actualizará 'monto' y 'interes'.

Return ejemplo:
**INSERT INTO mi_tabla (num_cta, monto, interes) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE monto = VALUES(monto), interes = VALUES(interes);**


