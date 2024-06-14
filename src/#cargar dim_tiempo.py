#cargar dim_tiempo
import connections_config
import pandas as pd

# %%
#Guardar conexion a la base de datos de akademos en la variable "akademos"
akademos = connections_config.akademos_data_base

#Guardar conexion al Mercado de Datos en la variable "datamart"
datamart =connections_config.datamart_data_base

# %%
#Consulta para extracción de datos
query = """SELECT id, fecha_creacion, extract(YEAR from fecha_creacion) AS anno, extract(month from fecha_creacion) AS mes, EXTRACT (DAY FROM fecha_creacion) AS dia FROM tramite_tbd_tramite_estudiante"""

query_almacen = """SELECT * FROM dim_tiempo"""

# %%
#Cargar las fechas de los movimientos de la BD en un DataFrame
df = pd.read_sql(query, akademos)

#cargar las fechas de los movimientos del Mercado en un DataFrame
df_datamart = pd.read_sql(query_almacen, datamart)

df['id_modificated'] = df.apply(lambda row: int(row['anno']*10000 + row['mes']*100 + row['dia']), axis = 1)


# Realizar la operación para definir el id_tiempo y guardar los resultados en un nuevo DataFrame
df_resultado = pd.DataFrame()
df_resultado['id_tiempo'] = df.apply(lambda row: int(row['anno']*10000 + row['mes']*100 + row['dia']), axis = 1)
df_resultado['fecha'] = df['fecha_creacion']

# Eliminar duplicados en la columna 'id_tiempo'
df_resultado_unicos = df_resultado.drop_duplicates(subset=['id_tiempo'])

df_resultado, df_resultado_unicos, df_datamart, df


# %%
#Compara los DataFrame y quedarse solo con los valores nuevos, los que no se repiten (Cuando se ralizan procesos nuevos con fechas no activas en el sistema)
df_guardar = df[~df['id_modificated'].isin(df_resultado['id_tiempo'])]
df_guardar

# %%
#Guardar las fechas en el Mercado
df_resultado_unicos.to_sql("dim_tiempo", con = datamart, if_exists = "append", index = False)


