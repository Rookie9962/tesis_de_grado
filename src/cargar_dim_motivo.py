#cargar dim_motivo
import connections_config
import pandas as pd

# %%
#Guardar conexion a la base de datos de akademos en la variable "akademos"
akademos = connections_config.akademos_data_base

#Guardar conexion al Mercado de Datos en la variable "datamart"
datamart =connections_config.datamart_data_base

# %%
#Consulta para extracción de datos
query = """SELECT id AS id_motivo, nombre AS motivo_tramite FROM tramite_tbr_tramite_configuracion"""

query_almacen = """SELECT * FROM dim_motivo"""

# %%
#Cargar los motivos de la BD en un DataFrame
df = pd.read_sql(query, akademos)

#cargar los motivos del Mercado en un DataFrame
df_datamart = pd.read_sql(query_almacen, datamart)
df_datamart #Imprimir tabla dim_motivo del Mercado

# %%
#Compara los DataFrame y quedarse solo con los valores nuevos, los que no se repiten (En caso de annadir nuevos motivos el sistema)
df_resultado = df[~df['id_motivo'].isin(df_datamart['id_motivo'])]
df_resultado

# %%
#Guardar motivos en el Mercado
df_resultado.to_sql("dim_motivo", con = datamart, if_exists = "append", index = False)


