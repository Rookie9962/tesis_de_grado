# %%
import connections_config
import pandas as pd
import uuid
import numpy as np

# %%
#Guardar conexion a la base de datos de akademos en la variable "akademos"
akademos = connections_config.akademos_data_base

#Guardar conexion al Mercado de Datos en la variable "datamart"
datamart =connections_config.datamart_data_base

# %%
#Consulta para extracción de datos
query = """SELECT tc.id AS id_motivo,te.fecha_creacion AS fecha, extract(YEAR from te.fecha_creacion) AS anno, extract(month from te.fecha_creacion) AS mes, EXTRACT (DAY FROM te.fecha_creacion) AS dia, te.tramite_anterior AS tramite_anterior
	        FROM tramite_tbd_tramite_estudiante AS te
	            INNER JOIN tramite_tbr_tramite_configuracion AS tc
    		        ON te.tipo_tramite_estudiante = tc.id"""

query_almacen = """SELECT *	FROM h_movimiento_estudiantes;"""

consulta_prueba = """SELECT tc.id id_tramite, te.fecha_creacion, extract(YEAR from te.fecha_creacion) AS anno, extract(month from te.fecha_creacion) AS mes, EXTRACT (DAY FROM te.fecha_creacion) AS dia
	FROM tramite_tbd_tramite_estudiante AS te
	INNER JOIN tramite_tbr_tramite_configuracion AS tc
		ON te.tipo_tramite_estudiante = tc.id"""

solo_fechas = """SELECT DISTINCT fecha_creacion, 
		EXTRACT(YEAR FROM fecha_creacion) AS anno, 
		EXTRACT(MONTH FROM fecha_creacion) AS mes, 
		EXTRACT(DAY FROM fecha_creacion) AS dia 
	FROM tramite_tbd_tramite_estudiante"""

tramite = """SELECT tc.id id_tramite, te.fecha_creacion, 
		EXTRACT(YEAR FROM te.fecha_creacion) AS anno, 
		EXTRACT(MONTH FROM te.fecha_creacion) AS mes, 
		EXTRACT(DAY FROM te.fecha_creacion) AS dia
	FROM tramite_tbd_tramite_estudiante AS te
	INNER JOIN tramite_tbr_tramite_configuracion AS tc
		ON te.tipo_tramite_estudiante = tc.id"""

# %%
#Cargar los movimienitos de la BD en un DataFrame
df_fechas = pd.read_sql(solo_fechas, akademos)

df_tramite = pd.read_sql(tramite, akademos)

#cargar los movimientos del Mercado en un DataFrame
df_datamart = pd.read_sql(query_almacen, datamart)
df_fechas, df_tramite

# %%
df_fechas['id_tiempo'] = df_fechas.apply(lambda row: int(row['anno']*10000 + row['mes']*100 + row['dia']), axis = 1)
df_tramite['id_tiempo'] = df_fechas.apply(lambda row: int(row['anno']*10000 + row['mes']*100 + row['dia']), axis = 1)

altas = np.array([
    uuid.UUID('d0d9d3da-f51c-4baa-91e3-a6679068da5f'),
    uuid.UUID('7bdc04be-f760-4cae-a4e2-33f34b47c076'),
    uuid.UUID('a493688f-8b3c-4437-b50f-143214d205d5'),
    uuid.UUID('64c0228b-60f0-4507-972b-def4d12c9624')
])

bajas = np.array([
    uuid.UUID('e993a8f4-4038-472a-88f4-00b286ca1548'),
    uuid.UUID('33ddf217-f253-480f-a7db-b70c643fa663'),
    uuid.UUID('95da1316-e31a-4dcc-9b9f-42c4de270ca7'),
    uuid.UUID('560caf97-2d1b-459b-af64-409dde17abe7'),
    uuid.UUID('7fbd3773-c598-4d6b-90fd-222eeac81970')
])

otras_bajas = np.array([
    uuid.UUID('5ac77cc3-3e54-43b3-a989-a509d216226c'),
    uuid.UUID('adf8e754-2bcf-46ce-bd09-b3d71dcbcb8c')
])



df_tramite.loc[df_tramite['id_tramite'].isin(altas), 'motivos'] = 'cantidad_altas'
df_tramite.loc[df_tramite['id_tramite'].isin(bajas), 'motivos'] = 'cantidad_bajas'
df_tramite.loc[df_tramite['id_tramite'].isin(otras_bajas), 'motivos'] = 'cantidad_otras_bajas'


df_tramite

# %% [markdown]
# Sacando motivos por fechas unicas (despues de hablar con Hyseki)

# %%
# Inicializa un DataFrame para contar motivos por fecha
motivos_unicos = df_tramite['motivos'].unique()
df_contadores = pd.DataFrame({'id_tiempo': df_fechas['id_tiempo']})
for motivo in motivos_unicos:
    df_contadores[motivo] = 0

# Inicializa el índice del ciclo
index = 0

# Mientras el índice sea menor que el número de filas en df_fechas
while index < len(df_fechas):
    # Obtén la fecha actual
    fecha_actual = df_fechas.loc[index, 'id_tiempo']
    
    # Filtra los motivos correspondientes a la fecha actual
    motivos_actuales = df_tramite[df_tramite['id_tiempo'] == fecha_actual]['motivos']
    
    # Cuenta los motivos y actualiza el DataFrame de contadores
    for motivo in motivos_actuales:
        df_contadores.loc[df_contadores['id_tiempo'] == fecha_actual, motivo] += 1
    
    # Incrementa el índice
    index += 1

df_contadores['cantidad_graduados_egresados'] = 0
df_contadores['id_motivo'] = 'd0d9d3da-f51c-4baa-91e3-a6679068da5f'

df_contadores = df_contadores[['id_motivo', 'id_tiempo', 'cantidad_bajas', 'cantidad_altas', 'cantidad_otras_bajas', 'cantidad_graduados_egresados']]
df_contadores

# %%
df_guardar = df_contadores[~df_contadores['id_tiempo'].isin(df_datamart['id_tiempo'])]

df_guardar

# %%
#Guardar motivos en el Mercado
df_guardar.to_sql("h_movimiento_estudiantes", con = datamart, if_exists = "append", index = False)


