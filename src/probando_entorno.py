import pandas as pd
import connections_config

def main():
    # Accediendo a las variables del archivo de configuración
    akademos = connections_config.akademos_data_base
    datamart = connections_config.datamart_data_base
    
    #Consulta para extracción de datos
    query = """SELECT id, nombre FROM tramite_tbr_tramite_configuracion"""

    df = pd.read_sql(query, akademos)
    # Imprimiendo las variables para verificar
    print(df)
    

if __name__ == "__main__":
    main()
