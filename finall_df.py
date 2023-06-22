import pyodbc
import pandas as pd
import logging
import sys

server = "rep.database.windows.net"
database = "REP"
username = "administrador"
password = "Rep2023*"
driver= '{ODBC Driver 17 for SQL Server}'


# dt_historico = pd.read_csv('historico_data.csv')
# print(dt_historico)

def get_dt_historico():
    logging.info("START connection DB")
    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
        query = "SELECT * FROM pruebas2"
        df = pd.read_sql_query(query, conn)
    logging.info("END connection DB")    
    return df    


def final_df():
    try:
        dt_historico = get_dt_historico()
        
        dt_Data_to_dashboard = pd.read_csv("/tmp/data_to_dashboard.csv")        
        dt_prediccion = pd.read_csv("/tmp/base_prediccion2.csv")


        # Establecer la configuración regional antes de la conexión
        connection_string = (
            f"DRIVER={driver};"
            f"SERVER=tcp:{server};"
            f"PORT=1433;"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            "DecimalSeparator=."  # Establecer el separador decimal como punto
        )

        # Validar existencia de fechas en dt_historico y agregar fila si demanda está vacía
        print("Validar existencia de fechas en dt_historico y agregar fila si demanda está vacía")
        for index, row in dt_historico.iterrows():
            fecha = row['fechahora']
            demanda = row['demanda']
            if pd.isnull(demanda):
                fecha_formatted = fecha.strftime("%Y-%m-%d")  # Formatear fecha como "AAAA-MM-DD"
                if fecha_formatted in dt_Data_to_dashboard['fechahora'].astype(str).str[:10].values:
                    id_historico = row['id']  # Obtener el id del registro en dt_historico
                    demanda_actualizada = dt_Data_to_dashboard.loc[dt_Data_to_dashboard['fechahora'].astype(str).str[:10] == fecha_formatted, 'demanda'].values[0]


                    if len(str(demanda_actualizada)) > 1:
                        demanda_actualizada = str(demanda_actualizada).replace(",", ".")
                        dt_historico.at[index, 'demanda'] = demanda_actualizada


                        # Actualizar el registro en la base de datos
                        with pyodbc.connect(connection_string) as conn:
                            cursor = conn.cursor()
                            query = f"UPDATE pruebas2 SET demanda = {demanda_actualizada} WHERE id = {id_historico}"
                            cursor.execute(query)
                            conn.commit()     
        #        

        logging.info("Agregar filas faltantes de dt_Data_to_dashboard a dt_historico")

        # Insertar las filas nuevas en la tabla pruebas2
        # Agregar filas faltantes de dt_Data_to_dashboard a dt_historico
        fechas_historico = set(dt_historico['fechahora'].astype(str).str[:10])
        fechas_nuevas_prediccion = dt_prediccion[~dt_prediccion['fechahora'].astype(str).str[:10].isin(fechas_historico)]
        fechas_nuevas_prediccion = fechas_nuevas_prediccion.fillna(0)
        print("#################### Tamaño fechas_nuevas_prediccion ####################")
        print(fechas_nuevas_prediccion.shape)
        print(fechas_nuevas_prediccion.columns)

        logging.info("#################### Insertar df_predict a la db ####################")

        if not fechas_nuevas_prediccion.empty:
            logging.info("#### fechas_nuevas_prediccion tiene datos ##########")
            with pyodbc.connect('DRIVER=' + driver + ';SERVER=tcp:' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password) as conn:
                cursor = conn.cursor()
                for index, row in fechas_nuevas_prediccion.iterrows():
                    fecha = row['fechahora']
                    Prediccion = row['Prediccion']
                    Prediccion = str(Prediccion).replace(",", ".")
                    intervalo1 = row['intervalo1']           
                    # query = f"UPDATE pruebas2 SET demanda = {demanda} WHERE id = {id_historico}"
                    query = f"INSERT INTO pruebas2 (fechahora, Prediccion, intervalo1) VALUES ('{fecha}', {Prediccion}, {intervalo1})"

                    cursor.execute(query)
                conn.commit()
     

        final_df = pd.concat([dt_historico, dt_prediccion], ignore_index=True)

        return final_df
    except:
        print("Unexpected error ##### finall_df.py ###: %s", sys.exc_info())
        logging.info("Unexpected error ##### finall_df.py ###: %s", sys.exc_info())


  