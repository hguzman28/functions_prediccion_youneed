import logging
from pandas import concat
import pandas as pd

import azure.functions as func
from main import Predict
from io import BytesIO

from blob import upload_objects_to_container,get_all_object


# datos_test = ./data/base_forecasting.csv

def main(myblob: func.InputStream):
def main():    
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")


    blob_data_list = get_all_object("cointainer-s3")

    fecha_inicio_test, fecha_final_test = 'Fecha1', 'Fecha2'
    datos_test = pd.read_csv("base_forecasting.csv")
    datos_test = datos_test.drop_duplicates(subset = 'fechahora', keep = 'first')
    datos_test = datos_test[(datos_test.fechahora >= fecha_inicio_test)& (datos_test.fechahora <= fecha_final_test)]
    fecha_test = datos_test[['fechahora']].values
    nombres_datos_test = datos_test.columns
    nombres_datos_test = nombres_datos_test[0:-1]

    prediction_object = Predict()
    demanda_p = prediction_object.final_prediction(model=model.pkl, x_test=datos_test)

    base_prediccion = prediction_object.fun_dataset_prediccion(ypred=demanda_p, x_test=datos_test, escalador=scaler.pkl, nombres_datos_test=nombres_datos_test, datos_testeo=datos_test, fecha_test=fecha_test)

    print(base_prediccion) #
    

main()
    # upload_objects_to_container(blob_data_list,"container-output")