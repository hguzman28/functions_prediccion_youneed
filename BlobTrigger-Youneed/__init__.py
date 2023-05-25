import logging
from pandas import concat
import pandas as pd
import azure.functions as func
from main import Predict
from io import BytesIO
import pickle
import sklearn
from main import Predict
from io import BytesIO
from blob import upload_objects_to_container,get_all_object


def main(myblob: func.InputStream):
    try:
        logging.info(f"Python blob trigger function processed blob \n"
                    f"Name: {myblob.name}\n"
                    f"Blob Size: {myblob.length} bytes")


        blob_data_list = get_all_object("cointainer-s3")
        print(blob_data_list)
        logging.info(blob_data_list)
        logging.info("Pre Cargo Model.pkl")
        ruta_archivo = '/tmp/model.pkl'
        with open(ruta_archivo, 'rb') as f:
            model = pickle.load(f)  

        logging.info("End Cargo Model.pkl")
        logging.info("Star scaler.pkl")
        ruta_archivo = '/tmp/scaler.pkl'
        with open(ruta_archivo, 'rb') as f:
            scaler = pickle.load(f)
    
        fecha_inicio_test, fecha_final_test = '2023-05-01', '2023-05-31'
        logging.info("Read train_set and base_forecasting")

        data_preparation_api=pd.read_csv("/tmp/train_set.csv")
        data_train_endpoint=pd.read_csv("/tmp/base_forecasting.csv")
        datos_test = concat([data_preparation_api, data_train_endpoint])
        datos_test = datos_test.drop_duplicates(subset = 'fechahora', keep = 'first')
        datos_test = datos_test[(datos_test.fechahora >= fecha_inicio_test)& (datos_test.fechahora <= fecha_final_test)]
        fecha_test = datos_test[['fechahora']].values
        nombres_datos_test = data_train_endpoint.columns
        nombres_datos_test = nombres_datos_test[0:-1]

        logging.info("nombres_datos_test OK")

        prediction_object = Predict()
        demanda_p = prediction_object.final_prediction(model=model, x_test=data_train_endpoint)
        logging.info("demanda_p OK")
        base_prediccion = prediction_object.fun_dataset_prediccion(ypred=demanda_p, x_test=data_train_endpoint, escalador=scaler, nombres_datos_test=nombres_datos_test, datos_testeo=datos_test, fecha_test=fecha_test)
        base_prediccion2= base_prediccion[['fechahora', 'Prediccion', 'intervalo1']]
        logging.info("base_prediccion2 OK")
        print(base_prediccion2)

        upload_objects_to_container([base_prediccion2],"container-output")
    except Exception as e:
        print(e)
        raise e   