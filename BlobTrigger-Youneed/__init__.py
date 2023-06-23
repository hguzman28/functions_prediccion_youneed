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
import sys
from finall_df import final_df
import datetime
from datetime import timedelta


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
        
        # fecha_actual = datetime.datetime.now()
        fecha_actual = datetime.datetime(2023, 5, 20)

        fecha2 = fecha_actual.replace(day=1)
        fecha3 = fecha_actual.replace(day=11, month=fecha_actual.month+1)
        fecha6 = fecha_actual.replace(day=10, month=fecha_actual.month+1)

        # fecha_inicio_test, fecha_final_test = '2023-05-01', '2023-05-31'
        fecha2=fecha2.strftime('%Y-%m-%d')
        fecha3=fecha3.strftime('%Y-%m-%d')
        fecha6=fecha6.strftime('%Y-%m-%d')
       
        logging.info(fecha2)
        logging.info(fecha3)
        logging.info(fecha6)
        
        fecha_inicio_test, fecha_final_test,fecha6 = fecha2,  fecha3, fecha6
        # fecha_inicio_test, fecha_final_test , fecha6= "2023-05-01",  "2023-06-11", "2023-06-10" 

        data_preparation_api=pd.read_csv("/tmp/train_set.csv")
        data_train_endpoint=pd.read_csv("/tmp/base_forecasting.csv")
        datos_test = concat([data_preparation_api, data_train_endpoint])
        datos_test = datos_test.drop_duplicates(subset = 'fechahora', keep = 'first')
        datos_test = datos_test[(datos_test.fechahora >= fecha_inicio_test)& (datos_test.fechahora <= fecha_final_test)]
        fecha_test = datos_test[['fechahora']].values
        nombres_datos_test = data_train_endpoint.columns
        nombres_datos_test = nombres_datos_test[0:-1]

        logging.info("nombres_datos_test check")


        prediction_object = Predict()
        demanda_p = prediction_object.final_prediction(model=model, x_test=data_train_endpoint)

        logging.info("demanda_p check")
        

        base_prediccion = prediction_object.fun_dataset_prediccion(ypred=demanda_p, x_test=data_train_endpoint, escalador=scaler, nombres_datos_test=nombres_datos_test, datos_testeo=datos_test, fecha_test=fecha_test)
        base_prediccion2= base_prediccion[['fechahora', 'Prediccion', 'intervalo1']]

        base_prediccion2 = base_prediccion2[ base_prediccion2['fechahora'] < fecha6 ] 

        base_prediccion2.to_csv("/tmp/base_prediccion2.csv", index=False)


        upload_objects_to_container(["/tmp/base_prediccion2.csv"],"container-output")

        logging.info("upload_objects_to_container check")
        
        logging.info("Start final_df")
        f_df = final_df()

        logging.info(f_df.head(5))

        f_df.to_csv("/tmp/final_df.csv")
        upload_objects_to_container(["/tmp/final_df.csv"],"container-output")

        logging.info("upload_objects_to_container final_df check")


    except:
        
        logging.info("Unexpected error ##### init ####: %s", sys.exc_info())
