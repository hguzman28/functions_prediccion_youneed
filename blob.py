from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from os import getenv
from typing import BinaryIO
import logging

load_dotenv()



def get_all_object(container_name):
    try:
        logging.info("INICIO get_all_object")
        blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sas3azureblobs;AccountKey=pP+l43qRKCGTEsTqjCUqfuKAGtsLeup0Drvwid4k2YYQZIt+X5g/U1x8TxRz+cpquSwt25PSgsrr+AStUd0ygg==;EndpointSuffix=core.windows.net")

        # # TODO: Replace <storage-account-name> with your actual storage account name
        # account_url = "https://<storage-account-name>.blob.core.windows.net"
        # connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

        # Create the BlobServiceClient object
        # blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        container_client = blob_service_client.get_container_client(container_name)
        # Obtener una lista de todos los blobs en el contenedor
        blob_list = container_client.list_blobs()

        # Lista para almacenar los datos de los blobs
        blob_data_list = []

        # Iterar sobre la lista de blobs y guardarlos localmente
        print("###### Cantidad ########")
        # print("Cantidad de objetos: "+(str(len(blob_list))))
        print(blob_list)

        for blob in blob_list:
            # Obtener el nombre del blob
            blob_name = blob.name
            print("###### NAME ########")
            print(blob_name)

            # Descargar el blob y guardar los datos localmente en un archivo
            blob_client = container_client.get_blob_client(blob_name)

            filepath = '/tmp/' + blob_name
            with open(filepath, "wb") as file:
                file.write(blob_client.download_blob().readall())

            # Guardar los datos del blob en memoria o en una lista
            blob_data_list.append(blob_name)

        return blob_data_list

    except Exception as e:
        logging.info('ERRORRR get_all_object')
        logging.info(e)
        print(e.message) 



def upload_objects_to_container(object_list, destination_container_name):

    try:
        blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sas3azureblobs;AccountKey=pP+l43qRKCGTEsTqjCUqfuKAGtsLeup0Drvwid4k2YYQZIt+X5g/U1x8TxRz+cpquSwt25PSgsrr+AStUd0ygg==;EndpointSuffix=core.windows.net")

        logging.info("INICIO upload_objects_to_container")
        container_client = blob_service_client.get_container_client(destination_container_name)
        
        for object_name in object_list:
            # filepath = '/tmp/' + object_name
            with open(object_name, "rb") as file:
                container_client.upload_blob(name=object_name, data=file)
            
            print(f"Objeto {object_name} subido al contenedor {destination_container_name}")
    
    except Exception as e:
        logging.info('ERROR:::: upload_objects_to_container')
        logging.info(e)
        print("Error al subir los objetos al contenedor:", str(e))