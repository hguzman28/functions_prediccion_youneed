from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from os import getenv
from typing import BinaryIO

load_dotenv()


blob_service_client = BlobServiceClient.from_connection_string(
    getenv("AZURE_STORAGE_CONNECTION_STRING"))

def get_all_object(container_name):
    try:
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
            with open(blob_name, "wb") as file:
                file.write(blob_client.download_blob().readall())

            # Guardar los datos del blob en memoria o en una lista
            blob_data_list.append(blob_name)

        return blob_data_list

    except Exception as e:
        print(e.message) 



def upload_objects_to_container(object_list, destination_container_name):
    try:
        container_client = blob_service_client.get_container_client(destination_container_name)
        
        for object_name in object_list:
            with open(object_name, "rb") as file:
                container_client.upload_blob(name=object_name, data=file)
            
            print(f"Objeto {object_name} subido al contenedor {destination_container_name}")
    
    except Exception as e:
        print("Error al subir los objetos al contenedor:", str(e))