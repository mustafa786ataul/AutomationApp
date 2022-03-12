from azure.storage.blob import BlobServiceClient
import pandas as pd

STORAGEACCOUNTURL= "https://riteststorage.blob.core.windows.net"
STORAGEACCOUNTKEY= "Grd504drYmO6q4bG3pk/E3YQtLwRtx8mJj4B5MaP9ymCNJxRCAH4t8b7PwJxiIPbK984FhAINCtow7/YW6IlCg=="
LOCALFILENAME= "CompletedFile.xlsx"
CONTAINERNAME= "ricalculationfile"

blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
blob_client_instance = blob_service_client_instance.get_blob_client(CONTAINERNAME+'/US_RAW_FILE', LOCALFILENAME, snapshot=None)

print("\nUploading to Azure Storage as blob:\n\t" + LOCALFILENAME)

# Upload the created file
with open(LOCALFILENAME, "rb") as data:
    blob_client_instance.upload_blob(data)