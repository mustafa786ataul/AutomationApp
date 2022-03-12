import azure
from azure.storage.blob import BlobServiceClient

blob_service = BlobServiceClient(account_url= 'https://riteststorage.blob.core.windows.net', credential='Grd504drYmO6q4bG3pk/E3YQtLwRtx8mJj4B5MaP9ymCNJxRCAH4t8b7PwJxiIPbK984FhAINCtow7/YW6IlCg==')
