import os
import random
from azure.storage.filedatalake import DataLakeServiceClient
from io import BytesIO
from PIL import Image
import pandas as pd

storage_account_name = "mnistdata123456"  # your storage account name
storage_account_key = "uIMQcei8EB1vYwUe/gShtn/kYaGBgs+WqM3wgWaLFkqeWAmn0O1NOhmmBlEpoEVY4oa6BkJD9jXU+AStdyO0Pw=="  # your key
file_system_name = "mnist"  # your container name
base_path = "mnist_png/training"  # inside ADLS

def initialize_storage():
    service_client = DataLakeServiceClient(
        account_url=f"https://{storage_account_name}.dfs.core.windows.net",
        credential=storage_account_key
    )
    return service_client

def get_random_images(service_client):
    file_system_client = service_client.get_file_system_client(file_system_name)
    image_data = []

    for digit in range(10):
        folder_path = f"{base_path}/{digit}"
        paths = list(file_system_client.get_paths(path=folder_path))
        chosen = random.sample(paths, 5)

        for file_path in chosen:
            file_client = file_system_client.get_file_client(file_path.name)
            download = file_client.download_file()
            content = download.readall()
            image = Image.open(BytesIO(content)).convert("L")
            pixels = list(image.getdata())
            image_data.append({
                "label": digit,
                "pixels": pixels,
                "filename": os.path.basename(file_path.name)
            })

    return pd.DataFrame(image_data)

if __name__ == "__main__":
    service_client = initialize_storage()
    df = get_random_images(service_client)
    df.to_pickle("scripts/image_data.pkl")
    print("✅ 5 images per digit saved as pickle.")
