import os
import json
from datetime import datetime
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import requests

# Load API key
load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

REGION_CODE = "PL"
MAX_RESULTS = 50

# YouTube API call
url = "https://www.googleapis.com/youtube/v3/videos"
params = {
    "part": "snippet,statistics,contentDetails",
    "chart": "mostPopular",
    "regionCode": REGION_CODE,
    "maxResults": MAX_RESULTS,
    "key": API_KEY
}
response = requests.get(url, params=params)

if response.status_code == 200:
    data = response.json()

    # Connect to Azure Blob
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_name = "bronze"

    today = datetime.utcnow().strftime("%Y-%m-%d")
    blob_path = f"{today}/trending_videos.json"  # Azure path inside bronze

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
    blob_client.upload_blob(json.dumps(data, ensure_ascii=False, indent=2), overwrite=True)

    print(f"[✓] Data uploaded to Azure Blob: {container_name}/{blob_path}")
else:
    print(f"[!] Failed to fetch data: {response.status_code}")
    print(response.text)
