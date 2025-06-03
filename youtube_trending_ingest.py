# youtube_trending_ingest.py

import os
import json
from datetime import datetime
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
import requests

def fetch_trending_videos(api_key, region_code="PL", max_results_per_page=50):
    """
    Fetches trending videos from the YouTube Data API, handling pagination.

    Args:
        api_key (str): Your YouTube Data API v3 key.
        region_code (str): The ISO 3166-1 alpha-2 country code (e.g., 'PL' for Poland).
        max_results_per_page (int): Max results per API call (YouTube API max is 50).

    Returns:
        list: A list of dictionaries, each representing a trending video item.
              Returns an empty list if an error occurs or no videos are found.
    """
    all_videos = []
    next_page_token = None
    url = "https://www.googleapis.com/youtube/v3/videos"

    print(f"Starting to fetch trending videos for region: {region_code}")

    while True:
        params = {
            "part": "snippet,statistics,contentDetails",
            "chart": "trending", 
            "regionCode": region_code,
            "maxResults": max_results_per_page,
            "key": api_key
        }
        if next_page_token:
            params["pageToken"] = next_page_token

        try:
            response = requests.get(url, params=params)
            response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
            data = response.json()

            items = data.get("items", [])
            all_videos.extend(items)
            print(f"Fetched {len(items)} videos. Total fetched: {len(all_videos)}")

            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break # No more pages to fetch

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            print(f"Response content: {response.text}")
            break
        except requests.exceptions.ConnectionError as conn_err:
            print(f"Connection error occurred: {conn_err}")
            break
        except requests.exceptions.Timeout as timeout_err:
            print(f"Timeout error occurred: {timeout_err}")
            break
        except requests.exceptions.RequestException as req_err:
            print(f"An unexpected request error occurred: {req_err}")
            break
        except json.JSONDecodeError as json_err:
            print(f"JSON decode error: {json_err}. Response was: {response.text}")
            break
        except Exception as e:
            print(f"An unknown error occurred: {e}")
            break
    
    print(f"Finished fetching. Total videos collected: {len(all_videos)}")
    return all_videos

def upload_to_azure_blob(data, connection_string, container_name, blob_path):
    """
    Uploads JSON data to Azure Blob Storage.

    Args:
        data (dict or list): The data to upload.
        connection_string (str): Azure Storage Account connection string.
        container_name (str): The name of the blob container.
        blob_path (str): The path within the container (e.g., '2023-10-27/trending_videos.json').
    """
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        
        # Convert data to JSON string with pretty printing and ensure_ascii=False for Polish characters
        json_data_string = json.dumps(data, ensure_ascii=False, indent=2)
        
        blob_client.upload_blob(json_data_string, overwrite=True)
        print(f"[✓] Data uploaded to Azure Blob: {container_name}/{blob_path}")
    except Exception as e:
        print(f"[!] Failed to upload data to Azure Blob: {e}")

def main():
    # Load environment variables (for local testing)
    load_dotenv()

    # Retrieve API key and Azure connection string from environment variables
    # In GitHub Actions, these are automatically picked up from secrets
    api_key = os.getenv("YOUTUBE_API_KEY")
    azure_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    if not api_key:
        print("Error: YOUTUBE_API_KEY environment variable not set.")
        exit(1)
    if not azure_connection_string:
        print("Error: AZURE_STORAGE_CONNECTION_STRING environment variable not set.")
        exit(1)

    region_code = "PL"
    container_name = "bronze"
    
    today = datetime.utcnow().strftime("%Y-%m-%d")
    timestamp = datetime.utcnow().strftime("%H%M%S")
    blob_path = f"{today}/trending_videos_{timestamp}.json" 

    # 1. Fetch data from YouTube API
    trending_videos_data = fetch_trending_videos(api_key, region_code)

    if trending_videos_data:
        output_data = trending_videos_data # Uploading the list of items directly

        # 2. Upload data to Azure Blob Storage
        upload_to_azure_blob(output_data, azure_connection_string, container_name, blob_path)
    else:
        print("[!] No trending videos fetched. Skipping Azure Blob upload.")

if __name__ == "__main__":
    main()
