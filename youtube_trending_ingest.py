import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv


# 🔐 Load environment variables from .env
load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")


# 🔧 Replace this with your actual API key
REGION_CODE = "PL"
MAX_RESULTS = 50

# 📁 Output folder (local, to simulate Bronze)
today = datetime.utcnow().strftime("%Y-%m-%d")
output_dir = f"/Users/aliyusifov/Desktop/youtube_trend_analysis/datalake_demo/bronze/{today}"
os.makedirs(output_dir, exist_ok=True)

# 📡 YouTube API Endpoint
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
    output_path = os.path.join(output_dir, "trending_videos.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"[✓] Trending data saved to {output_path}")
else:
    print(f"[!] Failed to fetch data: {response.status_code}")
    print(response.text)
