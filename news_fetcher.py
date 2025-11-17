import os
import json
import time
import requests
from datetime import datetime

NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")

INPUT_DIR = "/opt/app/data/input"

os.makedirs(INPUT_DIR, exist_ok=True)


def fetch_news():
    if not NEWSAPI_KEY:
        print("ERROR: NEWSAPI_KEY is missing!")
        return

    url = f"https://newsapi.org/v2/top-headlines?country=us&apiKey={NEWSAPI_KEY}"

    try:
        r = requests.get(url, timeout=10)
        data = r.json()

        if data.get("status") != "ok":
            print("API ERROR:", data)
            return

        articles = data.get("articles", [])

        for i, article in enumerate(articles):
            record = {
                "publishedAt": article.get("publishedAt"),
                "source": article.get("source", {}).get("name"),
                "title": article.get("title"),
                "description": article.get("description")
            }

            # Filename format: article_YYYYMMDD_HHMMSS_i.json
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            fname = os.path.join(INPUT_DIR, f"article_{timestamp}_{i}.json")

            with open(fname, "w") as f:
                json.dump(record, f)

            print(f"Saved: {fname}")

    except Exception as e:
        print("ERROR fetching:", e)


if __name__ == "__main__":
    print("Fetcher running...")
    while True:
        fetch_news()
        time.sleep(20)    # fetch every 20 seconds
