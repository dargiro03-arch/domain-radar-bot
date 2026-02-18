import requests
import feedparser
import hashlib
from datetime import datetime
import os

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

TABLE_URL = f"{SUPABASE_URL}/rest/v1/startups"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

FEEDS = [
    "https://techcrunch.com/feed/",
    "https://sifted.eu/feed/",
    "https://www.eu-startups.com/feed/"
]

def extract_name(title):
    import re
    words = re.findall(r"\b[A-Z][a-zA-Z]+\b", title)
    if not words:
        return None
    return words[0]

def generate_id(url):
    return hashlib.md5(url.encode()).hexdigest()

def process():
    for feed_url in FEEDS:
        feed = feedparser.parse(feed_url)

        for entry in feed.entries[:10]:
            title = entry.title
            link = entry.link
            summary = getattr(entry, "summary", "")

            name = extract_name(title)
            if not name:
                continue

            data = {
                "external_id": generate_id(link),
                "name": name,
                "raw_title": title,
                "description": summary[:200],
                "source_url": link,
                "source_type": "WebScan",
                "rank_score": 50,
                "tm_risk": "MED",
                "dom_risk": "MED",
                "verification_status": "UNVERIFIED",
                "confidence": 0,
                "created_at": datetime.utcnow().isoformat()
            }

            r = requests.post(TABLE_URL, json=data, headers=HEADERS)
            print(r.status_code, r.text)

if __name__ == "__main__":
    process()
