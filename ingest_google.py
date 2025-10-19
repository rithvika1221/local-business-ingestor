import os
import time
import requests
import psycopg2
from datetime import date, timedelta
from pathlib import Path

GOOGLE_KEY = os.getenv("GOOGLE_API_KEY")
DB_URL = os.getenv("DATABASE_URL")

CENTER_LAT, CENTER_LON = 47.7599, -122.2050  # Bothell, WA

# Create local image folder if it doesn't exist
Path("images").mkdir(exist_ok=True)

PLACE_TYPES = [
    "restaurant", "cafe", "store", "gym", "hospital", "school",
    "bank", "beauty_salon", "book_store", "real_estate_agency",
    "lawyer", "electronics_store", "travel_agency", "pet_store",
    "supermarket", "clothing_store", "pharmacy"
]

def get_places(lat, lon, radius=3000, place_type=None, pagetoken=None):
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {"key": GOOGLE_KEY, "location": f"{lat},{lon}", "radius": radius}
    if place_type:
        params["type"] = place_type
    if pagetoken:
        params["pagetoken"] = pagetoken
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

def place_details(place_id):
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "key": GOOGLE_KEY,
        "place_id": place_id,
        "fields": (
            "name,formatted_address,geometry,formatted_phone_number,website,"
            "types,rating,user_ratings_total,reviews,opening_hours,photos"
        )
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json().get("result", {})

def download_photo(photo_ref, place_id):
    """Download photo once and return local path."""
    if not photo_ref:
        return None
    file_path = f"images/{place_id}.jpg"
    if os.path.exists(file_path):
        return file_path

    url = "https://maps.googleapis.com/maps/api/place/photo"
    params = {"maxwidth": 800, "photo_reference": photo_ref, "key": GOOGLE_KEY}
    resp = requests.get(url, params=params, stream=True)
    if resp.status_code == 200:
        with open(file_path, "wb") as f:
            for chunk in resp.iter_content(1024):
                f.write(chunk)
        return file_path
    return None

def upsert_business(conn, data):
    """Insert or update a business and return its ID."""
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO businesses (name, category, address, lat, lon, phone, website,
                                google_place_id, rating, rating_count, opening_hours, photo_path)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (google_place_id) DO UPDATE SET
          name=EXCLUDED.name,
          category=EXCLUDED.category,
          address=EXCLUDED.address,
          lat=EXCLUDED.lat,
          lon=EXCLUDED.lon,
          phone=EXCLUDED.phone,
          website=EXCLUDED.website,
          rating=EXCLUDED.rating,
          rating_count=EXCLUDED.rating_count,
          opening_hours=EXCLUDED.opening_hours,
          photo_path=EXCLUDED.photo_path,
          updated_at=now()
        RETURNING id;
        """, (
            data.get("name"),
            data.get("category"),
            data.get("address"),
            data.get("lat"),
            data.get("lon"),
            data.get("phone"),
            data.get("website"),
            data.get("place_id"),
            data.get("rating"),
            data.get("rating_count"),
            data.get("opening_hours"),
            data.get("photo_path"),
        ))
        return cur.fetchone()[0]

def insert_reviews(conn, business_id, reviews):
    """Insert up to 5 reviews for each business."""
    if not reviews:
        return
    with conn.cursor() as cur:
        for r in reviews[:5]:
            cur.execute("""
            INSERT INTO google_reviews (business_id, author_name, rating, text, relative_time)
            VALUES (%s,%s,%s,%s,%s)
            """, (
                business_id,
                r.get("author_name"),
                r.get("rating"),
                r.get("text"),
                r.get("relative_time_description")
            ))

def insert_deal(conn, business_id, category):
    """Insert a fake or example deal to demonstrate functionality."""
    sample_deals = {
        "restaurant": "10% off your first order!",
        "cafe": "Buy 1 Get 1 Free Latte",
        "gym": "Free first week membership",
        "store": "20% off all products this week"
    }
    title = "Special Offer"
    description = sample_deals.get(category, "Exclusive Local Deal")
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO deals (business_id, title, description, valid_from, valid_until)
        VALUES (%s,%s,%s,%s,%s)
        """, (
            business_id,
            title,
            description,
            date.today(),
            date.today() + timedelta(days=30)
        ))

def main():
    conn = psycopg2.connect(DB_URL)
    total_inserted = 0
    seen = set()

    for place_type in PLACE_TYPES:
        if total_inserted >= 200:
            break

        print(f"Fetching category: {place_type}")
        res = get_places(CENTER_LAT, CENTER_LON, radius=4000, place_type=place_type)
        next_page = None

        while True:
            for item in res.get("results", []):
                pid = item.get("place_id")
                if not pid or pid in seen:
                    continue
                seen.add(pid)

                details = place_details(pid)
                photo_ref = None
                if details.get("photos"):
                    photo_ref = details["photos"][0].get("photo_reference")

                photo_path = download_photo(photo_ref, pid)
                biz = {
                    "name": details.get("name"),
                    "address": details.get("formatted_address"),
                    "lat": details.get("geometry", {}).get("location", {}).get("lat"),
                    "lon": details.get("geometry", {}).get("location", {}).get("lng"),
                    "phone": details.get("formatted_phone_number"),
                    "website": details.get("website"),
                    "place_id": pid,
                    "category": (details.get("types") or [None])[0],
                    "rating": details.get("rating"),
                    "rating_count": details.get("user_ratings_total"),
                    "opening_hours": details.get("opening_hours"),
                    "photo_path": photo_path
                }

                business_id = upsert_business(conn, biz)
                insert_reviews(conn, business_id, details.get("reviews"))
                insert_deal(conn, business_id, biz["category"])

                total_inserted += 1
                if total_inserted >= 200:
                    break
                time.sleep(0.3)

            if total_inserted >= 200:
                break
            next_page = res.get("next_page_token")
            if not next_page:
                break
            time.sleep(2)
            res = get_places(CENTER_LAT, CENTER_LON, radius=4000, place_type=place_type, pagetoken=next_page)

        time.sleep(1)

    conn.commit()
    conn.close()
    print(f"✅ Inserted {total_inserted} businesses with reviews, deals, and local photos.")

if __name__ == "__main__":
    main()
