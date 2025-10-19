import os
import time
import requests
import psycopg2

GOOGLE_KEY = os.getenv("GOOGLE_API_KEY")
DB_URL = os.getenv("DATABASE_URL")

# Example city center (change to your area)
CENTER_LAT, CENTER_LON = 47.7599, -122.2050  # Bothell, WA

def get_places(lat, lon, radius=2500, keyword=None):
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        "key": GOOGLE_KEY,
        "location": f"{lat},{lon}",
        "radius": radius,
        "type": "restaurant"  # change type if you want other categories
    }
    if keyword:
        params["keyword"] = keyword
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

def place_details(place_id):
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "key": GOOGLE_KEY,
        "place_id": place_id,
        "fields": "name,formatted_address,geometry,formatted_phone_number,website,types,rating,user_ratings_total"
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json().get("result", {})

def upsert_business(conn, data):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO businesses (name, category, address, lat, lon, phone, website, google_place_id, rating, rating_count)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
          updated_at=now();
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
        ))

def main():
    conn = psycopg2.connect(DB_URL)
    total = 0

    while total < 200:
        res = get_places(CENTER_LAT, CENTER_LON, radius=3000)
        for item in res.get("results", []):
            details = place_details(item["place_id"])
            biz = {
                "name": details.get("name"),
                "address": details.get("formatted_address"),
                "lat": details.get("geometry", {}).get("location", {}).get("lat"),
                "lon": details.get("geometry", {}).get("location", {}).get("lng"),
                "phone": details.get("formatted_phone_number"),
                "website": details.get("website"),
                "place_id": item["place_id"],
                "category": (details.get("types") or [None])[0],
                "rating": details.get("rating"),
                "rating_count": details.get("user_ratings_total"),
            }
            upsert_business(conn, biz)
            total += 1
            if total >= 200:
                break
            time.sleep(0.2)

        if total >= 200:
            break

    conn.commit()
    conn.close()
    print(f"Inserted {total} businesses successfully.")

if __name__ == "__main__":
    main()
