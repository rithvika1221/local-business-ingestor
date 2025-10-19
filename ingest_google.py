import os
import time
import requests
import psycopg2

GOOGLE_KEY = os.getenv("GOOGLE_API_KEY")
DB_URL = os.getenv("DATABASE_URL")

# Example: Bothell, WA center point — change for your city
CENTER_LAT, CENTER_LON = 47.7599, -122.2050

# Define different business categories to query
PLACE_TYPES = [
    "restaurant", "cafe", "bar", "bakery",
    "store", "supermarket", "shopping_mall", "clothing_store",
    "beauty_salon", "hair_care", "spa", "gym",
    "school", "university", "library", "book_store",
    "doctor", "dentist", "hospital", "pharmacy",
    "bank", "atm", "insurance_agency", "real_estate_agency",
    "car_repair", "car_wash", "gas_station",
    "electronics_store", "hardware_store", "home_goods_store",
    "travel_agency", "laundry", "pet_store", "veterinary_care",
    "lawyer", "accounting", "post_office"
]

def get_places(lat, lon, radius=10000, place_type=None):
    """Fetch nearby places for a given type."""
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        "key": GOOGLE_KEY,
        "location": f"{lat},{lon}",
        "radius": radius
    }
    if place_type:
        params["type"] = place_type
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

def place_details(place_id):
    """Fetch detailed info for a given place ID."""
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
    """Insert or update a business in the database."""
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
    total_inserted = 0
    seen_ids = set()

    # Loop through multiple categories until ~200 businesses gathered
    for place_type in PLACE_TYPES:
        if total_inserted >= 500:
            break

        print(f"Fetching type: {place_type}")
        res = get_places(CENTER_LAT, CENTER_LON, radius=10000, place_type=place_type)

        for item in res.get("results", []):
            place_id = item.get("place_id")
            if not place_id or place_id in seen_ids:
                continue
            seen_ids.add(place_id)

            details = place_details(place_id)
            biz = {
                "name": details.get("name"),
                "address": details.get("formatted_address"),
                "lat": details.get("geometry", {}).get("location", {}).get("lat"),
                "lon": details.get("geometry", {}).get("location", {}).get("lng"),
                "phone": details.get("formatted_phone_number"),
                "website": details.get("website"),
                "place_id": place_id,
                "category": (details.get("types") or [None])[0],
                "rating": details.get("rating"),
                "rating_count": details.get("user_ratings_total"),
            }
            upsert_business(conn, biz)
            total_inserted += 1
            if total_inserted >= 500:
                break
            time.sleep(0.2)  # small delay to respect rate limits

        time.sleep(1)  # small pause between categories

    conn.commit()
    conn.close()
    print(f"✅ Inserted or updated {total_inserted} total businesses.")

if __name__ == "__main__":
    main()
