import os
import time
import requests
import psycopg2
import json
from datetime import date, timedelta
from pathlib import Path
from bs4 import BeautifulSoup

# ==============================
# SCRAPING HELPER
# ==============================
def scrape_website(website_url):
    """Scrape a website for meta description and potential menu links."""
    if not website_url:
        return None
    try:
        resp = requests.get(website_url, timeout=6)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract meta description
        meta_desc = None
        meta_tag = soup.find("meta", attrs={"name": "description"})
        if meta_tag and meta_tag.get("content"):
            meta_desc = meta_tag["content"]

        # Extract menu URLs (look for 'menu' in href)
        menu_links = [
            a["href"]
            for a in soup.find_all("a", href=True)
            if "menu" in a["href"].lower()
        ][:3]

        return {"meta_description": meta_desc, "menu_links": menu_links}
    except Exception as e:
        print(f"⚠️ Failed to scrape {website_url}: {e}")
        return None


# ==============================
# DB HELPER FOR WEBSITE EXTRAS
# ==============================
def insert_extras(conn, business_id, extras):
    """Insert scraped meta description and menu links."""
    if not extras:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
        INSERT INTO business_extras (business_id, meta_description, menu_links)
        VALUES (%s,%s,%s)
        ON CONFLICT (business_id) DO UPDATE SET
          meta_description=EXCLUDED.meta_description,
          menu_links=EXCLUDED.menu_links;
        """,
            (
                business_id,
                extras.get("meta_description"),
                extras.get("menu_links"),
            ),
        )


# ==============================
# CONFIG
# ==============================
GOOGLE_KEY = os.getenv("GOOGLE_API_KEY")
DB_URL = os.getenv("DATABASE_URL")
CENTER_LAT, CENTER_LON = 47.7599, -122.2050  # Bothell, WA

PLACE_TYPES = [
    "restaurant", "cafe", "store", "gym", "hospital", "school",
    "bank", "beauty_salon", "book_store", "real_estate_agency",
    "lawyer", "electronics_store", "travel_agency", "pet_store",
    "supermarket", "clothing_store", "pharmacy"
]


# ==============================
# GOOGLE API CALLS
# ==============================
def get_places(lat, lon, radius=10000, place_type=None, pagetoken=None):
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {"key": GOOGLE_KEY, "location": f"{lat},{lon}", "radius": radius}
    if place_type:
        params["type"] = place_type
    if pagetoken:
        params["pagetoken"] = pagetoken
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()


def place_details(place_id, retries=3):
    """Get detailed info for one place (retry up to 3 times if incomplete)."""
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "key": GOOGLE_KEY,
        "place_id": place_id,
        "fields": (
            "formatted_address,formatted_phone_number,website,"
            "types,rating,user_ratings_total,reviews,opening_hours,photos,"
            "price_level,editorial_summary,google_maps_uri,"
            "curbside_pickup,delivery,dine_in,reservable,"
            "serves_breakfast,serves_lunch,serves_dinner,serves_beer,serves_wine,takeout"
        ),
    }

    for i in range(retries):
        try:
            r = requests.get(url, params=params)
            r.raise_for_status()
            result = r.json().get("result", {})
            if "formatted_address" in result or "website" in result:
                return result
            time.sleep(1.5)
        except Exception as e:
            print(f"⚠️ Place details retry {i+1} failed for {place_id}: {e}")
            time.sleep(2)
    return {}


# ==============================
# PHOTO DOWNLOADER
# ==============================
def download_photo(photo_ref, place_id):
    """Download photo once and return local path or placeholder."""
    Path("public/images").mkdir(parents=True, exist_ok=True)
    file_path = f"public/images/{place_id}.jpg"

    if os.path.exists(file_path):
        return file_path

    if not photo_ref:
        # use a placeholder image
        placeholder = "public/images/placeholder.jpg"
        if not os.path.exists(placeholder):
            # create small blank file to avoid 404 in frontend
            with open(placeholder, "wb") as f:
                f.write(b"")
        return placeholder

    url = "https://maps.googleapis.com/maps/api/place/photo"
    params = {"maxwidth": 800, "photo_reference": photo_ref, "key": GOOGLE_KEY}
    resp = requests.get(url, params=params, stream=True)
    if resp.status_code == 200:
        with open(file_path, "wb") as f:
            for chunk in resp.iter_content(1024):
                f.write(chunk)
        print(f"✅ Saved image {file_path}")
        return file_path
    else:
        print(f"⚠️ Failed to download photo for {place_id}: {resp.status_code}")
        return "public/images/placeholder.jpg"


# ==============================
# UPSERT BUSINESS
# ==============================
def upsert_business(conn, data):
    with conn.cursor() as cur:
        cur.execute(
            """
        INSERT INTO businesses (name, category, address, lat, lon, phone, website,
                                google_place_id, rating, rating_count, opening_hours,
                                photo_path, description, price_level, maps_url)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
          description=EXCLUDED.description,
          price_level=EXCLUDED.price_level,
          maps_url=EXCLUDED.maps_url,
          updated_at=now()
        RETURNING id;
        """,
            (
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
                data.get("description"),
                data.get("price_level"),
                data.get("maps_url"),
            ),
        )
        return cur.fetchone()[0]


# ==============================
# REVIEWS & DEALS
# ==============================
def insert_reviews(conn, business_id, reviews):
    if not reviews:
        return
    with conn.cursor() as cur:
        for r in reviews[:5]:
            cur.execute(
                """
            INSERT INTO google_reviews (business_id, author_name, rating, text, relative_time)
            VALUES (%s,%s,%s,%s,%s)
            """,
                (
                    business_id,
                    r.get("author_name"),
                    r.get("rating"),
                    r.get("text"),
                    r.get("relative_time_description"),
                ),
            )


def insert_deal(conn, business_id, category):
    sample_deals = {
        "restaurant": "10% off your first order!",
        "cafe": "Buy 1 Get 1 Free Latte",
        "gym": "Free first week membership",
        "store": "20% off all products this week",
    }
    title = "Special Offer"
    description = sample_deals.get(category, "Exclusive Local Deal")
    with conn.cursor() as cur:
        cur.execute(
            """
        INSERT INTO deals (business_id, title, description, valid_from, valid_until)
        VALUES (%s,%s,%s,%s,%s)
        """,
            (
                business_id,
                title,
                description,
                date.today(),
                date.today() + timedelta(days=30),
            ),
        )


# ==============================
# MAIN
# ==============================
def main():
    conn = psycopg2.connect(DB_URL)
    total_inserted = 0
    seen = set()

    for place_type in PLACE_TYPES:
        if total_inserted >= 10:
            break

        print(f"Fetching category: {place_type}")
        res = get_places(CENTER_LAT, CENTER_LON, radius=10000, place_type=place_type)
        next_page = None

        while True:
            for item in res.get("results", []):
                pid = item.get("place_id")
                name = item.get("name")

                if not pid or not name or pid in seen:
                    continue
                seen.add(pid)

                details = place_details(pid)
                # merge Nearby and Details info
                photo_ref = (
                    details.get("photos", [{}])[0].get("photo_reference")
                    if details.get("photos")
                    else item.get("photos", [{}])[0].get("photo_reference")
                    if item.get("photos")
                    else None
                )
                photo_path = download_photo(photo_ref, pid)

                biz = {
                    "name": name,
                    "address": details.get("formatted_address") or item.get("vicinity"),
                    "lat": item.get("geometry", {}).get("location", {}).get("lat"),
                    "lon": item.get("geometry", {}).get("location", {}).get("lng"),
                    "phone": details.get("formatted_phone_number", "N/A"),
                    "website": details.get("website", "N/A"),
                    "place_id": pid,
                    "category": (details.get("types") or item.get("types") or [None])[0],
                    "rating": details.get("rating") or item.get("rating"),
                    "rating_count": details.get("user_ratings_total")
                    or item.get("user_ratings_total"),
                    "opening_hours": json.dumps(details.get("opening_hours"))
                    if details.get("opening_hours")
                    else None,
                    "photo_path": photo_path,
                    "description": details.get("editorial_summary", {}).get("overview"),
                    "price_level": details.get("price_level") or item.get("price_level"),
                    "maps_url": details.get("google_maps_uri"),
                }

                business_id = upsert_business(conn, biz)

                extras = scrape_website(details.get("website"))
                if extras:
                    insert_extras(conn, business_id, extras)

                insert_reviews(conn, business_id, details.get("reviews"))
                insert_deal(conn, business_id, biz["category"])

                total_inserted += 1
                if total_inserted >= 10:
                    break
                time.sleep(0.5)

            if total_inserted >= 10:
                break

            next_page = res.get("next_page_token")
            if not next_page:
                break

            time.sleep(2)
            res = get_places(
                CENTER_LAT,
                CENTER_LON,
                radius=4000,
                place_type=place_type,
                pagetoken=next_page,
            )

        time.sleep(1)

    conn.commit()
    conn.close()
    print(f"✅ Inserted {total_inserted} businesses with reviews, deals, and local photos.")


if __name__ == "__main__":
    main()
