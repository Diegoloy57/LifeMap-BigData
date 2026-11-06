
import json
import os
from pathlib import Path

import dask.bag as dbag
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

DATA_FILE = Path("/app/data/raw/yelp_academic_dataset_business.json")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")

DATABASE_NAME = "lifemap"
COLLECTION_NAME = "businesses"

PARTITION_SIZE = "16MB"
BATCH_SIZE = 500


def clean_business(record):
    try:
        business = json.loads(record)

        lat = business.get("latitude")
        lon = business.get("longitude")

        if lat is None or lon is None:
            return None

        lat = float(lat)
        lon = float(lon)

        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return None

        if lat == 0 and lon == 0:
            return None

        business_id = business.get("business_id")

        if not business_id:
            return None

        return {
            "business_id": business_id,
            "name": business.get("name"),
            "city": business.get("city"),
            "state": business.get("state"),
            "categories": business.get("categories"),
            "stars": business.get("stars"),
            "review_count": business.get("review_count"),
            "is_open": business.get("is_open"),
            "location": {
                "type": "Point",
                "coordinates": [lon, lat]
            }
        }

    except (ValueError, TypeError, json.JSONDecodeError):
        return None


def save_partition(records):
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000
    )
    collection = client[DATABASE_NAME][COLLECTION_NAME]

    batch = []
    inserted = 0

    try:
        for record in records:
            operation = UpdateOne(
                {"business_id": record["business_id"]},
                {"$set": record},
                upsert=True
            )

            batch.append(operation)

            if len(batch) >= BATCH_SIZE:
                collection.bulk_write(batch, ordered=False)
                inserted += len(batch)
                batch.clear()

        if batch:
            collection.bulk_write(batch, ordered=False)
            inserted += len(batch)

    finally:
        client.close()

    return inserted


def main():
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"No existe: {DATA_FILE}")

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000
    )

    collection = client[DATABASE_NAME][COLLECTION_NAME]
    collection.create_index("business_id", unique=True)
    collection.create_index([("location", "2dsphere")])

    client.close()

    print("Leyendo negocios de Yelp con Dask...")

    records = dbag.read_text(
        str(DATA_FILE),
        blocksize=PARTITION_SIZE
    )

    cleaned = records.map(clean_business).filter(
        lambda item: item is not None
    )

    results = cleaned.map_partitions(save_partition).compute(
        scheduler="threads",
        num_workers=2
    )

    print(f"Negocios procesados: {sum(results):,}")
    print("Carga finalizada en MongoDB.")


if __name__ == "__main__":
    main()
