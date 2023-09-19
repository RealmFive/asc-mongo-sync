import os
from pymongo import MongoClient, ReplaceOne
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

def get_sync_start(collection):
  if os.getenv("SYNC_START") is not None:
    return datetime.fromisoformat(os.getenv("SYNC_START")).astimezone(timezone.utc)
  
  latest_sync_status = collection.find().sort("syncStop", -1).limit(1)
  return latest_sync_status[0]["syncStop"]

def sync_databases(source_db, source_collection, destination_db):
  for col in source_collection:
    source_col = source_db[col]
    destination_col= destination_db[col]

    docs = source_col.find({'updatedAt': {'$gte': sync_start, '$lt': sync_stop}})
    updates = [ReplaceOne({"_id": doc["_id"]}, doc, upsert = True) for doc in docs]

    if len(updates) > 0: destination_col.bulk_write(updates)

script_start = datetime.now(tz=timezone.utc)
sync_stop = script_start

load_dotenv()

cloud_client = MongoClient(os.getenv("CLOUD_DATABASE_URI"))
cloud_db = cloud_client["optrack"]
cloud_collections = ["boundaries", "devices", "installations", "members", "organizations", "scalehouses"]

local_client = MongoClient(os.getenv("LOCAL_DATABASE_URI"))
local_db = local_client["optrack"]
local_collections = ["loadtickets", "scaleEvents"]

sync_status_collection = local_db["syncstatus"]
sync_start = get_sync_start(sync_status_collection)

sync_databases(cloud_db, cloud_collections, local_db) # sync from cloud to local
sync_databases(local_db, local_collections, cloud_db) # sync from local to cloud

sync_status = {
    "syncStart": sync_start,
    "syncStop": sync_stop,
    "scriptStart": script_start,
    "scriptStop": datetime.now(tz=timezone.utc),
}

sync_status_collection.insert_one(sync_status)
