import unittest
import mongomock
import database_sync
import os
from datetime import datetime, timezone, timedelta

class DatabaseSyncTest(unittest.TestCase):
  def test_get_sync_start_with_sync_status(self):
    del os.environ["SYNC_START"]
    collection = mongomock.MongoClient().db.collection
    expected_sync_start = script_start = sync_stop = datetime(2023, 9, 17, 12, 30, 0)
    sync_status = {
      "syncStart": sync_stop - timedelta(minutes=5),
      "syncStop": sync_stop,
      "scriptStart": script_start,
      "scriptStop": script_start + timedelta(seconds=15),
    }
    collection.insert_one(sync_status)

    sync_start = database_sync.DatabaseSync.get_sync_start(collection)
    self.assertEqual(sync_start, expected_sync_start)

  def test_get_sync_start_with_env_var(self):
    os.environ["SYNC_START"] = "2023-09-19 12:00:00.000"
    expected_sync_start = datetime.fromisoformat(os.getenv("SYNC_START")).astimezone(timezone.utc)
    collection = mongomock.MongoClient().db.collection

    sync_start = database_sync.DatabaseSync.get_sync_start(collection)
    self.assertEqual(sync_start, expected_sync_start)
    
  def test_sync_databases(self):
    source_db = mongomock.MongoClient().source_db
    source_collection = source_db.source_collection
    doc = {
        "source": "source_collection",
        "updatedAt": datetime(2023, 9, 17, 12, 0, 0)
    }
    source_collection.insert_one(doc)

    destination_db = mongomock.MongoClient().destination_db
    sync_start = datetime(2023, 9, 17, 11, 55, 0)
    sync_stop = datetime(2023, 9, 17, 12, 5, 0)
    database_sync.DatabaseSync.sync_databases(source_db, ['source_collection'], destination_db, sync_start, sync_stop)

    expected_doc = destination_db.source_collection.find_one()
    self.assertEqual(doc["source"], expected_doc["source"])
    self.assertEqual(doc["updatedAt"], expected_doc["updatedAt"])

    # update doc to test upsert
    filter = { "source": "source_collection" }
    expected_value = "source_collection 2"
    expected_updated_at = datetime(2023, 9, 17, 12, 30, 0)
    new_values = { "$set": { "source": expected_value, 'updatedAt': expected_updated_at} }
    source_collection.update_one(filter, new_values)

    sync_start = datetime(2023, 9, 17, 12, 25, 0)
    sync_stop = datetime(2023, 9, 17, 12, 35, 0)
    database_sync.DatabaseSync.sync_databases(source_db, ['source_collection'], destination_db, sync_start, sync_stop)

    expected_doc = destination_db.source_collection.find_one()
    self.assertEqual(expected_doc["source"], expected_value)
    self.assertEqual(expected_doc["updatedAt"], expected_updated_at)
    self.assertEqual(source_db['source_collection'].count_documents({}), 1)
    self.assertEqual(destination_db['source_collection'].count_documents({}), 1)
  
  def test_insert_sync_status(self):
    collection = mongomock.MongoClient().db.collection
    sync_start = datetime(2023, 9, 17, 12, 25, 0)
    script_start = sync_stop = sync_start + timedelta(minutes=5)
    database_sync.DatabaseSync.insert_sync_status(collection, sync_start, sync_stop, script_start)

    expected_sync_status = collection.find_one()
    self.assertEqual(expected_sync_status["syncStart"], sync_start)
    self.assertEqual(expected_sync_status["syncStop"], sync_stop)
    self.assertEqual(expected_sync_status["scriptStart"], script_start)
    self.assertEqual(expected_sync_status["scriptStop"].date(), datetime.now(tz=timezone.utc).date())

unittest.main()
