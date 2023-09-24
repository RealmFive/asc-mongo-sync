import os
from pymongo import MongoClient, ReplaceOne
from dotenv import load_dotenv
import sys
from datetime import datetime, timezone
import re

class DatabaseSync:

  # List of environmental variables that are required
  #  for the app to run
  REQUIRED_ENVIRONMENTAL_VARIABLES = ["CLOUD_DATABASE_URI",
                                      "LOCAL_DATABASE_URI",
                                      "CLOUD_DB_NAME",
                                      "LOCAL_DB_NAME",
                                      "CLOUD_TO_LOCAL_COLLECTIONS",
                                      "LOCAL_TO_CLOUD_COLLECTIONS"
                                      ]

  def get_sync_start(collection):
    print('Getting sync start...')
    if os.environ.get("SYNC_START", None) is not None:
      return datetime.fromisoformat(os.environ["SYNC_START"]).astimezone(timezone.utc)
    
    latest_sync_status = list(collection.find().sort("syncStop", -1).limit(1))
    if len(latest_sync_status) > 0:
      return latest_sync_status[0]["syncStop"]
    else:
      print('No syncStop found, starting from now')
      return datetime.now().astimezone(timezone.utc)

  def sync_databases(source_db, source_collections, destination_db, sync_start, sync_stop):
    for col in source_collections:
      print('Processing collection {}'.format( col ))
      source_col = source_db[col]
      destination_col= destination_db[col]

      docs = source_col.find({'updatedAt': {'$gte': sync_start, '$lt': sync_stop}})
      updates = [ReplaceOne({"_id": doc["_id"]}, doc, upsert = True) for doc in docs]

      if len(updates) > 0: destination_col.bulk_write(updates)
  
  def insert_sync_status(collection, sync_start, sync_stop, script_start):
    sync_status = {
      "syncStart": sync_start,
      "syncStop": sync_stop,
      "scriptStart": script_start,
      "scriptStop": datetime.now(tz=timezone.utc),
    }

    collection.insert_one(sync_status)

  @classmethod
  def check_start_conditions(cls):
    result = True
    for variable_name in cls.REQUIRED_ENVIRONMENTAL_VARIABLES:
      if os.getenv(variable_name) is None:
        sys.stderr.write('Environmental variable {} is not set!\n'.format( variable_name ))
        result = False
    return result

  @classmethod
  def preprocess_collections_list_input(cls, input:str)->list:
    elements = [ item.strip() for item in input.split(' ') ]
    elements = [ item for item in elements if len(item) > 0 ]
    return elements

  @classmethod
  def run(cls):

    script_start = datetime.now(tz=timezone.utc)
    sync_stop = script_start

    load_dotenv()
    if cls.check_start_conditions() is False:
      sys.exit(1)


    cloud_client = MongoClient(os.environ["CLOUD_DATABASE_URI"])
    cloud_db = cloud_client[ os.environ["CLOUD_DB_NAME"] ]
    cloud_collections = cls.preprocess_collections_list_input(os.environ['CLOUD_TO_LOCAL_COLLECTIONS'])

    local_client = MongoClient(os.environ["LOCAL_DATABASE_URI"])
    local_db = local_client[ os.environ["LOCAL_DB_NAME"] ]
    local_collections = cls.preprocess_collections_list_input( os.environ['LOCAL_TO_CLOUD_COLLECTIONS'] )

    sync_status_collection = local_db["syncstatus"]

    print('Starting the sync...')
    sync_start = DatabaseSync.get_sync_start(sync_status_collection)
    if len(cloud_collections) > 0:
      print('Syncing from CLOUD -> LOCAL: {}'.format( cloud_collections ))
      DatabaseSync.sync_databases(cloud_db, cloud_collections, local_db, sync_start, sync_stop) # sync from cloud to local
    else:
      print('No cloud collections to be synced')

    if len(local_collections) > 0:
      print('Syncing LOCAL -> CLOUD: {}'.format( local_collections ))
      DatabaseSync.sync_databases(local_db, local_collections, cloud_db, sync_start, sync_stop) # sync from local to cloud
    else:
      print('No local collections to be synced')
    print('Updating sync status')
    DatabaseSync.insert_sync_status(sync_status_collection, sync_start, sync_stop, script_start)
