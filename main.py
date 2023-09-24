import database_sync
import argparse

if __name__ == '__main__':

	parser = argparse.ArgumentParser(description='CLI for ASC Mongo Sync')
	parser.add_argument('--cloud-db-name', type=str, required=True, help='Name of the cloud database')
	parser.add_argument('--local-db-name', type=str, required=True, help='Name of the local database')
	parser.add_argument('--c2l-collections', type=str, required=True, help='List of collections to be synced from the cloud to the local database, space separated')
	parser.add_argument('--l2c-collections', type=str, required=True, help='List of collections to be synced from local to the cloud database, space separated')
	parser.add_argument('--sync-start', type=str, default=None, help='For initialization ONLY, define the start of the sync')
	args = vars(parser.parse_args())


	database_sync.DatabaseSync.run(cloud_db_name=args['cloud_db_name'],
		local_db_name=args['local_db_name'],
		c2l_collections=args['c2l_collections'],
		l2c_collections=args['l2c_collections'],
		sync_start=args['sync_start'])
