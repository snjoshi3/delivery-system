from pymongo import MongoClient
from time import sleep

def main():
    client = MongoClient('mongos', 27020)
    admin_db = client.admin

    # Wait for the mongos and shard servers to be ready
    sleep(30)

    # Add the shards
    admin_db.command('addShard', 'shard1svr:27018')
    admin_db.command('addShard', 'shard2svr:27017')

    # Enable sharding for your database (replace 'mydb' with your database name)
    admin_db.command('enableSharding', 'mydb')

if __name__ == '__main__':
    main()
