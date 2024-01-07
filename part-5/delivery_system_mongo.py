from pymongo import MongoClient
import json
client = MongoClient('mongodb://root:password@localhost:27017/')
# client.drop_database('JhatPatMedicines')
db = client['JhatPatMedicines']

medicine_collection = db['medicine_details']
fullfillment_collection = db['fullfillment_details']
customer_collection = db['customer_details']

def insert_medicine_details(file_path):

    with open(file_path, 'r') as f:
        data = json.load(f)

    insert_data(medicine_collection, data)
    return medicine_collection

def insert_customer_data_from_file(file_path):

    with open(file_path, 'r') as f:
        data = json.load(f)

    insert_data(customer_collection, data)
    return medicine_collection

def insert_data(collection, data):
    collection.insert_many(data)

if __name__ == "__main__":
    client.drop_database('JhatPatMedicines')
    
    db = client['JhatPatMedicines']
    if 'medicine_details' in db.list_collection_names():
        db['medicine_details'].drop()
    if 'customer_details' in db.list_collection_names():
        db['customer_details'].drop()
    medicine_collection = db['medicine_details']
    fullfillment_collection = db['fullfillment_details']
    customer_collection = db['customer_details']
    insert_medicine_details('medicine_details_mock.json')
    insert_customer_data_from_file('customer_details_mock.json')