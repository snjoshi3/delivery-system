from pymongo import MongoClient
import json
from delivery_system_mongo import medicine_collection, customer_collection, fullfillment_collection

from prettytable import PrettyTable

def insert_data(collection, data):
    collection.insert_many(data)

def read_data(collection, query):
    return collection.find_one(query)

def update_data(collection, query, new_data):
    collection.update_one(query, {"$set": new_data})

def delete_data(collection, query):
    collection.delete_one(query)


def get_medicine_price(collection, medicine_id, medicine_name):
    query = {"med_id": medicine_id, "med_name": medicine_name}
    print (query)
    medicine = read_data(collection, query)
    return medicine["price"] if medicine else None


def add_order(collection, customer_id, order_id, order_data):
    customer = collection.find_one({"customer_id": customer_id})
    
    if not customer:
        print(f"No customer found with id {customer_id}")
        return

    recent_orders = customer.get("recent_orders", [])
    older_orders = customer.get("older_orders", [])

    # If recent_orders already has 4 items, move the oldest one to older_orders
    if len(recent_orders) == 4:
        older_orders.append(recent_orders.pop(0)["order_id"])

    # Add the new order to recent_orders
    recent_orders.append(order_data)

    # Update the customer document
    collection.update_one({"customer_id": customer_id}, {"$set": {"recent_orders": recent_orders, "older_orders": older_orders}})

def get_customer_details(collection, customer_id):
    query = {"customer_id": customer_id}
    customer = read_data(collection, query)
    return customer


def generate_analytics(fullfillment_collection, medicine_collection):
    # Total revenue generated for successful orders
    pipeline = [
        {"$match": {"status": True}},
        {"$unwind": "$items"},
        {"$lookup": {
            "from": "medicine_details",
            "localField": "items.med_id",
            "foreignField": "med_id",
            "as": "medicine_details"
        }},
        {"$unwind": "$medicine_details"},
        {"$group": {"_id": None, "totalRevenue": {"$sum": {"$multiply": ["$medicine_details.price", "$items.quantity"]}}}}
    ]
    revenue = list(fullfillment_collection.aggregate(pipeline))[0]['totalRevenue']
    print(f"Total revenue generated for successful orders: {revenue}")

    # Total revenue loss for status false orders
    pipeline = [
        {"$match": {"status": False}},
        {"$unwind": "$items"},
        {"$lookup": {
            "from": "medicine_details",
            "localField": "items.med_id",
            "foreignField": "med_id",
            "as": "medicine_details"
        }},
        {"$unwind": "$medicine_details"},
        {"$group": {"_id": None, "totalLoss": {"$sum": {"$multiply": ["$medicine_details.price", "$items.quantity"]}}}}
    ]
    loss = list(fullfillment_collection.aggregate(pipeline))[0]['totalLoss']
    print(f"Total revenue loss for status false orders: {loss}")


    # Top 5 medicines which are sold
    pipeline = [
        {"$unwind": "$items"},
        {"$group": {"_id": "$items.med_id", "totalSold": {"$sum": "$items.quantity"}}},
        {"$sort": {"totalSold": -1}},
        {"$limit": 5}
    ]
    top_meds = list(fullfillment_collection.aggregate(pipeline))
    print("Top 5 medicines sold:")
    table = PrettyTable(['Medicine ID', 'Medicine Name', 'Drug', 'Total Sold'])
    for med in top_meds:
        med_details = medicine_collection.find_one({"med_id": med['_id']})
        table.add_row([med_details['med_id'], med_details['med_name'], med_details['drug'], med['totalSold']])
    print(table)
    # Bottom 5 medicines which are sold
    pipeline = [
        {"$unwind": "$items"},
        {"$group": {"_id": "$items.med_id", "totalSold": {"$sum": "$items.quantity"}}},
        {"$sort": {"totalSold": 1}},
        {"$limit": 5}
    ]
    bottom_meds = list(fullfillment_collection.aggregate(pipeline))
    print("Bottom 5 medicines sold:")
    table = PrettyTable(['Medicine ID', 'Medicine Name', 'Drug', 'Total Sold'])
    for med in bottom_meds:
        med_details = medicine_collection.find_one({"med_id": med['_id']})
        table.add_row([med_details['med_id'], med_details['med_name'], med_details['drug'], med['totalSold']])
    print(table)

def get_warehouse_details(collection, warehouse_id):
    query = {"warehouse_id": warehouse_id}
    warehouse = read_data(collection, query)
    return warehouse

def get_customers_by_zipcode(collection, postal_code):
    query = {"postal_code": postal_code}
    customers = collection.find(query)
    customers = list(customers)

    table = PrettyTable(['Customer Name', 'Email', 'Phone Number', 'Full Address', 'Postal Code', 'Country'])
    for customer in customers:
        full_name = f"{customer['first_name']} {customer['last_name']}"
        full_address = f"{customer['address']}, {customer['city']}, {customer['state']}"
        table.add_row([full_name, customer['email'], customer['phone_number'], full_address, customer['postal_code'], customer['country']])
    print(table)

def get_customer_orders(collection, customer_id):
    customer = collection.find_one({"customer_id": customer_id})
    if not customer:
        print(f"No customer found with id {customer_id}")
        return

    recent_orders = customer.get("recent_orders", [])
    older_orders = customer.get("older_orders", [])

    table = PrettyTable(['Order Type', 'Order ID', 'Status', 'Items'])
    for order in recent_orders:
        items = ', '.join([f"Medicine ID: {item['med_id']}, Quantity: {item['quantity']}" for item in order['items']])
        table.add_row(['Recent', order['order_id'], order['status'], items])
    print(table)

def get_medicine_demand(medicine_collection, fullfillment_collection, med_id):
    # Fetch all orders with the given medicine id
    orders = fullfillment_collection.find({"items.med_id": med_id})

    # Calculate total demand and successful orders
    total_demand = 0
    successful_orders = 0
    for order in orders:
        for item in order['items']:
            if item['med_id'] == med_id:
                total_demand += item['quantity']
                if order['status']:
                    successful_orders += item['quantity']

    # Fetch medicine details
    medicine = medicine_collection.find_one({"med_id": med_id})

    # Print details in a table
    table = PrettyTable(['Medicine ID', 'Medicine Name', 'Drug', 'Company', 'Price', 'Total Demand', 'Successful Orders'])
    table.add_row([medicine['med_id'], medicine['med_name'], medicine['drug'], medicine['drug_company'], medicine['price'], total_demand, successful_orders])
    print(table)

if __name__ == "__main__":

    # Fetch the price of a medicine
    medicine_id = 15
    medicine_name = "Clopidogrel Bisulfate"
    # price = get_medicine_price(medicine_collection, medicine_id, medicine_name)
    # print(f"The price of {medicine_name} is {price}")
    generate_analytics(fullfillment_collection, medicine_collection)
    get_customers_by_zipcode (customer_collection, 85201)
    get_customer_orders(customer_collection, 13)
    get_medicine_demand(medicine_collection, fullfillment_collection, 15)