import json
import threading
import psycopg2
from psycopg2 import pool
from mongo_CRUD import *
import time
from concurrent.futures import ThreadPoolExecutor

QUERYTIME = 0.0

# Create a connection pool
conn_pool = psycopg2.pool.SimpleConnectionPool(
    90,  # minconn
    100,  # maxconn
    database="delivery_system",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)


# def reserve_order_items(cursor, order_id, order_items, zip_code):
#     QUERYTIME = 0
#     start_time = time.time()  # Start the timer
#     try:
#         uuids = []
#         items = []
#         retry_count = 1
#         for item in order_items:
#             # Get the total inventory count for the current item at that point 
#             cursor.execute("SELECT COUNT(*) FROM inventory WHERE med_id = %s AND order_id IS NULL AND zip_code = %s", (item['med_id'], zip_code))
#             total_inventory = cursor.fetchone()[0]

#             cursor.callproc('update_inventory', [order_id, item['med_id'], zip_code, item['quantity']])
#             updated_count = cursor.fetchone()[0]

#             # Get the assigned UUIDs for the current item
#             cursor.execute("SELECT uuid FROM inventory WHERE order_id = %s AND med_id = %s AND zip_code = %s", (order_id, item['med_id'], zip_code))
#             assigned_uuids = [row[0] for row in cursor.fetchall()]

#             # Append item to the items list
#             items.append({
#                 'med_id': item['med_id'],
#                 'demand': item['quantity'],
#                 'inInventory': total_inventory,
#                 'fulfilled': updated_count == item['quantity'],
#                 'uuids': assigned_uuids
#             })

#             # # Store the fulfillment details in MongoDB
#             # fullfillment_collection.insert_one({
#             #     'order_id': order_id,
#             #     'items': [{
#             #         'med_id': item['med_id'],
#             #         'demand': item['quantity'],
#             #         'inInventory': total_inventory,
#             #         'fulfilled': updated_count == item['quantity'],
#             #         'uuids': assigned_uuids
#             #     }]
#             # })

#             if updated_count != item['quantity']:
#                 print(f"Failed to reserve items for order_id {order_id} after {retry_count} retries")
#             else:
#                 uuids.extend(assigned_uuids)
#         end_time = time.time()
#         fullfillment_collection.insert_one({
#             'order_id': order_id,
#             'items': items,
#             'status': all(item['fulfilled'] for item in items)  # Set the status to True if all items are fulfilled
#         })
#         # print(f"Time taken to reserve order items for order_id {order_id}: {end_time - start_time} seconds")
#         return uuids
#     except Exception as e:
#         print(f"Error in reserve_order_items: {e}")
#         return None
    


def reserve_order_items(cursor, order_id, order_items, zip_code):
    QUERYTIME = 0
    start_time = time.time()  # Start the timer
    try:
       
        retry_count = 1
        for item in order_items:
       
            cursor.callproc('update_inventory', [order_id, item['med_id'], zip_code, item['quantity']])
            updated_count = cursor.fetchone()[0]
            # print(updated_count)
            if updated_count != item['quantity']:
                print(f"Failed to reserve items for order {order_id} after {retry_count} retries")
                end_time = time.time()
                return False, end_time - start_time
        end_time = time.time()
        return True, end_time - start_time
    except Exception as e:
        end_time = time.time()
        print(f"Error in reserve_order_items: {e}")
        return False, end_time - start_time


# def reserve_order_items(cursor, order_id, order_items, zip_code):
#     QUERYTIME = 0
#     start_time = time.time()  # Start the timer
#     try:
#         uuids = []
#         retry_count = 1
#         for item in order_items:
#             retries = 0
            
#             while retries < retry_count:  # Set a limit for the number of retries
#                 # cursor.execute(" SELECT * FROM inventory WHERE med_id = %s AND order_id IS NULL order by uuid  LIMIT %s FOR UPDATE", (item['med_id'], item['quantity']))

#                 # cursor.execute(" SELECT * FROM inventory WHERE med_id = %s AND order_id IS NULL AND zip_code = %s order by uuid  LIMIT %s FOR UPDATE", (item['med_id'], zip_code, item['quantity']))
#                 rows = cursor.fetchall()
#                 if len(rows) < item['quantity']:
#                     # Not enough items, release locks and try again
#                     # cursor.execute("ROLLBACK")
#                     # time.sleep(0.1)  # Wait for a short delay before trying again
#                     retries += 1
#                     return None
#                 else:
#                     cursor.executemany("UPDATE inventory SET order_id = %s WHERE uuid = %s AND zip_code = %s", [(order_id, row[0], zip_code) for row in rows])
#                     uuids.extend([row[0] for row in rows])
#                     break  # Exit the loop once the necessary number of rows have been locked
#             if retries == retry_count:
#                 raise Exception("Failed to reserve items after {retry_count} retries")
#         end_time = time.time()  # End the timer
#         # print(f"Time taken to reserve order items for order_id {order_id}: {end_time - start_time} seconds")
#         return uuids
#     except Exception as e:
        
#         print(f"Error in reserve_order_items: {e}")
#         return None

def assign_agent(cursor, order_id, zip_code):
    start_time = time.time()  # Start the timer
    try:
        cursor.execute("SELECT * FROM Delivery_Agent WHERE order_id IS NULL AND zip_code = %s FOR UPDATE LIMIT 1", (zip_code,))
        agent = cursor.fetchone()
        if agent is None:
            return None
            raise Exception("No agent available")
        else:
            cursor.execute("UPDATE Delivery_Agent SET order_id = %s WHERE agent_id = %s AND zip_code = %s", (order_id, agent[0], zip_code))
            end_time = time.time()  # End the timer
            # print(f"Time taken to assign agent for order_id {order_id}: {end_time - start_time} seconds")
            return agent[0]
    except Exception as e:
        print(f"Error in assign_agent: {e}")
        return None

def update_order_status(cursor, order_id, status, agent_id):
    try:
        cursor.execute("UPDATE Orders SET status = %s, agent_id = %s WHERE order_id = %s", (status, agent_id, order_id))
        # print(f"Time taken to update order status for order_id {order_id}: {end_time - start_time} seconds")
        return order_id
    except Exception as e:
        print(f"Error in update_order_status: {e}")
        return None
    
def update_mongo_order_status(data, order_id, status):
    try:
        data["order_id"] = order_id
        data["status"] = status
        add_order(customer_collection, data["customer_id"], order_id, data)
    
    except Exception as e:
        print(f"Error in update_order_status: {e}")
        return None


def process_order(json_data, order_id):
   

    # Parse the JSON data
    data = json.loads(json_data)
    # print(f"Processing order for customer id: {data['customer_id']}, order_id: {order_id}")

    # Get a connection from the pool
    conn = conn_pool.getconn()
    

    cursor = conn.cursor()
    try:
        # Start the transaction

        cursor.execute("BEGIN")
        cursor.execute(" SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")
        # Reserve the order for each item in the order
        response_reserve, time_reserve = reserve_order_items(cursor, order_id, data['items'], data["zip_code"])
        if response_reserve==False:
            raise Exception("Failed to reserve order items")

        # # Find the first agent with null or no order_id and assign the order id to this table
        agent_id = assign_agent(cursor, order_id, data["zip_code"])
        if agent_id is None:
            raise Exception("Failed to assign agent")


        order_id = update_order_status(cursor, order_id, 'PROCESSED', agent_id)

        update_mongo_order_status(data, order_id, 'PROCESSED')
        # Commit the transaction
        conn.commit()

    

        return True, order_id, agent_id, time_reserve
    except Exception as e:
        # Rollback the transaction in case of any errors
        conn.rollback()
        update_mongo_order_status(data, order_id, 'FAILED')
        print(f"Error in process_order: {e}")
      
        return False, [], None,  time_reserve
    finally:
        # Close the database connection
        conn_pool.putconn(conn)

def store_order_details(json_data):
    # Parse the JSON data
    data = json.loads(json_data)
    customer_id = data['customer_id']
    zip_code = data['zip_code']

    # Get a connection from the pool
    conn = conn_pool.getconn()
    cursor = conn.cursor()

    try:
        # Start the transaction
        cursor.execute("BEGIN")
        # cursor.execute("set enable_index_onlyscan TO off")
        # Generate a sequential order_id
        cursor.execute("SELECT nextval('order_id_seq')")
        order_id = cursor.fetchone()[0]

        # Insert the order details into the ORDER table with status as 'pending' and agent_id as null
        cursor.execute("INSERT INTO ORDERS (order_id, customer_id, zip_code, status, agent_id) VALUES (%s, %s, %s, %s, %s)", (order_id, customer_id, zip_code, 'NOT_PROCESSED', None))
        for item in data['items']:
            cursor.execute("INSERT INTO ORDER_ITEM (order_id, med_id, quantity, zip_code) VALUES (%s, %s, %s, %s)", (order_id, item['med_id'], item['quantity'], zip_code))
        # Commit the transaction
        conn.commit()

        # Return the order_id
        return order_id
    except Exception as e:
        # Rollback the transaction in case of any errors
        conn.rollback()
        print("Failed to store order details" , e)
    finally:
        # Return the connection back to the pool
        conn_pool.putconn(conn)




def generate_bill_using_mongo_aggregation(orders, successful_order_ids):
    medicine_pipeline = [
        {
            "$match": {
                "med_id": {"$in": [item['med_id'] for order in orders for item in order['items']]}
            }
        },
        {
            "$project": {
                "_id": 0,
                "med_id": 1,
                "med_name": 1,
                "price": 1
            }
        }
    ]

    medicine_prices = list(medicine_collection.aggregate(medicine_pipeline))

    customer_pipeline = [
        {
            "$match": {
                "customer_id": {"$in": [order['customer_id'] for order in orders]}
            }
        },
        {
            "$project": {
                "_id": 0,
                "customer_id": 1,
                "name": {"$concat": [{"$toString": "$first_name"}, " ", {"$toString": "$last_name"}]},
                "address": {"$concat": [{"$toString": "$address"}, ", ", {"$toString": "$city"}, ", ", {"$toString": "$state"}, ", ", {"$toString": "$postal_code"}, ", ", {"$toString": "$country"}]},
                "phone_number": {"$toString": "$phone_number"}
            }
        }
    ]

    customer_details = list(customer_collection.aggregate(customer_pipeline))
    with open('bills.txt', 'w') as f:
        for order in orders:
            if order['order_id'] in successful_order_ids:
                fullfillment_collection.insert_one({
                    'order_id': order['order_id'],
                    'items': order['items'],
                    'status': True  # Set the status to True for successful orders
                })
                order['items'] = [
                {
                    **item,
                    'med_name': next((m['med_name'] for m in medicine_prices if m['med_id'] == item['med_id']), None),
                    'price': next((m['price'] for m in medicine_prices if m['med_id'] == item['med_id']), 0),
                    'item_total': item['quantity'] * next((m['price'] for m in medicine_prices if m['med_id'] == item['med_id']), 0)
                }
                for item in order['items']
                ]   
                order['total'] = sum(item['item_total'] for item in order['items'])
                order['customer'] = next((c for c in customer_details if c['customer_id'] == order['customer_id']), None)
                f.write(format_order_as_bill(order))
                f.write("\n" + "=" * 50 + "\n")  # write a separator between orders
            else:
                fullfillment_collection.insert_one({
                    'order_id': order['order_id'],
                    'items': order['items'],
                    'status': False  # Set the status to True for successful orders
                })
                f.write(f"Order ID: {order['order_id']}\n")
                f.write("Order Failed\n")
                f.write("\n" + "=" * 50 + "\n")
    json_object = json.dumps(orders, indent = 4)
    # print(json_object)
    return orders


def format_order_as_bill(order):
    bill = []
    bill.append(f"Order ID: {order['order_id']}")
    bill.append(f"Customer Name: {order['customer']['name']}")
    bill.append(f"Address: {order['customer']['address']}")
    bill.append(f"Phone Number: {order['customer']['phone_number']}")
    bill.append("\nItems:")
    bill.append(f"{'Item':<40}{'Quantity':<10}{'Price':<10}")
    for i, item in enumerate(order['items'], start=1):
        bill.append(f"{item['med_name']:<40}{item['quantity']:<10}{item['price']:<10}")
    bill.append(f"\nTotal: {order['total']}")
    return "\n".join(bill)

def main():
    start_time = time.time()
    successful_orders = 0
    unsuccessful_orders = 0
    total_qty =0
    ttime = 0
    # with open('sample_order.json') as json_file:
    # with open('sample_order2.json') as json_file:
    with open('mockaroo_orderplace_400.json') as json_file:
    # with open('sample_order2_partition.json') as json_file:
        orders = json.loads(json_file.read())
    successful_order_ids = []
    with ThreadPoolExecutor() as executor:
        
        futures = []
        for order in orders:
            # print(executor._work_queue.qsize())
            json_data = json.dumps(order)
            order_id = store_order_details(json_data) #store order details in pending status
            order['order_id'] = order_id
            total_qty += (order['items'][0].get('quantity'))
            future = executor.submit(process_order, json_data, order_id)


            futures.append(future)

        for future in futures:

            if future.result()[0]:
                successful_orders += 1
                ttime+= future.result()[3]
                successful_order_ids.append(future.result()[1])
            else:
                unsuccessful_orders += 1
                ttime+= future.result()[3]


    print(successful_order_ids)
    generate_bill_using_mongo_aggregation(orders, successful_order_ids)



    print(f"Time taken to complete all order reservations: {ttime} milliseconds")
    print(f"Number of successful orders: {successful_orders}")
    print(f"Number of unsuccessful orders: {unsuccessful_orders}")
    print(f"Number of orders: {len(orders)}")
    print(f"Total quantity of items ordered: {total_qty}")


main()