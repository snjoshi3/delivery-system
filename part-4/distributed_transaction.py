import json
import threading
import psycopg2
from psycopg2 import pool
import time
from concurrent.futures import ThreadPoolExecutor
import postgres_creation_script
import sys

QUERYTIME = 0.0

# Create a connection pool
def create_conn_pool ():
    global conn_pool
    conn_pool = psycopg2.pool.SimpleConnectionPool(
        90,  # minconn
        100,  # maxconn
        database="delivery_system",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )



def reserve_order_items(cursor, order_id, order_items, zip_code):
    QUERYTIME = 0
    start_time = time.time()  # Start the timer
    try:
       
        retry_count = 1
        for item in order_items:
       
            cursor.callproc('update_inventory', [order_id, item['med_id'], zip_code, item['quantity']])
            updated_count = cursor.fetchone()[0]
            # print(str(updated_count) + " " + str(item['quantity']) + " " + str(updated_count != item['quantity']))
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
        # cursor.execute(" SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")
        # Reserve the order for each item in the order
        response_reserve, time_reserve = reserve_order_items(cursor, order_id, data['items'], data["zip_code"])
        # print(response_reserve)
        if response_reserve ==False:
            raise Exception("Failed to reserve order items")

        # # Find the first agent with null or no order_id and assign the order id to this table
        agent_id = assign_agent(cursor, order_id, data["zip_code"])
        if agent_id is None:
            raise Exception("Failed to assign agent")


        order_id = update_order_status(cursor, order_id, 'PROCESSED', agent_id)

        # Commit the transaction
        conn.commit()

    

        return True, order_id, agent_id, time_reserve
    except Exception as e:
        # Rollback the transaction in case of any errors
        conn.rollback()
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
    # print(conn)

    cursor = conn.cursor()
    # print(cursor)
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



def execute():
    start_time = time.time()
    successful_orders = 0
    unsuccessful_orders = 0
    total_qty =0
    ttime = 0
    # with open('sample_order.json') as json_file:
    # with open('sample_order2.json') as json_file:
    with open('mockorder_100_2med.json') as json_file:
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



    print(f"Time taken to complete all order reservations: {ttime} milliseconds")
    print(f"Number of successful orders: {successful_orders}")
    print(f"Number of unsuccessful orders: {unsuccessful_orders}")
    print(f"Number of orders: {len(orders)}")
    print(f"Total medicines in inventory: 200")
    print(f"Total demand of medicines: {total_qty}")
    print(f"Total medicine demands fulfilled: {successful_orders*2} ")


def main():
    
    type = sys.argv[1]
    #vanilla
    #forupdatenoskip
    #forupdateskiplocked

    postgres_creation_script.create(type)
    create_conn_pool()
    execute()



main()