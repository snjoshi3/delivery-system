DATABASE = "delivery_system"
DELIVERY_AGENT_TABLE = "Delivery_Agent"
INVENTORY = "Inventory"
AGENT_ID = "agent_id"
AGENT_NAME = "agent_name"
ORDER_ID = "order_id"
ZIPCODE = "zip_code"
UUID = "uuid"
STATUS = "status"
WAREHOUSE_ID = "warehouse_id"
MED_ID = "med_id"
ORDER = "Orders"
ORDER_ITEM = "Order_Item"
CUSTOMER_ID = "customer_id"
MED_ID = "med_id"
QUANTITY = "quantity"

zipcodes = 10
warehouse = 10
medicine = 100
agent_per_zipcode = 5
stock_per_warehouse = 400

import psycopg2
from psycopg2 import extensions
import random
import logging
from prettytable import PrettyTable

# logging.basicConfig(level=logging.INFO)
print("hello tables...")

def make_order(conn, zip_codes):
    print("Creating Order table...")
    try:
        print("Creating Order table...")
        # logging.info("Creating Order table...")
        zip_codes = min(100, zip_codes)
        cur = conn.cursor()
        table_creation_query = f"""
        CREATE TABLE {ORDER}(
            {CUSTOMER_ID} INT NOT NULL,
            {STATUS} VARCHAR(20) NOT NULL,
            {ORDER_ID} INT,
            {ZIPCODE} INT NOT NULL,
            {AGENT_ID} INT
        );"""
   
     
        create_sequence_query = f"CREATE SEQUENCE order_id_seq START 9000;"
        cur.execute(create_sequence_query)
        cur.execute(table_creation_query)
        conn.commit()
    except Exception as e:
        logging.error(f"Error creating Order table: {e}")


def make_order_item(conn, zip_codes):
    print("Creating Order_Item table...")
    try:
        print("Creating Order_Item table...")
        # logging.info("Creating Order_Item table...")
        zip_codes = min(100, zip_codes)
        cur = conn.cursor()
        table_creation_query = f"""
        CREATE TABLE {ORDER_ITEM}(
            {ORDER_ID} INT,
            {MED_ID} INT NOT NULL,
            {QUANTITY} INT NOT NULL,
            {ZIPCODE} INT NOT NULL
        );"""
        cur.execute(table_creation_query)
        conn.commit()
    except Exception as e:
        logging.error(f"Error creating Order_Item table: {e}")


def create_database(dbname, conn):
    print("Creating database...")
    cur = conn.cursor()
    conn.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    create = "create database " + dbname + ";"
    drop_database_query = f"DROP DATABASE IF EXISTS {dbname} WITH (FORCE);"
    cur.execute(drop_database_query)
    cur.execute(create)
    conn.commit()
    conn.close()


def make_delivery_agent(conn, zip_codes):
    print("Creating Delivery_Agent table...")
    zip_codes = min(100, zip_codes)

    cur = conn.cursor()
    table_creation_query = f"""
    CREATE TABLE {DELIVERY_AGENT_TABLE}(
        {AGENT_ID} SERIAL,
        {AGENT_NAME} VARCHAR(255) NOT NULL,
        {ORDER_ID} INT,
        {ZIPCODE} INT NOT NULL
    );"""

    cur.execute(table_creation_query)
    conn.commit()


def insert_data_agent(conn, zip_codes):
    print("Inserting data into Delivery_Agent table...")
    zip_codes = min(100, zip_codes)
    zip_c =  85200
        
    names = ["Alice", "Bob", "Charlie", "David", "Eva", "Frank", "Grace", "Henry", "Ivy", "Jack",
             "Kate", "Leo", "Mia", "Noah", "Olivia", "Peter", "Quinn", "Ruby", "Sam", "Tom"]

    cur = conn.cursor()
    for i in range (0, zip_codes):
        for _ in range(agent_per_zipcode):  # Create 4 agents for each zip code
            name = random.choice(names)
            insert_data_query = f"INSERT INTO {DELIVERY_AGENT_TABLE} ({AGENT_NAME},{ORDER_ID},{ZIPCODE}) VALUES ('{name}',NULL,{zip_c+i}) RETURNING {AGENT_ID}"
            cur.execute(insert_data_query)

    conn.commit()


def make_inventory(conn, zipcodes):
    print("Creating Inventory table...")
    zip_codes = min(100, zipcodes)

    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")

    table_creation_query = f"""
    CREATE TABLE Inventory (
        {UUID} serial,
        {WAREHOUSE_ID} INT NOT NULL,
        {ORDER_ID} INT,
        {MED_ID} INT NOT NULL,
        {ZIPCODE} INT NOT NULL,
        PRIMARY KEY ({UUID})
    );"""

    cur.execute(table_creation_query)
    conn.commit()


def insert_data_inventory(conn, zip_codes, warehouse, medicine):
    print("Inserting data into Inventory table...")
    zip_codes = min(6, zip_codes)
    warehouse_list = list(range(1, warehouse + 1))
    medicine_list = list(range(1, medicine + 1))
    cur = conn.cursor()
    zip_c =  85200
    for i in range (0, zip_codes):
        for medi in medicine_list:
            for _ in range(stock_per_warehouse):  # Ensure 10 counts for each med_id
                wh = random.choice(warehouse_list)
                insert_data_query = f"INSERT INTO {INVENTORY} ({WAREHOUSE_ID},{ORDER_ID},{MED_ID},{ZIPCODE}) VALUES ({wh},NULL,{medi},{zip_c+i})"
                cur.execute(insert_data_query)

    conn.commit()

def get_medicine_in_inventory(conn, zip_code, med_id):
    cur = conn.cursor()
    query = f"SELECT * FROM {INVENTORY} WHERE {ZIPCODE} = {zip_code} and {MED_ID} = {med_id}"
    cur.execute(query)
    results = cur.fetchall()

    table = PrettyTable()
    table.field_names = ["UUID", "Warehouse ID", "Order ID", "Medicine ID", "Zip Code"]
    for row in results:
        table.add_row(row)

    print(table)

def get_agent_details(conn, zip_code):
    cur = conn.cursor()
    query = f"SELECT {AGENT_ID}, {AGENT_NAME}, {ZIPCODE}, {ORDER_ID} FROM {DELIVERY_AGENT_TABLE} WHERE {ZIPCODE} = {zip_code}"
    cur.execute(query)
    results = cur.fetchall()

    table = PrettyTable()
    table.field_names = ["Agent ID", "Agent Name", "Zip Code", "Order ID"]
    for row in results:
        table.add_row(row)

    print(table)

def get_available_agents(conn, zip_code):
    cur = conn.cursor()
    query = f"SELECT {AGENT_ID} FROM {DELIVERY_AGENT_TABLE} WHERE {ZIPCODE} = {zip_code} AND {ORDER_ID} IS NULL"
    cur.execute(query)
    results = cur.fetchall()

    table = PrettyTable()
    table.field_names = ["Agent ID"]
    for row in results:
        table.add_row(row)

    print(table)

def get_available_medicine_items(conn, zip_code, med_id):
    cur = conn.cursor()
    query = f"SELECT {MED_ID}, {UUID}, {WAREHOUSE_ID} FROM {INVENTORY} WHERE {ZIPCODE} = {zip_code} AND {ORDER_ID} IS NULL AND {MED_ID} = {med_id}"
    cur.execute(query)
    results = cur.fetchall()

    table = PrettyTable()
    table.field_names = ["Medicine ID", "UUID", "Warehouse ID"]
    for row in results:
        table.add_row(row)

    print(table)

def setup_unpartitioned_db():
    # zipcodes = 10
    # warehouse = 10
    # medicine = 100
    # agent_per_zipcode = 5
    # stock_per_warehouse = 30
    try:
        conn = psycopg2.connect(database="postgres", user="postgres", host='localhost', password="postgres", port=5432)
        create_database(DATABASE, conn)
        conn = psycopg2.connect(database=DATABASE, user="postgres", host='localhost', password="postgres", port=5432)
        # print(conn)
        make_order(conn, zipcodes)
        make_order_item(conn, zipcodes)
        make_delivery_agent(conn, zipcodes)
        insert_data_agent(conn, zipcodes)
        make_inventory(conn, zipcodes)
        insert_data_inventory(conn, zipcodes, warehouse, medicine)

        # sample SQL queries to fetch data
        # get_medicine_in_inventory(conn, 85200,3)
        # get_agent_details(conn, 85200)
        # get_available_agents(conn, 85200)
        # get_available_medicine_items(conn, 85200, 3)



        conn.close()
    except Exception as e:
        print(e)
        print("Error")
        logging.error(f"Error creating database: {e}")

if __name__ == '__main__':
    setup_unpartitioned_db()

