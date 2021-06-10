#!/bin/python3
#  profits.py  - script that manages

import sqlite3
import csv
#  from pprint import pprint

def main():
    database_file_name = "items.db"
    purchases_file_name ="purchases.csv"
    #  sales_file_name = "sales.csv"
    connection = sqlite3.connect(database_file_name)
    initialize(connection)
    write_file_to_table(purchases_file_name,connection,transaction_type="purchases",table_name="Purchases")
    #  write_file_to_table(sales_file_name,connection,type="sales",table_name="Sales")
    connection.close()
    #  write_file_to_table(file_name)

def write_file_to_table(file_name,connection, transaction_type, table_name):
    entries = get_sql_data_from_file(file_name, transaction_type = transaction_type)
    for entry in entries:
        fields = ",".join(entry.keys())
        values = list(entry.values())
        placeholders = ("?,"*len(values))[:-1]
        query = f"INSERT INTO {table_name} ({fields}) VALUES ({placeholders});"
        connection.execute(query, values)
    connection.commit()


def get_sql_data_from_file(file_name,transaction_type="purchases"):
    #Mappings from the the sql database to the matching column in the uploaders
    PURCHASE_DB_FILE_MAPPINGS = {"InternalSKU":"SKU",
        "VendorOrderID":"Vendor Order ID",
        "VendorSku":"Vendor Sku",
        "PurchaseDate":"Date",
        "Vendor":"Vendor",
        "Cost":"Cost",
        "Quantity":"Quantity"}
    SALES_DB_FILE_MAPPINGS = {}

    if transaction_type == "purchases":
        use_mappings = PURCHASE_DB_FILE_MAPPINGS
    else:
        use_mappings = SALES_DB_FILE_MAPPINGS

    with open(file_name,"r") as file:
        reader = csv.DictReader(file)
        transactions = [{db_field: row[file_col] for db_field, file_col in use_mappings.items()}
            for row in reader]
    return transactions


def initialize(connection):
    """ Creates the tables for the items database"""
    cursor = connection.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS Purchases
    (InternalOrderID INTEGER PRIMARY KEY ,  InternalSKU TEXT NOT NULL, VendorOrderID TEXT, VendorSku TEXT, PurchaseDate DATE,  Vendor TEXT, Cost DECIMAL, Quantity INT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS Sales
    (SKU TEXT Primary Key, Purchase Date DATE, Vendor TEXT, Cost DECIMAL, Quantity INT)''')
    connection.commit()


if __name__ == "__main__":
    main()

