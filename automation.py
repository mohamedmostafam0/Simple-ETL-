#!/usr/bin/env python3
# Import libraries required for connecting to MySQL
import mysql.connector

# Import libraries required for connecting to DB2 or PostgreSQL
import psycopg2

# Connect to MySQL
connection_mysql = mysql.connector.connect(user='root',
password='MjU2NjQtdTIxcDAy',
host='127.0.0.1',
database='sales')

# Connect to DB2 or PostgreSQL
dsn_hostname = "127.0.0.1"
dsn_user = "postgres"
dsn_pwd = "MTk1NDQtdTIxcDAy"
dsn_port = "5432"
dsn_database = "postgres"

connection_postgresql = psycopg2.connect(
    database=dsn_database,
    user=dsn_user,
    password=dsn_pwd,
    host=dsn_hostname,
    port=dsn_port
)

# Find out the last rowid from DB2 data warehouse or PostgreSQL data warehouse
def get_last_rowid(connection, cursor):
    try:
        # Execute SQL query to get the last rowid
        cursor.execute("SELECT MAX(rowid) FROM sales_data")
        # Fetch the result
        last_rowid = cursor.fetchone()[0]
        # Return the last rowid
        return last_rowid
    finally:
        # No need to close the connection here, as it is still needed in the subsequent functions
        pass

# Use postgresql connection for get_last_rowid
cursor_postgresql = connection_postgresql.cursor()
#get last row for production data warehouse
last_row_id = get_last_rowid(connection_postgresql, cursor_postgresql)
cursor_postgresql.close()
print("Last row id on production data warehouse =", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
def get_latest_records(connection, cursor, last_row_id):
    try:
        # Execute SQL query to get records with rowid greater than last_row_id
        cursor.execute("SELECT * FROM sales_data WHERE rowid > %s", (last_row_id,))
        # Fetch all records
        new_records = cursor.fetchall()

        # Return the list of new records
        return new_records

    finally:
        # No need to close the connection here, as it is still needed in the subsequent functions
        pass

# Use MySQL connection for get_latest_records
cursor_mysql = connection_mysql.cursor()
new_records = get_latest_records(connection_mysql, cursor_mysql, last_row_id)
cursor_mysql.close()
print("New rows on staging data warehouse =", len(new_records))

# Insert the additional records from MySQL into DB2 or PostgreSQL data warehouse
def insert_records(connection, cursor, records):
    try:
        # Insert records
        for record in new_records:
            cursor.execute("INSERT INTO sales_data (rowid, product_id, customer_id, quantity) VALUES (%s, %s, %s, %s)", record)

        # Commit the transaction
        connection.commit()

    finally:
        # No need to close the connection here, as it is still needed in the subsequent functions
        pass

# Use PostgreSQL connection for insert_records
cursor_postgresql = connection_postgresql.cursor()
insert_records(connection_postgresql, cursor_postgresql, new_records)
cursor_postgresql.close()

print("New rows inserted into production data warehouse =", len(new_records))

# Disconnect from MySQL
connection_mysql.close()

# Disconnect from PostgreSQL
connection_postgresql.close()
