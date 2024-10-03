import pymongo
import mysql.connector
from mysql.connector import Error

# MongoDB (CosmosDB Mongo API) connection details
mongo_uri = "your_cosmosdb_mongo_uri"
mongo_db_name = "your_mongo_database"
mongo_collection_name = "your_mongo_collection"

# MySQL connection details
mysql_host = "your_mysql_host"
mysql_user = "your_mysql_username"
mysql_password = "your_mysql_password"
mysql_db = "your_mysql_database"
mysql_table = "your_mysql_table"

# Connect to MongoDB (CosmosDB Mongo API)
client = pymongo.MongoClient(mongo_uri)
mongo_db = client[mongo_db_name]
mongo_collection = mongo_db[mongo_collection_name]

# Connect to MySQL
try:
    mysql_connection = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_db
    )
    if mysql_connection.is_connected():
        cursor = mysql_connection.cursor()

        # Query to insert data into MySQL table
        insert_query = f"INSERT INTO {mysql_table} (field1, field2, field3) VALUES (%s, %s, %s)"

        # Fetch documents from MongoDB and insert them into MySQL
        documents = mongo_collection.find()
        for doc in documents:
            # Extract fields from the MongoDB document
            field1 = doc.get('field1')
            field2 = doc.get('field2')
            field3 = doc.get('field3')

            # Insert document into MySQL
            cursor.execute(insert_query, (field1, field2, field3))

        # Commit the transaction
        mysql_connection.commit()

        print(f"Migration completed successfully. {cursor.rowcount} rows inserted.")
except Error as e:
    print(f"Error: {e}")
finally:
    if mysql_connection.is_connected():
        cursor.close()
        mysql_connection.close()

# Close MongoDB connection
client.close()
