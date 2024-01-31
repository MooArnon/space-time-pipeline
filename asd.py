import mysql.connector
import os

# Function to establish MySQL connection
def establish_connection():
    return mysql.connector.connect(
        host = os.environ.get('DB_HOST'),
        user = os.environ.get('DB_USERNAME'),
        password = os.environ.get('DB_PASSWORD'),
        database = os.environ.get('DB_NAME'),
    )

# Example usage
try:
    # Attempt to execute SQL queries
    connection = establish_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM fact_raw_data")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    cursor.close()
    connection.close()
except mysql.connector.Error as err:
    # Handle connection errors gracefully
    print("MySQL Error: {}".format(err))
    # Attempt to reconnect
    print("Attempting to reconnect...")
    connection = establish_connection()
    cursor = connection.cursor()
    # Retry SQL queries
    # ...
finally:
    # Close cursor and connection
    if 'cursor' in locals() and cursor is not None:
        cursor.close()
    if 'connection' in locals() and connection.is_connected():
        connection.close()
