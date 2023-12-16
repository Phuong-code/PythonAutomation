import mysql.connector
from mysql.connector import Error
import uuid
import csv
import glob

def create_connection():
    """Create and return a database connection."""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='coinlizard_database',
            user='Phil',
            password='Philwork123'
        )
        return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return None
    
def generate_uuid():
    """Generate a UUID as a 16-byte string."""
    return uuid.uuid4().bytes

def insert_crypto(cursor, name, symbol, image):
    crypto_id = generate_uuid()
    insert_query = "INSERT INTO crypto (id, name, symbol, image) VALUES (%s, %s, %s, %s)"
    cursor.execute(insert_query, (crypto_id, name, symbol, image))
    return crypto_id

def insert_price_data(cursor, crypto_id, date, high, low, open_price, close, volume, marketcap):
    price_data_id = generate_uuid()
    insert_query = """
    INSERT INTO price_data (id, crypto_id, date, high, low, open, close, volume, marketcap)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(insert_query, (price_data_id, crypto_id, date, high, low, open_price, close, volume, marketcap))

def read_csv(file_path):
    """Read data from a CSV file."""
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        return list(reader)

def main():
    connection = create_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                # List all CSV files in the 'crypto_dataset' directory
                csv_files = glob.glob('crypto_dataset/*.csv')
                image_data = read_csv('images.csv')

                for file_path in csv_files:
                    print(f"Processing file: {file_path}")
                    csv_data = read_csv(file_path)
                    _, name, symbol, _, _, _, _, _, _, _ = csv_data[0]
                    for img_row in image_data:
                        if img_row[0] == symbol:
                            image_url = img_row[1]
                            break
                    crypto_id = insert_crypto(cursor, name, symbol, image_url)
                    for row in csv_data:
                        _, name, symbol, date, high, low, open, close, volume, marketcap = row                    
                        insert_price_data(cursor, crypto_id, date, high, low, open, close, volume, marketcap)
                connection.commit()
                print("Data inserted successfully")
        except Error as e:
            print(f"Error during data insertion: {e}")
            connection.rollback()
        finally:
            connection.close()
            print("MySQL connection is closed")

if __name__ == "__main__":
    main()
