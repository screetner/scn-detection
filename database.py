import cuid2
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()


def get_db_connection():
    try:
        connection = psycopg2.connect(os.getenv("DB_CONNECTION"))
        return connection
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None


def get_asset_type(connection):
    try:
        cursor = connection.cursor()
        cursor.execute('SELECT * FROM public."assetTypes" WHERE "assetType" = %s', ('billboard',))
        result = cursor.fetchone()
        return result[0]
    except Exception as e:
        print(f"Error getting asset type from the database: {e}")
    finally:
        cursor.close()


def insert_uploaded_files(connection, file_names, recorder_id):
    try:
        assetTypeId = get_asset_type(connection)
        cursor = connection.cursor()

        insert_query = '''
        INSERT INTO public."assets" 
        ("assetId", "geoCoordinate", "assetTypeId", "imageFileLink", "recordedUser", "recordedAt") 
        VALUES (%s, %s, %s, %s, %s, %s)
        '''

        # Preparing data for executemany
        data_to_insert = [
            (cuid2.Cuid().generate(), '(18.788, 98.597)', assetTypeId, file_name, recorder_id, '2024-08-24 09:41:18.803852')
            for file_name in file_names
        ]

        # Execute the query with the prepared data
        cursor.executemany(insert_query, data_to_insert)
        connection.commit()
    except Exception as e:
        connection.rollback()
        print(f"Error inserting into the database: {e}")
