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


def insert_uploaded_files(connection, file_names):
    try:
        assetId = get_asset_type(connection)
        print(assetId)
        cursor = connection.cursor()
        insert_query = 'INSERT INTO public."asset" (assetId,geoCoordinate,assetTypeId,imageFileLink,recordedUser) VALUES (%s,%s,%s,%s,%s),(assetId,[0,0],assetTypeId,%s,%s)'
        cursor.executemany(insert_query, [(file_name,) for file_name in file_names])
        connection.commit()
    except Exception as e:
        connection.rollback()
        print(f"Error inserting into the database: {e}")