import pandas as pd
import psycopg2
from pymongo import MongoClient
import time
import json
from datetime import datetime


class DataImporter:
    def __init__(self):
        self.df = pd.read_csv('Housing.csv')
        self.pg_conn = None
        self.mongo_client = None

    def connect_databases(self):
        # PostgreSQL connection
        self.pg_conn = psycopg2.connect(
            host="localhost",
            database="real_estate_db",
            user="postgres",
            password="030305"
        )

        self.mongo_client = MongoClient('mongodb://localhost:27017/')
        self.mongo_db = self.mongo_client['real_estate_db']

    def close_connections(self):
        if self.pg_conn:
            self.pg_conn.close()
        if self.mongo_client:
            self.mongo_client.close()

    def transform_data(self):
        bool_columns = ['mainroad', 'guestroom', 'basement', 'hotwaterheating', 'airconditioning', 'prefarea']
        for col in bool_columns:
            self.df[col] = self.df[col].map({'yes': True, 'no': False})

        furnishing_map = {
            'furnished': 1,
            'semi-furnished': 2,
            'unfurnished': 3
        }
        self.df['furnishingstatus_id'] = self.df['furnishingstatus'].map(furnishing_map)

        print(f"Data transformation completed, total {len(self.df)} records")

    def import_to_postgresql(self):
        cursor = self.pg_conn.cursor()
        insert_sql = """
        INSERT INTO properties 
        (area, bedrooms, bathrooms, stories, mainroad, guestroom, basement, 
         hotwaterheating, airconditioning, parking, prefarea, furnishingstatus_id, price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        data_tuples = [
            tuple(row) for row in self.df[[
                'area', 'bedrooms', 'bathrooms', 'stories', 'mainroad', 'guestroom',
                'basement', 'hotwaterheating', 'airconditioning', 'parking',
                'prefarea', 'furnishingstatus_id', 'price'
            ]].itertuples(index=False, name=None)
        ]

        start_time = time.time()
        cursor.executemany(insert_sql, data_tuples)
        self.pg_conn.commit()
        end_time = time.time()

        print(f"PostgreSQL data import completed, time: {end_time - start_time:.2f} seconds")
        cursor.close()

    def import_to_mongodb(self):
        collection = self.mongo_db['properties']

        mongo_docs = []
        for _, row in self.df.iterrows():
            doc = {
                'basic_info': {
                    'area': float(row['area']),
                    'bedrooms': int(row['bedrooms']),
                    'bathrooms': int(row['bathrooms']),
                    'stories': int(row['stories']),
                    'price': float(row['price'])
                },
                'features': {
                    'mainroad': bool(row['mainroad']),
                    'guestroom': bool(row['guestroom']),
                    'basement': bool(row['basement']),
                    'hotwaterheating': bool(row['hotwaterheating']),
                    'airconditioning': bool(row['airconditioning']),
                    'parking': int(row['parking']),
                    'prefarea': bool(row['prefarea'])
                },
                'furnishing_status': row['furnishingstatus'],
                'metadata': {
                    'created_at': datetime.now(),
                    'imported_at': datetime.now()
                }
            }
            mongo_docs.append(doc)

        start_time = time.time()
        result = collection.insert_many(mongo_docs)
        end_time = time.time()

        print(
            f"MongoDB data import completed, inserted {len(result.inserted_ids)} records, time: {end_time - start_time:.2f} seconds")

    def run_import(self):
        try:
            self.connect_databases()
            self.transform_data()

            print("Starting PostgreSQL import...")
            self.import_to_postgresql()

            print("Starting MongoDB import...")
            self.import_to_mongodb()

            print("Data import completed!")

        except Exception as e:
            print(f"Error during import: {e}")
        finally:
            self.close_connections()


if __name__ == "__main__":
    importer = DataImporter()
    importer.run_import()