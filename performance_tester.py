import time
import psycopg2
from pymongo import MongoClient
import threading
import statistics
from concurrent.futures import ThreadPoolExecutor
import json
from datetime import datetime

class PerformanceTester:
    def __init__(self):
        self.pg_conn = None
        self.mongo_client = None
        self.results = {
            'postgresql': {},
            'mongodb': {}
        }

    def connect_databases(self):

        # PostgreSQL
        self.pg_conn = psycopg2.connect(
            host="localhost",
            database="real_estate_db",
            user="postgres",
            password="030305"
        )

        # MongoDB
        self.mongo_client = MongoClient('mongodb://localhost:27017/')
        self.mongo_db = self.mongo_client['real_estate_db']

    def close_connections(self):
        if self.pg_conn:
            self.pg_conn.close()
        if self.mongo_client:
            self.mongo_client.close()

    def test_postgresql_query(self, query_name, query_sql, params=None, iterations=100):
        cursor = self.pg_conn.cursor()
        execution_times = []

        for i in range(iterations):
            start_time = time.time()
            cursor.execute(query_sql, params or ())
            results = cursor.fetchall()
            end_time = time.time()

            execution_times.append((end_time - start_time) * 1000)

        cursor.close()

        avg_time = statistics.mean(execution_times)
        p95_time = statistics.quantiles(execution_times, n=20)[18]
        qps = iterations / (sum(execution_times) / 1000)

        self.results['postgresql'][query_name] = {
            'avg_time_ms': round(avg_time, 2),
            'p95_time_ms': round(p95_time, 2),
            'qps': round(qps, 2),
            'iterations': iterations
        }

        print(f"PostgreSQL {query_name}: Average: {avg_time:.2f}ms, P95 {p95_time:.2f}ms, QPS {qps:.2f}")

        return execution_times

    def test_mongodb_query(self, query_name, query_func, iterations=100):
        execution_times = []

        for i in range(iterations):
            start_time = time.time()
            results = list(query_func())
            end_time = time.time()

            execution_times.append((end_time - start_time) * 1000)

        avg_time = statistics.mean(execution_times)
        p95_time = statistics.quantiles(execution_times, n=20)[18]
        qps = iterations / (sum(execution_times) / 1000)

        self.results['mongodb'][query_name] = {
            'avg_time_ms': round(avg_time, 2),
            'p95_time_ms': round(p95_time, 2),
            'qps': round(qps, 2),
            'iterations': iterations
        }

        print(f"MongoDB {query_name}: Average: {avg_time:.2f}ms, P95 {p95_time:.2f}ms, QPS {qps:.2f}")

        return execution_times

    def test_scenario_1_simple_query(self):
        print("\n=== Scenario 1: Simple Price Range Query ===")

        # PostgreSQL
        pg_query = "SELECT * FROM properties WHERE price < %s"
        self.test_postgresql_query("Scenario 1: Simple Price Range Query", pg_query, (5000000,), 1000)

        # MongoDB
        def mongo_query():
            return self.mongo_db.properties.find({
                "basic_info.price": {"$lt": 5000000}
            })

        self.test_mongodb_query("Scenario 1: Simple Price Range Query", mongo_query, 1000)

    def test_scenario_2_complex_query(self):
        print("\n=== Scenario 2: Complex multi condition query ===")

        # PostgreSQL
        pg_query = """
        SELECT * FROM properties 
        WHERE price BETWEEN %s AND %s
          AND area > %s 
          AND bedrooms >= %s
          AND airconditioning = %s
          AND parking >= %s
        """
        params = (3000000, 8000000, 5000, 3, True, 1)
        self.test_postgresql_query("Scenario 2: Complex multi condition query", pg_query, params, 500)

        # MongoDB
        def mongo_query():
            return self.mongo_db.properties.find({
                "basic_info.price": {"$gte": 3000000, "$lte": 8000000},
                "basic_info.area": {"$gt": 5000},
                "basic_info.bedrooms": {"$gte": 3},
                "features.airconditioning": True,
                "features.parking": {"$gte": 1}
            })

        self.test_mongodb_query("Scenario 2: Complex multi condition query", mongo_query, 500)

    def test_scenario_3_aggregation(self):
        print("\n=== Scenario 3: Aggregation Analysis Query ===")

        # PostgreSQL
        pg_query = """
        SELECT 
            furnishingstatus_id,
            COUNT(*) as count,
            AVG(price) as avg_price,
            AVG(area) as avg_area
        FROM properties 
        GROUP BY furnishingstatus_id
        """
        self.test_postgresql_query("Scenario 3: Aggregation Analysis Query", pg_query, iterations=200)

        # MongoDB
        def mongo_query():
            return self.mongo_db.properties.aggregate([
                {
                    "$group": {
                        "_id": "$furnishing_status",
                        "count": {"$sum": 1},
                        "avg_price": {"$avg": "$basic_info.price"},
                        "avg_area": {"$avg": "$basic_info.area"}
                    }
                }
            ])

        self.test_mongodb_query("Scenario 3: Aggregation Analysis Query", mongo_query, iterations=200)

    def test_scenario_4_write_performance(self):
        print("\n=== Scenario 4: Writing Performance Test ===")

        test_property = {
            'area': 6000, 'bedrooms': 3, 'bathrooms': 2, 'stories': 2,
            'mainroad': True, 'guestroom': False, 'basement': True,
            'hotwaterheating': False, 'airconditioning': True,
            'parking': 2, 'prefarea': True, 'furnishingstatus_id': 2,
            'price': 6500000
        }

        pg_query = """
        INSERT INTO properties 
        (area, bedrooms, bathrooms, stories, mainroad, guestroom, basement, 
         hotwaterheating, airconditioning, parking, prefarea, furnishingstatus_id, price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = tuple(test_property.values())

        execution_times = []
        for i in range(100):
            cursor = self.pg_conn.cursor()
            start_time = time.time()
            cursor.execute(pg_query, params)
            self.pg_conn.commit()
            end_time = time.time()
            execution_times.append((end_time - start_time) * 1000)
            cursor.close()

            cursor = self.pg_conn.cursor()
            cursor.execute("DELETE FROM properties WHERE price = %s", (6500000,))
            self.pg_conn.commit()
            cursor.close()

        avg_time = statistics.mean(execution_times)
        inserts_per_second = 1000 / avg_time * 100

        self.results['postgresql']['Single write'] = {
            'avg_time_ms': round(avg_time, 2),
            'inserts_per_second': round(inserts_per_second, 2),
            'iterations': 100
        }
        print(f"PostgreSQL Single write: average {avg_time:.2f}ms, {inserts_per_second:.2f} pieces per second")

        test_doc = {
            'basic_info': {
                'area': 6000, 'bedrooms': 3, 'bathrooms': 2,
                'stories': 2, 'price': 6500000
            },
            'features': {
                'mainroad': True, 'guestroom': False, 'basement': True,
                'hotwaterheating': False, 'airconditioning': True,
                'parking': 2, 'prefarea': True
            },
            'furnishing_status': 'semi-furnished',
            'metadata': {'created_at': datetime.now()}
        }

        execution_times = []
        for i in range(100):
            start_time = time.time()
            result = self.mongo_db.properties.insert_one(test_doc.copy())
            end_time = time.time()
            execution_times.append((end_time - start_time) * 1000)

            self.mongo_db.properties.delete_one({"_id": result.inserted_id})

        avg_time = statistics.mean(execution_times)
        inserts_per_second = 1000 / avg_time * 100

        self.results['mongodb']['Single write'] = {
            'avg_time_ms': round(avg_time, 2),
            'inserts_per_second': round(inserts_per_second, 2),
            'iterations': 100
        }
        print(f"MongoDB Single write: average {avg_time:.2f}ms, {inserts_per_second:.2f} pieces per second")

    def test_scenario_5_concurrent_queries(self):
        print("\n=== Scenario 5: Concurrent Query Testing ===")

        def postgresql_worker(worker_id):
            conn = psycopg2.connect(
                host="localhost", database="real_estate_db",
                user="postgres", password="030305"
            )
            cursor = conn.cursor()

            queries_executed = 0
            start_time = time.time()

            while time.time() - start_time < 10:
                cursor.execute(
                    "SELECT * FROM properties WHERE price BETWEEN %s AND %s AND bedrooms >= %s",
                    (3000000, 8000000, 2)
                )
                cursor.fetchall()
                queries_executed += 1

            cursor.close()
            conn.close()
            return queries_executed

        def mongodb_worker(worker_id):
            client = MongoClient('mongodb://localhost:27017/')
            db = client['real_estate_db']

            queries_executed = 0
            start_time = time.time()

            while time.time() - start_time < 10:
                list(db.properties.find({
                    "basic_info.price": {"$gte": 3000000, "$lte": 8000000},
                    "basic_info.bedrooms": {"$gte": 2}
                }))
                queries_executed += 1

            client.close()
            return queries_executed

        with ThreadPoolExecutor(max_workers=50) as executor:
            start_time = time.time()
            futures = [executor.submit(postgresql_worker, i) for i in range(50)]
            results = [f.result() for f in futures]
            total_queries = sum(results)
            total_time = time.time() - start_time

        pg_qps = total_queries / total_time
        self.results['postgresql']['Scenario 5: Concurrent Query'] = {
            'qps': round(pg_qps, 2),
            'avg_time_ms': 'N/A',
            'iterations': 50
        }
        print(f"PostgreSQL 50 concurrent: {pg_qps:.2f} QPS")

        with ThreadPoolExecutor(max_workers=50) as executor:
            start_time = time.time()
            futures = [executor.submit(mongodb_worker, i) for i in range(50)]
            results = [f.result() for f in futures]
            total_queries = sum(results)
            total_time = time.time() - start_time

        mongo_qps = total_queries / total_time
        self.results['mongodb']['Scenario 5: Concurrent Query'] = {
            'qps': round(mongo_qps, 2),
            'avg_time_ms': 'N/A',
            'iterations': 50
        }
        print(f"MongoDB 50 concurrent: {mongo_qps:.2f} QPS")

    def generate_report(self):
        print("\n" + "=" * 50)
        print("Performance Test Report")
        print("=" * 50)

        comparison_data = []

        scenarios = set()
        for db in ['postgresql', 'mongodb']:
            for scenario in self.results[db].keys():
                scenarios.add(scenario)

        for scenario in sorted(scenarios):
            pg_data = self.results['postgresql'].get(scenario, {})
            mongo_data = self.results['mongodb'].get(scenario, {})

            if 'qps' in pg_data:
                row = {
                    'scene': scenario,
                    'PostgreSQL QPS': pg_data.get('qps', 'N/A'),
                    'MongoDB QPS': mongo_data.get('qps', 'N/A'),
                    'PostgreSQL delay(ms)': pg_data.get('avg_time_ms', 'N/A'),
                    'MongoDB delay(ms)': mongo_data.get('avg_time_ms', 'N/A')
                }
            else:
                row = {
                    'scene': scenario,
                    'PostgreSQL Insertion per second': pg_data.get('inserts_per_second', 'N/A'),
                    'MongoDB Insertion per second': mongo_data.get('inserts_per_second', 'N/A')
                }

            comparison_data.append(row)

        print("\nPerformance Comparison Table:")
        print("-" * 80)
        for row in comparison_data:
            for key, value in row.items():
                print(f"{key}: {value}", end=" | ")
            print()
        print("-" * 80)

        with open('performance_results.json', 'w') as f:
            json.dump(self.results, f, indent=2)
        print(f"\nDetailed results have been saved to: performance_results.json")

    def run_all_tests(self):
        try:
            self.connect_databases()

            self.test_scenario_1_simple_query()
            self.test_scenario_2_complex_query()
            self.test_scenario_3_aggregation()
            self.test_scenario_4_write_performance()
            self.test_scenario_5_concurrent_queries()

            self.generate_report()

        except Exception as e:
            print(f"An error occurred during the testing process: {e}")
        finally:
            self.close_connections()


if __name__ == "__main__":
    tester = PerformanceTester()
    tester.run_all_tests()