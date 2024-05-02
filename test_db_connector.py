import mysql.connector
import time
import numpy as np
import logging

L = logging.getLogger(__name__)

class MySQL:
    def __init__(self, host, user, password, database, table_name, bucket):
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.connection.cursor()
        self.table_name = table_name
        self.bucket = bucket
        self.connection.autocommit = True

    def analyze_table(self):
        start_time = time.time()
        try:
            self.cursor.execute(f"ANALYZE TABLE `{self.table_name}` UPDATE HISTOGRAM ON ALL COLUMNS WITH {self.bucket} BUCKETS;")
            rows = self.cursor.fetchall()
            L.info(f"Analyze results: {rows}")
        except mysql.connector.Error as err:
            L.error(f"Failed to analyze table: {err}")
        duration = (time.time() - start_time) / 60
        L.info(f"Table analysis completed in {duration:.4f} minutes")

    def explain_query(self, query):
        start_time = time.time()
        try:
            self.cursor.execute(f"EXPLAIN FORMAT=JSON {query}")
            result = self.cursor.fetchall()
            duration_ms = (time.time() - start_time) * 1000
            return result[0][0], duration_ms
        except mysql.connector.Error as err:
            L.error(f"Failed to explain query: {err}")
            return None, 0

    def close(self):
        self.connection.close()
