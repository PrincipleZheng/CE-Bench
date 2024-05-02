import psycopg2
import pandas as pd

from compile.utils import gen_full_join_query, print_conditions


class DatabaseConnection:
    """Manages database connections and operations."""

    def __init__(self, username="postgres", password="postgres", host="localhost", port="5432", database="shopdb"):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database

    def vacuum_database(self):
        """Performs a VACUUM operation on the database to clean and optimize it."""
        with psycopg2.connect(user=self.username, password=self.password, host=self.host, port=self.port, database=self.database) as conn:
            conn.set_isolation_level(0)  # Set the isolation level to 0 to allow vacuuming
            with conn.cursor() as cursor:
                cursor.execute("VACUUM")

    def fetch_dataframe(self, sql_query):
        """Executes SQL query and returns results as a Pandas DataFrame."""
        with psycopg2.connect(user=self.username, password=self.password, host=self.host, port=self.port, database=self.database) as conn:
            return pd.read_sql(sql_query, conn)

    def execute_query(self, sql_query):
        """Executes a given SQL query against the database."""
        with psycopg2.connect(user=self.username, password=self.password, host=self.host, port=self.port, database=self.database) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                conn.commit()

    def fetch_single_result(self, sql_query):
        """Executes SQL query and fetches a single result."""
        with psycopg2.connect(user=self.username, password=self.password, host=self.host, port=self.port, database=self.database) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                return cursor.fetchone()[0]

    def fetch_all_results(self, sql_query, include_column_names=False):
        """Fetches all rows of result set and optionally includes column names."""
        with psycopg2.connect(user=self.username, password=self.password, host=self.host, port=self.port, database=self.database) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if include_column_names else None
                return (rows, columns) if include_column_names else rows


class CardinalityEstimator:
    """Queries the database to determine the true cardinalities based on a schema graph."""

    def __init__(self, schema_graph, db_connection):
        self.schema_graph = schema_graph
        self.db_connection = db_connection

    def estimate_cardinality(self, query):
        """Estimates the cardinality of a given query using the actual database."""
        join_query = generate_full_join_query(self.schema_graph, query.relationships, query.tables, "JOIN")
        conditions = format_conditions(query.conditions, separator='AND')
        where_clause = f"WHERE {conditions}" if conditions else ""
        sql_query = join_query.format("COUNT(*)", where_clause)
        cardinality = self.db_connection.fetch_single_result(sql_query)
        return sql_query, cardinality
