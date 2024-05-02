import time
import psycopg2
import logging
from typing import Any, Dict

from .estimator import Estimator
from .utils import run_test
from ..workload.workload import query_2_sql
from ..dataset.dataset import load_table
from ..constants import DATABASE_URL

logger = logging.getLogger(__name__)

class Postgres(Estimator):
    def __init__(self, table, stat_target, seed):
        super().__init__(table=table, version=table.version, stat=stat_target, seed=seed)
        
        self.conn = psycopg2.connect(DATABASE_URL)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

        # Construct statistics
        start_timestamp = time.time()
        self.cursor.execute(f'select setseed({1 / seed});')
        for c in table.columns.values():
            self.cursor.execute(f'ALTER TABLE \"{table.name}\" ALTER COLUMN {c.name} SET STATISTICS {stat_target};')
        self.cursor.execute(f'ANALYZE \"{table.name}\";')
        duration_minutes = (time.time() - start_timestamp) / 60

        # Get size of statistics
        self.cursor.execute(f'SELECT sum(pg_column_size(pg_stats)) FROM pg_stats WHERE tablename=\'{table.name}\'')
        size_mb = self.cursor.fetchone()[0] / 1024 / 1024  # Convert size to MB

        logger.info(f"Statistics construction completed in {duration_minutes:.4f} minutes, consuming {size_mb:.2f} MBs")

    def query_sql(self, sql):
        sql_explain = f'EXPLAIN (FORMAT JSON) {sql}'
        start_timestamp = time.time()
        self.cursor.execute(sql_explain)
        results = self.cursor.fetchall()
        estimated_cardinality = results[0][0][0]['Plan']['Plan Rows']
        duration_ms = (time.time() - start_timestamp) * 1000
        return estimated_cardinality, duration_ms

    def query(self, query):
        sql = 'explain(format json) {}'.format(query_2_sql(query, self.table, aggregate=False))
        #  L.info('sql: {}'.format(sql))

        start_stmp = time.time()
        self.cursor.execute(sql)
        dur_ms = (time.time() - start_stmp) * 1e3
        res = self.cursor.fetchall()
        card = res[0][0][0]['Plan']['Plan Rows']
        #  L.info(card)
        return card, dur_ms

    def query_sql(self, sql):
        sql_explain = f'EXPLAIN (FORMAT JSON) {sql}'
        start_timestamp = time.time()
        self.cursor.execute(sql_explain)
        results = self.cursor.fetchall()
        estimated_cardinality = results[0][0][0]['Plan']['Plan Rows']
        duration_ms = (time.time() - start_timestamp) * 1000
        return estimated_cardinality, duration_ms
    
def test_postgres(seed: int, dataset: str, version: str, workload: str, params: Dict[str, Any], overwrite: bool):
    """
    Test the PostgreSQL estimator with provided parameters.
    """
    table = load_table(dataset, params.get('version', version))
    logger.info("Constructing PostgreSQL estimator...")
    estimator = Postgres(table, stat_target=params['stat_target'], seed=seed)
    logger.info(f"PostgreSQL estimator constructed: {estimator}")

    queries = pd.read_csv('queries.csv')
    for idx, row in queries.iterrows():
        sql_query = row['query']
        logger.info(f"Executing SQL query {idx+1}: {sql_query}")
        card, duration = estimator.query_sql(sql_query)
        logger.info(f"Query {idx+1}: Estimated cardinality = {card}, Duration = {duration} ms")

    run_test(dataset, version, workload, estimator, overwrite)


