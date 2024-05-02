import time
import mysql.connector
import logging
from typing import Any, Dict
import numpy as np

from .estimator import Estimator
from .utils import run_test
from ..workload.workload import query_2_sql
from ..dataset.dataset import load_table
from ..constants import MYSQL_HOST, MYSQL_PORT, MYSQL_DB, MYSQL_USER, MYSQL_PSWD

logger = logging.getLogger(__name__)

class MySQL(Estimator):
    def __init__(self, table, bucket, seed):
        super(MySQL, self).__init__(table=table, version=table.version, bucket=bucket, seed=seed)

        self.conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PSWD, host=MYSQL_HOST, port=MYSQL_PORT, database=MYSQL_DB)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

        # construct statistics
        start_stmp = time.time()
        self.cursor.execute(f"analyze table `{self.table.name}` update histogram on "
                            f"{','.join([c.name for c in table.columns.values()])} "
                            f"with {bucket} buckets;")
        rows = self.cursor.fetchall()
        L.info(f"{rows}")
        dur_min = (time.time() - start_stmp) / 60

        L.info(f"construct statistics finished, using {dur_min:.4f} minutes")

    # def query(self, query):
    #     start_timestamp = time.time()
    #     self.cursor.execute(query)
    #     duration_ms = (time.time() - start_timestamp) * 1000
    #     results = self.cursor.fetchall()
    #     assert len(results) == 1, results
    #     # this cardinality is not correct. We use another script to get the correct cardinality
    #     cardinality = np.round(0.01 * results[0][10] * self.table.row_num)
    #     return cardinality, duration_ms

    def query(self, query):
        start_timestamp = time.time()
        self.cursor.execute(query)
        duration_ms = (time.time() - start_timestamp) * 1000
        results = self.cursor.fetchall()
        
        # Check if the results have exactly one row
        assert len(results) == 1, results
        
        # Calculate a preliminary cardinality
        cardinality = np.round(0.01 * results[0][10] * self.table.row_num)
        
        # Write results to a file
        with open('query_results.txt', 'a') as file:
            file.write(f"Query: {query}\n")
            file.write(f"Results: {results}\n")
            file.write(f"Calculated Cardinality: {cardinality}\n")
            file.write(f"Query Duration: {duration_ms} ms\n")
            file.write("------------------------------------------------\n")
        
        return cardinality, duration_ms

# def test_mysql(seed: int, dataset: str, version: str, workload:str, params: Dict[str, Any], overwrite: bool):
#     """
#     params:
#         version: the version of table that mysql construct statistics, might not be the same with the one we test on
#         bucket: number of bucket for each histogram
#     """
#     # prioriy: params['version'] (build statistics from another dataset) > version (build statistics on the same dataset)
#     table = load_table(dataset, params.get('version') or version)

#     L.info("construct mysql estimator...")
#     estimator = MySQL(table, params['bucket'], seed=seed)
#     L.info(f"built mysql estimator: {estimator}")

#     run_test(dataset, version, workload, estimator, overwrite)


def test_mysql(seed: int, dataset: str, version: str, workload: str, params: Dict[str, Any], overwrite: bool):
    """
    Test the MySQL estimator with provided parameters.
    """
    table = load_table(dataset, params.get('version', version))
    logger.info("Constructing MySQL estimator...")
    estimator = MySQL(table, params['bucket'], seed)
    logger.info(f"MySQL estimator constructed: {estimator}")

    queries = pd.read_csv('queries.csv')
    for idx, row in queries.iterrows():
        query = row['query']
        logger.info(f"Executing query {idx+1}: {query}")
        card, duration = estimator.query(query)
        logger.info(f"Query {idx+1}: Estimated cardinality = {card}, Duration = {duration} ms")

    run_test(dataset, version, workload, estimator, overwrite)