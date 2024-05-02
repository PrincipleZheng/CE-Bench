import copy
import logging
import math
import pickle
import random

import pandas as pd
from spn.structure.StatisticalTypes import MetaType

from prep.prepare_single_tables import find_relationships
from create_esb.utils import create_random_join

logger = logging.getLogger(__name__)

def probability_round(value):
    floor_value = math.floor(value)
    if random.random() < value - floor_value:
        floor_value += 1
    return floor_value

class JoinPreparator:
    def __init__(self, metadata_path, schema_graph, max_table_data=20000000, disable_cache=True):
        self.metadata_path = metadata_path
        self.schema_graph = schema_graph
        with open(metadata_path, 'rb') as file:
            self.table_metadata = pickle.load(file)
        self.cached_tables = {}
        self.max_table_data = max_table_data
        self.disable_cache = disable_cache

    def find_start_table(self, relationship_list, min_table_size):
        table_counts = {}
        table_set = set()

        for relationship in relationship_list:
            relationship_data = self.schema_graph.relationship_dictionary.get(relationship)
            table_counts[relationship_data.end] = table_counts.get(relationship_data.end, 0) + 1
            table_set.update([relationship_data.start, relationship_data.end])

        sampled_tables = [table for table in table_set if self.sampling_rate(table) < 1]
        if sampled_tables:
            return sampled_tables[0], table_set

        start_table = max(table_counts, key=table_counts.get, default=None)
        return start_table, table_set

    def count_columns(self, relationship_list=None, single_table=None):
        if single_table:
            return self.count_result_columns(single_table)
        elif relationship_list:
            return sum(self.count_result_columns(table) for table in self.find_corresponding_tables(relationship_list))
        else:
            raise ValueError("Either relationship_list or single_table must be provided")

    def find_corresponding_tables(self, relationship_list):
        tables = set()
        for relationship in relationship_list:
            relationship_data = self.schema_graph.relationship_dictionary[relationship]
            tables.update([relationship_data.start, relationship_data.end])
        return tables

    def get_next_relationship(self, relationship_list, joined_tables):
        for relationship in relationship_list:
            relationship_data = self.schema_graph.relationship_dictionary[relationship]
            if relationship_data.start in joined_tables:
                return relationship_data, True
            if relationship_data.end in joined_tables:
                return relationship_data, False
        raise ValueError("No more relationships to be joined.")

    def read_table_data(self, path, table):
        if path in self.cached_tables:
            return self.cached_tables[path]

        table_data = pd.read_hdf(path, key='df')
        irrelevant_attributes = [
            f"{table}.{attr}"
            for attr in self.schema_graph.table_dictionary[table].irrelevant_attributes
            if f"{table}.{attr}" in table_data.columns
        ]
        table_data.drop(columns=irrelevant_attributes, inplace=True)

        if not self.disable_cache:
            self.cached_tables[path] = table_data
        return table_data

    def get_null_value(self, table, attribute):
        idx = self.table_metadata[table]['relevant_attributes_full'].index(attribute)
        return self.table_metadata[table]['null_values_column'][idx]

    def sampling_rate(self, table_name):
        table_info = self.schema_graph.table_dictionary[table_name]
        theoretical_sample_size = table_info.sample_rate * table_info.table_size
        return min(self.max_table_data / table_info.table_size, theoretical_sample_size)

    def estimate_join_size(self, single_table=None, relationship_list=None, min_start_table_size=1):
        if single_table:
            sample_size = min(
                self.table_metadata[single_table]['length'] * self.sampling_rate(single_table),
                self.max_table_data
            )
            return sample_size, self.table_metadata[single_table]['length']

        start_table, _ = self.find_start_table(relationship_list, min_start_table_size)
        sample_size_estimate = self.table_metadata[start_table]['length'] * self.sampling_rate(start_table)
        full_join_size = self.table_metadata[start_table]['length']
        return sample_size_estimate, full_join_size

    def generate_samples(self, sample_size, post_sampling_factor=30, single_table=None, relationship_list=None, min_start_table_size=1):
        sample_estimate, full_join_size = self.estimate_join_size(
            single_table=single_table,
            relationship_list=relationship_list,
            min_start_table_size=min_start_table_size
        )

        if sample_estimate > sample_size:
            sample_rate = min(sample_size / sample_estimate * post_sampling_factor, 1)
            samples = self.sample_join(
                single_table=single_table,
                relationship_list=relationship_list,
                min_start_table_size=min_start_table_size,
                sample_rate=sample_rate
            )
            if len(samples) > sample_size:
                samples = samples.sample(sample_size)
            return samples, full_join_size

        samples = self.sample_join(
            single_table=single_table,
            relationship_list=relationship_list,
            min_start_table_size=min_start_table_size,
            sample_rate=1
        )
        if len(samples) > sample_size:
            samples = samples.sample(sample_size)
        return samples, full_join_size

    def count_result_columns(self, table):
        attributes = self.table_metadata[table]['relevant_attributes']
        multipliers = [attr for attr in attributes if self.is_multiplier(attr)]
        return len(attributes) - len(multipliers) / 2

    def is_multiplier(self, attribute):
        for relationship in self.schema_graph.relationships:
            if attribute in [relationship.multiplier_attribute_name_nn, relationship.multiplier_attribute_name]:
                return True
        return False

