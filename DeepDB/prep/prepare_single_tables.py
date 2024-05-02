import logging
import pickle
from time import perf_counter

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

def read_csv_data(table, separator=','):
    """
    Reads CSV data from a specified location, renames columns, and removes unnecessary columns.
    """
    data_frame = pd.read_csv(table.csv_location, header=None, escapechar='\\', encoding='utf-8', quotechar='"', sep=separator)
    data_frame.columns = [f"{table.name}." + attribute for attribute in table.attributes]

    for attribute in table.unneeded_attributes:
        data_frame.drop(f"{table.name}.{attribute}", axis=1, inplace=True)

    return data_frame.apply(pd.to_numeric, errors="ignore")

def identify_relationships(graph, table_name, is_incoming=True):
    relationships = []
    for relationship in graph.relationships:
        if (relationship.end == table_name and is_incoming) or (relationship.start == table_name and not is_incoming):
            relationships.append(relationship)
    return relationships

def prepare_table(graph, table_name, output_path, max_unique=10000, separator=',', max_data=20000000):
    """
    Prepares a table by reading data, managing dependencies, and saving processed data.
    """
    metadata = {}
    table = graph.table_dict[table_name]
    data = read_csv_data(table, separator)
    sample_rate = table.sample_rate

    metadata['hdf_path'] = output_path
    metadata['incoming_means'] = {}

    logger.info(f"Processing functional dependencies for {table_name}")
    metadata['dependencies'] = {}
    columns_to_drop = []
    for attribute in table.attributes:
        full_attr = f"{table_name}.{attribute}"
        dependents = table.dependent_attributes(full_attr)
        if dependents:
            metadata['dependencies'][full_attr] = {
                dependent: data.drop_duplicates([full_attr, dependent]).set_index(full_attr)[dependent].to_dict()
                for dependent in dependents
            }
            columns_to_drop.append(full_attr)
    data.drop(columns=columns_to_drop, inplace=True)

    logger.info(f"Adding multipliers for {table_name}")
    relationships = identify_relationships(graph, table_name, is_incoming=True)
    for relationship in relationships:
        logger.info(f"Adding multiplier for {relationship.identifier} in {table_name}")
        left_attr = f"{table_name}.{relationship.end_attr}"
        right_attr = f"{relationship.start}.{relationship.start_attr}"

        neighbor_data = read_csv_data(graph.table_dict[relationship.start], separator).set_index(right_attr, drop=False)
        data = data.set_index(left_attr, drop=False)

        assert len(table.primary_keys) == 1, "Only single primary keys are supported"
        primary_key = f"{table_name}.{table.primary_keys[0]}"
        assert primary_key == left_attr, "Only primary key references are supported"

        data.index.name = None
        neighbor_data.index.name = None
        multipliers = data.join(neighbor_data, how='left')[[primary_key, right_attr]].groupby([primary_key]).count()
        multiplier_col = f"{relationship.end}.{relationship.multiplier_attr}"

        multipliers.rename(columns={right_attr: multiplier_col}, inplace=True)
        multipliers[multiplier_col] *= 1 / graph.table_dict[relationship.start].sample_rate

        data = data.join(multipliers)
        metadata['incoming_means'][relationship.identifier] = data[multiplier_col].mean()

    logger.info(f"Processing categorical and null values for {table_name}")
    metadata['categoricals'] = {}
    metadata['null_values'] = []
    delete_attrs = []

    for attr in table.attributes:
        full_attr = f"{table_name}.{attr}"
        if data.dtypes[full_attr] == object:
            distinct_values = data[full_attr].unique()
            if len(distinct_values) > max_unique:
                delete_attrs.append(attr)
            elif not data[full_attr].notna().any():
                delete_attrs.append(attr)
            else:
                value_dict = {val: idx + 1 for idx, val in enumerate(distinct_values)}
                value_dict[np.nan] = 0
                metadata['categoricals'][full_attr] = value_dict
                data[full_attr] = data[full_attr].map(value_dict).fillna(0)
                metadata['null_values'].append(value_dict[np.nan])
        else:
            if not data[full_attr].notna().any():
                delete_attrs.append(attr)
            else:
                unique_null = data[full_attr].mean() + 0.0001
                assert not (data[full_attr] == unique_null).any()
                data[full_attr].fillna(unique_null, inplace=True)
                metadata['null_values'].append(unique_null)

    data.drop(columns=[f"{table_name}.{attr}" for attr in delete_attrs], inplace=True)
    logger.info(f"Final attributes for {table_name} are {[f'{table_name}.{attr}' for attr in table.attributes if attr not in delete_attrs]}")

    if len(data) < max_data:
        data.to_hdf(output_path, key='df', format='table')
    else:
        data.sample(max_data).to_hdf(output_path, key='df', format='table')

    return metadata

def prepare_all_tables(graph, output_path, separator=',', max_data=20000000):
    start_time = perf_counter()
    all_metadata = {}
    for table in graph.tables:
        logger.info(f"Preparing HDF for {table.name}")
        all_metadata[table.name] = prepare_table(graph, table.name, f"{output_path}/{table.name}.hdf", separator, max_data)

    with open(f"{output_path}/meta_data.pkl", 'wb') as file:
        pickle.dump(all_metadata, file, pickle.HIGHEST_PROTOCOL)

    end_time = perf_counter()
    with open(f"{output_path}/build_time_hdf.txt", 'w') as file:
        file.write(str(round(end_time - start_time)))

    return all_metadata
