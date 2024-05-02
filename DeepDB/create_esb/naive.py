import logging

from aqp_spn.aqp_spn import AQPSPN
from prep.join_data_preparation import JoinDataPreparator
from compile.spn_ensemble import SPNEnsemble

logger = logging.getLogger(__name__)

RATIO_MIN_INSTANCE_SLICE = 1 / 100


def create_naive_all_split_ensemble(schema, hdf_storage, sample_volume, ensemble_storage, dataset_name, bloom_filters,
                                    rdc_threshold, max_table_data, post_sampling_factor, incremental_rate):
    metadata_path = hdf_storage + '/metadata.pkl'
    preparator = JoinDataPreparator(metadata_path, schema, max_table_data=max_table_data)
    spn_ensemble = SPNEnsemble(schema)

    logger.info(f"Creating naive ensemble.")

    for table in schema.tables:
        logger.info(f"Learning SPN for {table.table_name}.")
        if incremental_rate > 0:
            samples, incremental_samples, meta_types, null_values, full_join_est = preparator.generate_n_samples_with_incremental_part(
                sample_volume,
                single_table=table.table_name,
                post_sampling_factor=post_sampling_factor,
                incremental_rate=incremental_rate)
            logger.debug(f"Requested {sample_volume} samples and got {len(samples)} + {len(incremental_samples)} "
                         f"(for incremental learning)")
        else:
            samples, meta_types, null_values, full_join_est = preparator.generate_n_samples(sample_volume,
                                                                                             single_table=table.table_name,
                                                                                             post_sampling_factor=post_sampling_factor)

        # learn spn
        aqp_spn = AQPSPN(meta_types, null_values, full_join_est, schema, None, full_sample_size=len(samples),
                         table_set={table.table_name}, column_names=list(samples.columns),
                         table_meta_data=preparator.table_meta_data)
        min_instance_slice = RATIO_MIN_INSTANCE_SLICE * min(sample_volume, len(samples))
        logger.debug(f"Using min_instance_slice parameter {min_instance_slice}.")
        logger.info(f"SPN training phase with {len(samples)} samples")
        aqp_spn.learn(samples.values, min_instances_slice=min_instance_slice, bloom_filters=bloom_filters,
                      rdc_threshold=rdc_threshold)
        if incremental_rate > 0:
            logger.info(f"additional incremental SPN training phase with {len(incremental_samples)} samples "
                        f"({incremental_rate}%)")
            aqp_spn.learn_incremental(incremental_samples.values)
        spn_ensemble.add_spn(aqp_spn)

    ensemble_storage += '/ensemble_single_' + dataset_name + '_' + str(sample_volume) + '.pkl'
    logger.info(f"Saving ensemble to {ensemble_storage}")
    spn_ensemble.save(ensemble_storage)


def naive_every_relationship_ensemble(schema, hdf_storage, sample_volume, ensemble_storage, dataset_name, bloom_filters,
                                      rdc_threshold, max_table_data, post_sampling_factor,
                                      incremental_rate=0):
    metadata_path = hdf_storage + '/metadata.pkl'
    preparator = JoinDataPreparator(metadata_path, schema, max_table_data=max_table_data)
    spn_ensemble = SPNEnsemble(schema)

    logger.info(f"Creating naive ensemble for every relationship.")
    for relationship in schema.relationships:
        logger.info(f"Learning SPN for {relationship.identifier}.")

        if incremental_rate > 0:
            samples, incremental_samples, meta_types, null_values, full_join_est = preparator.generate_n_samples_with_incremental_part(
                sample_volume, relationship_list=[relationship.identifier], post_sampling_factor=post_sampling_factor,
                incremental_rate=incremental_rate)
        else:
            samples, meta_types, null_values, full_join_est = preparator.generate_n_samples(
                sample_volume, relationship_list=[relationship.identifier], post_sampling_factor=post_sampling_factor)
        logger.debug(f"Requested {sample_volume} samples and got {len(samples)}")

        # learn spn
        aqp_spn = AQPSPN(meta_types, null_values, full_join_est, schema,
                         [relationship.identifier], full_sample_size=len(samples),
                         column_names=list(samples.columns), table_meta_data=preparator.table_meta_data)
        min_instance_slice = RATIO_MIN_INSTANCE_SLICE * min(sample_volume, len(samples))
        logger.debug(f"Using min_instance_slice parameter {min_instance_slice}.")
        logger.info(f"SPN training phase with {len(samples)} samples")
        aqp_spn.learn(samples.values, min_instances_slice=min_instance_slice, bloom_filters=bloom_filters,
                      rdc_threshold=rdc_threshold)
        if incremental_rate > 0:
            logger.info(f"additional incremental SPN training phase with {len(incremental_samples)} samples "
                        f"({incremental_rate}%)")
            aqp_spn.learn_incremental(incremental_samples)
        spn_ensemble.add_spn(aqp_spn)

    ensemble_storage += '/ensemble_relationships_' + dataset_name + '_' + str(sample_volume) + '.pkl'
    logger.info(f"Saving ensemble to {ensemble_storage}")
    spn_ensemble.save(ensemble_storage)
