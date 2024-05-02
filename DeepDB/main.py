import argparse
import logging
import os
import shutil
import time

import numpy as np

from rspn.code_generation.generate_code import generate_ensemble_code
from prep.join_data_preparation import prepare_sample_hdf
from prep.prepare_single_tables import prepare_all_tables
from compile.spn_ensemble import read_ensemble
from create_esb.naive import create_naive_all_split_ensemble, naive_every_relationship_ensemble
from create_esb.rdc_based import candidate_evaluation
from evaluate.confidence_interval_evaluation import evaluate_confidence_intervals
from schemas.flights.schema import gen_flights_1B_schema
from schemas.imdb.schema import gen_job_light_imdb_schema
from schemas.ssb.schema import gen_500gb_ssb_schema
from schemas.tpc_ds.schema import gen_1t_tpc_ds_schema

np.random.seed(1)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--data_collection', default='ssb-500gb', help='Which dataset to be used')

    # generate hdf
    parser.add_argument('--create_hdf', help='Prepare hdf5 files for single tables', action='store_true')
    parser.add_argument('--create_sampled_hdf_files', help='Prepare hdf5 files for single tables', action='store_true')
    parser.add_argument('--column_delimiter', default='|')
    parser.add_argument('--csv_directory', default='../ssb-benchmark')
    parser.add_argument('--hdf_directory', default='../ssb-benchmark/gen_hdf')
    parser.add_argument('--max_rows_per_file', type=int, default=20000000)
    parser.add_argument('--hdf_sample_size', type=int, default=1000000)

    # generate ensembles
    parser.add_argument('--create_ensemble', help='Trains SPNs on schema', action='store_true')
    parser.add_argument('--ensemble_tactic', default='single')
    parser.add_argument('--ensemble_directory', default='../ssb-benchmark/spn_ensembles')
    parser.add_argument('--pairwise_rdc_path', default=None)
    parser.add_argument('--samples_rdc_ensemble_tests', type=int, default=10000)
    parser.add_argument('--samples_per_spn', help="How many samples to use for joins with n tables",
                        nargs='+', type=int, default=[10000000, 10000000, 2000000, 2000000])
    parser.add_argument('--post_sampling_factor', nargs='+', type=int, default=[30, 30, 2, 1])
    parser.add_argument('--rdc_threshold', help='If RDC value is smaller independence is assumed', type=float,
                        default=0.3)
    parser.add_argument('--bloom_filters', help='Generates Bloom filters for grouping', action='store_true')
    parser.add_argument('--ensemble_budget_factor', type=int, default=5)
    parser.add_argument('--ensemble_max_no_joins', type=int, default=3)
    parser.add_argument('--learning_rate_increment', type=int, default=0)
    parser.add_argument('--incremental_condition', type=str, default=None)

    # generate code
    parser.add_argument('--generate_code', help='Generates code for trained SPNs for faster Inference',
                        action='store_true')
    parser.add_argument('--apply_generated_code', action='store_true')

    # ground truth
    parser.add_argument('--aqp_ground_truth', help='Computes ground truth for AQP', action='store_true')
    parser.add_argument('--cardinalities_ground_truth', help='Computes ground truth for Cardinalities',
                        action='store_true')

    # evaluate
    parser.add_argument('--assess_cardinalities', help='Evaluates SPN ensemble to compute cardinalities',
                        action='store_true')
    parser.add_argument('--rdc_spn_selection', help='Uses pairwise rdc values to for the SPN compilation',
                        action='store_true')
    parser.add_argument('--evaluate_cardinalities_scale', help='Evaluates SPN ensemble to compute cardinalities',
                        action='store_true')
    parser.add_argument('--assess_aqp_queries', help='Evaluates SPN ensemble for AQP', action='store_true')
    parser.add_argument('--against_ground_truth', help='Computes ground truth for AQP', action='store_true')
    parser.add_argument('--assess_confidence_intervals',
                        help='Evaluates SPN ensemble and compares stds with true stds', action='store_true')
    parser.add_argument('--confidence_upsampling_factor', type=int, default=300)
    parser.add_argument('--confidence_sample_size', type=int, default=10000000)
    parser.add_argument('--ensemble_location', nargs='+',
                        default=['../ssb-benchmark/spn_ensembles/ensemble_single_ssb-500gb_10000000.pkl',
                                 '../ssb-benchmark/spn_ensembles/ensemble_relationships_ssb-500gb_10000000.pkl'])
    parser.add_argument('--query_file_location', default='./benchmarks/ssb/sql/cardinality_queries.sql')
    parser.add_argument('--ground_truth_file_location',
                        default='./benchmarks/ssb/sql/cardinality_true_cardinalities_100GB.csv')
    parser.add_argument('--database_name', default=None)
    parser.add_argument('--target_path', default='../ssb-benchmark/results')
    parser.add_argument('--raw_folder', default='../ssb-benchmark/results')
    parser.add_argument('--calculate_confidence_intervals', help='Compute confidence intervals', action='store_true')
    parser.add_argument('--max_variants', help='How many spn compilations should be computed for the cardinality '
                                               'estimation. Setting this parameter to 1 means greedy strategy.',
                        type=int, default=1)
    parser.add_argument('--no_exploit_overlapping', action='store_true')
    parser.add_argument('--no_merge_indicator_exp', action='store_true')

    # evaluate of spn ensembles in folder
    parser.add_argument('--hdf_build_path', default='')

    # log level
    parser.add_argument('--logging_level', type=int, default=logging.DEBUG)

    args = parser.parse_args()
    args.exploit_overlapping = not args.no_exploit_overlapping
    args.merge_indicator_exp = not args.no_merge_indicator_exp

    os.makedirs('logs', exist_ok=True)
    logging.basicConfig(
        level=args.logging_level,
        # [%(threadName)-12.12s]
        format="%(asctime)s [%(levelname)-5.5s]  %(message)s",
        handlers=[
            logging.FileHandler("logs/{}_{}.log".format(args.data_collection, time.strftime("%Y%m%d-%H%M%S"))),
            logging.StreamHandler()
        ])
    logger = logging.getLogger(__name__)

    # Generate schema
    table_csv_path = args.csv_directory + '/{}.csv'
    if args.data_collection == 'imdb-light':
        schema = gen_job_light_imdb_schema(table_csv_path)
    elif args.data_collection == 'ssb-500gb':
        schema = gen_500gb_ssb_schema(table_csv_path)
    elif args.data_collection == 'flights1B':
        schema = gen_flights_1B_schema(table_csv_path)
    elif args.data_collection == 'tpc-ds-1t':
        schema = gen_1t_tpc_ds_schema(table_csv_path)
    else:
        raise ValueError('Dataset unknown')

    # Generate HDF files for simpler sampling
    if args.create_hdf:
        logger.info(f"Generating HDF files for tables in {args.csv_directory} and store to path {args.hdf_directory}")

        if os.path.exists(args.hdf_directory):
            logger.info(f"Removing target path {args.hdf_directory}")
            shutil.rmtree(args.hdf_directory)

        logger.info(f"Making target path {args.hdf_directory}")
        os.makedirs(args.hdf_directory)

        prepare_all_tables(schema, args.hdf_directory, column_delimiter=args.column_delimiter,
                           max_table_data=args.max_rows_per_file)
        logger.info(f"Files successfully created")

    # Generate sampled HDF files for fast join calculations
    if args.create_sampled_hdf_files:
        logger.info(f"Generating sampled HDF files for tables in {args.csv_directory} and store to path {args.hdf_directory}")
        prepare_sample_hdf(schema, args.hdf_directory, args.max_rows_per_file, args.hdf_sample_size)
        logger.info(f"Files successfully created")

    # Generate ensemble for cardinality schemas
    if args.create_ensemble:

        if not os.path.exists(args.ensemble_directory):
            os.makedirs(args.ensemble_directory)

        if args.ensemble_tactic == 'single':
            create_naive_all_split_ensemble(schema, args.hdf_directory, args.samples_per_spn[0], args.ensemble_directory,
                                            args.data_collection, args.bloom_filters, args.rdc_threshold,
                                            args.max_rows_per_file, args.post_sampling_factor[0],
                                            learning_rate_increment=args.learning_rate_increment)
        elif args.ensemble_tactic == 'relationship':
            naive_every_relationship_ensemble(schema, args.hdf_directory, args.samples_per_spn[1], args.ensemble_directory,
                                              args.data_collection, args.bloom_filters, args.rdc_threshold,
                                              args.max_rows_per_file, args.post_sampling_factor[0],
                                              learning_rate_increment=args.learning_rate_increment)
        elif args.ensemble_tactic == 'rdc_based':
            logging.info(
                f"maqp(create_ensemble: ensemble_tactic={args.ensemble_tactic}, learning_rate_increment={args.learning_rate_increment}, incremental_condition={args.incremental_condition}, ensemble_directory={args.ensemble_directory})")
            candidate_evaluation(schema, args.hdf_directory, args.samples_rdc_ensemble_tests, args.samples_per_spn,
                                 args.max_rows_per_file, args.ensemble_directory, args.database_name,
                                 args.post_sampling_factor, args.ensemble_budget_factor, args.ensemble_max_no_joins,
                                 args.rdc_threshold, args.pairwise_rdc_path,
                                 learning_rate_increment=args.learning_rate_increment,
                                 incremental_condition=args.incremental_condition)
        else:
            raise NotImplementedError

    # Read pre-trained ensemble and evaluate cardinality queries scale
    if args.generate_code:
        spn_ensemble = read_ensemble(args.ensemble_directory, build_reverse_dict=True)
        generate_ensemble_code(spn_ensemble, floating_data_type='float', ensemble_directory=args.ensemble_directory)

    # Read pre-trained ensemble and evaluate cardinality queries scale
    if args.evaluate_cardinalities_scale:
        from evaluate.cardinality_evaluation import evaluate_cardinalities

        for i in [3, 4, 5, 6]:
            for j in [1, 2, 3, 4, 5]:
                target_path = args.target_path.format(i, j)
                query_file_location = args.query_file_location.format(i, j)
                true_cardinalities_path = args.ground_truth_file_location.format(i, j)
                evaluate_cardinalities(args.ensemble_location, args.database_name, query_file_location, target_path,
                                       schema, args.rdc_spn_selection, args.pairwise_rdc_path,
                                       use_generated_code=args.apply_generated_code,
                                       merge_indicator_exp=args.merge_indicator_exp,
                                       exploit_overlapping=args.exploit_overlapping, max_variants=args.max_variants,
                                       true_cardinalities_path=true_cardinalities_path, min_sample_ratio=0)

    # Read pre-trained ensemble and evaluate cardinality queries
    if args.assess_cardinalities:
        from evaluate.cardinality_evaluation import evaluate_cardinalities

        logging.info(
            f"maqp(assess_cardinalities: database_name={args.database_name}, target_path={args.target_path})")
        evaluate_cardinalities(args.ensemble_location, args.database_name, args.query_file_location, args.target_path,
                               schema, args.rdc_spn_selection, args.pairwise_rdc_path,
                               use_generated_code=args.apply_generated_code,
                               merge_indicator_exp=args.merge_indicator_exp,
                               exploit_overlapping=args.exploit_overlapping, max_variants=args.max_variants,
                               true_cardinalities_path=args.ground_truth_file_location, min_sample_ratio=0)

    # Compute ground truth for AQP queries
    if args.aqp_ground_truth:
        from evaluate.aqp_evaluation import compute_ground_truth

        compute_ground_truth(args.target_path, args.database_name, query_filename=args.query_file_location)

    # Compute ground truth for Cardinality queries
    if args.cardinalities_ground_truth:
        from evaluate.cardinality_evaluation import compute_ground_truth

        compute_ground_truth(args.query_file_location, args.target_path, args.database_name)

    # Read pre-trained ensemble and evaluate AQP queries
    if args.assess_aqp_queries:
        from evaluate.aqp_evaluation import evaluate_aqp_queries

        evaluate_aqp_queries(args.ensemble_location, args.query_file_location, args.target_path, schema,
                             args.ground_truth_file_location, args.rdc_spn_selection, args.pairwise_rdc_path,
                             max_variants=args.max_variants,
                             merge_indicator_exp=args.merge_indicator_exp,
                             exploit_overlapping=args.exploit_overlapping, min_sample_ratio=0, debug=True,
                             show_confidence_intervals=args.calculate_confidence_intervals)

    # Read pre-trained ensemble and evaluate the confidence intervals
    if args.assess_confidence_intervals:
        evaluate_confidence_intervals(args.ensemble_location, args.query_file_location, args.target_path, schema,
                                      args.ground_truth_file_location, args.confidence_sample_size,
                                      args.rdc_spn_selection, args.pairwise_rdc_path,
                                      max_variants=args.max_variants, merge_indicator_exp=args.merge_indicator_exp,
                                      exploit_overlapping=args.exploit_overlapping, min_sample_ratio=0,
                                      true_result_upsampling_factor=args.confidence_upsampling_factor,
                                      sample_size=args.confidence_sample_size)
