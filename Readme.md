# Cardinality Estimation

## Traditional Cardinality Estimation

Linux: curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to /usr/local/bin

Install Poetry: pip install poetry

Install Python dependencies: just install-dependencies

Run: just test-postgres

Note: The query is not generated by the inside code logic and the tables was imported by sql script by hand, also the ANALYZE process was performed by ourselves, since we use this code logic to test the performance of cardinality estimation on multitable dataset.

For Postgres, it's the same. Since the MySQL database has a different logic for obtaining multi-table test results, we redirect the MySQL output to a file before calculating it through the script.

## Machine Learning Based Cardinality Estimation

### DeepDB

```shell
cd DeepDB
sudo apt install -y libpq-dev gcc python3-dev
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
```

For python3.8: Sometimes spflow fails, in this case remove spflow from requirements.txt, install them and run
```shell
pip3 install spflow --no-deps
```

Download the [Job dataset](http://homepages.cwi.nl/~boncz/job/imdb.tgz).
Generate hdf files from csvs.
```
python3 main.py --create_hdf
  --data_collection imdb-light
  --column_delimiter ,
  --csv_directory ../imdb-benchmark
  --hdf_directory ../imdb-benchmark/gen_hdf
  --max_rows_per_file 100000000
```

Generate sampled hdf files from csvs.
```
python3 main.py --create_sampled_hdf_files
  --data_collection imdb-light
  --hdf_directory ../imdb-benchmark/gen_hdf
  --max_rows_per_file 100000000
  --hdf_sample_size 10000
```

Learn ensemble with the optimized rdc strategy (requires postgres with imdb dataset)
```
python3 main.py --create_ensemble
  --data_collection imdb-light
  --samples_per_spn 10000000 10000000 1000000 1000000 1000000
  --ensemble_tactic rdc_based
  --ensemble_directory ../imdb-benchmark/spn_ensembles
  --pairwise_rdc_path ../imdb-benchmark/spn_ensembles/pairwise_rdc.pkl
  --samples_rdc_ensemble_tests 10000
  --hdf_build_path ../imdb-benchmark/gen_hdf
  --rdc_threshold 0.3
  --bloom_filters
  --ensemble_budget_factor 5
  --ensemble_max_no_joins 3
  --learning_rate_increment 0
```

Alternatively: Learn base ensemble over different tables with naive strategy. 
(Does not work with different dataset sizes because join sizes are hard coded but does not require postgres)
```
python3 main.py --create_ensemble
  --data_collection imdb-light
  --samples_per_spn 1000000 1000000 1000000 1000000 1000000
  --ensemble_tactic relationship
  --ensemble_directory ../imdb-benchmark/spn_ensembles
  --hdf_build_path ../imdb-benchmark/gen_hdf
```

Evaluate performance for queries.
```
python3 main.py --assess_cardinalities
  --rdc_spn_selection
  --max_variants 1
  --pairwise_rdc_path ../imdb-benchmark/spn_ensembles/pairwise_rdc.pkl
  --data_collection imdb-light
  --target_path ./baselines/cardinality_estimation/results/deepDB/imdb_light_model_based_budget_5.csv
  --ensemble_location ../imdb-benchmark/spn_ensembles/ensemble_join_3_budget_5_10000000.pkl
  --query_file_location ./benchmarks/job-light/sql/job_light_queries.sql
  --ground_truth_file_location ./benchmarks/job-light/sql/job_light_true_cardinalities.csv
```

## Updates

Conditional incremental learning (i.e., initial learning of all films before 2013, newer films learn incremental)
```
python3 main.py --create_ensemble
  --data_collection imdb-light
  --samples_per_spn 10000000 10000000 1000000 1000000 1000000
  --ensemble_tactic rdc_based
  --ensemble_directory ../imdb-benchmark/spn_ensembles
  --hdf_build_path ../imdb-benchmark/gen_hdf
  --samples_rdc_ensemble_tests 10000
  --rdc_threshold 0.3
  --ensemble_budget_factor 0
  --ensemble_max_no_joins 3
  --learning_rate_increment 0
  --pairwise_rdc_path ../imdb-benchmark/spn_ensembles/pairwise_rdc.pkl
  --incremental_condition "title.production_year<2013"
```

## Acknowledgments

Special thanks to the contributors of [AreCELearnedYet](https://github.com/sfu-db/AreCELearnedYet), [DeepDB](https://github.com/DataManagementLab/deepdb-public) for their work that inspired and informed this project.