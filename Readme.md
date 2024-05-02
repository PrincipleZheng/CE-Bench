# Cardinality Estimation

## Traditional Cardinality Estimation

Linux: curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to /usr/local/bin

Install Poetry: pip install poetry

Install Python dependencies: just install-dependencies

Run: just test-postgres

Note: The query is not generated by the inside code logic and the tables was imported by sql script by hand, also the ANALYZE process was performed by ourselves, since we use this code logic to test the performance of cardinality estimation on multitable dataset.

For Postgres, it's the same. Since the MySQL database has a different logic for obtaining multi-table test results, we redirect the MySQL output to a file before calculating it through the script.

## Learned Cardinality Estimation

### MSCN
Requirements:
- pytorch 1.0
- python 3.7

Usage:
cd mscn && python3 train.py --queries 100000 --epochs 100 job-light

Reference:
[learnedcadinalities](https://github.com/andreaskipf/learnedcardinalities) for the mscn model architechture and training.

## Acknowledgments

Special thanks to the contributors of [AreCELearnedYet](https://github.com/sfu-db/AreCELearnedYet) for their work that inspired and informed this project.
