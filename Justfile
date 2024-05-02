shell:
    poetry run ipython

install-dependencies:
    poetry lock
    poetry install

formatcsv path:
    sed -i '1 s/ /_/g' {{path}}

csv2pkl path:
    #!/usr/bin/env python3
    from pathlib import Path
    import pandas as pd
    from pandas.api.types import CategoricalDtype

    path = Path("{{path}}")
    df = pd.read_csv(path)
    df = df.astype({k: CategoricalDtype(ordered=True) for k, d in df.dtypes.items() if d == "O"})
    df.to_pickle(path.with_suffix(".pkl"))

pkl2table dataset version:
    poetry run python -m TraditionalModel dataset table -d{{dataset}} -v{{version}} --overwrite

table2num dataset version:
    poetry run python -m TraditionalModel dataset dump -d{{dataset}} -v{{version}}

syn2postgres dataset='dom1000' version='skew0.0_corr0.0':
    echo "DROP TABLE IF EXISTS \"{{dataset}}_{{version}}\";" > tmp.sql
    echo "CREATE TABLE \"{{dataset}}_{{version}}\"(" >> tmp.sql
    echo "	col0  DOUBLE PRECISION," >> tmp.sql
    echo "	col1  DOUBLE PRECISION);" >> tmp.sql
    echo "\\\copy \"{{dataset}}_{{version}}\" FROM 'data/{{dataset}}/{{version}}.csv' DELIMITER ',' CSV HEADER;" >> tmp.sql
    $PSQL $DATABASE_URL -f tmp.sql

census2postgres version='original' dataset='census13':
    echo "DROP TABLE IF EXISTS \"{{dataset}}_{{version}}\";" > tmp.sql
    echo "CREATE TABLE \"{{dataset}}_{{version}}\"(" >> tmp.sql
    echo "	age  DOUBLE PRECISION," >> tmp.sql
    echo "	workclass  VARCHAR(64)," >> tmp.sql
    echo "	education  VARCHAR(64)," >> tmp.sql
    echo "	education_num  DOUBLE PRECISION," >> tmp.sql
    echo "	marital_status  VARCHAR(64)," >> tmp.sql
    echo "	occupation  VARCHAR(64)," >> tmp.sql
    echo "	relationship  VARCHAR(64)," >> tmp.sql
    echo "	race  VARCHAR(64)," >> tmp.sql
    echo "	sex  VARCHAR(64)," >> tmp.sql
    echo "	capital_gain  DOUBLE PRECISION," >> tmp.sql
    echo "	capital_loss  DOUBLE PRECISION," >> tmp.sql
    echo "	hours_per_week  DOUBLE PRECISION," >> tmp.sql
    echo "	native_country  VARCHAR(64));" >> tmp.sql
    echo "\\\copy \"{{dataset}}_{{version}}\" FROM 'data/{{dataset}}/{{version}}.csv' DELIMITER ',' CSV HEADER;" >> tmp.sql
    $PSQL $DATABASE_URL -f tmp.sql


    echo "DROP TABLE IF EXISTS \"{{dataset}}_{{version}}\";" > tmp.sql
    echo "CREATE TABLE \"{{dataset}}_{{version}}\"(" >> tmp.sql
    echo "	age  DOUBLE PRECISION," >> tmp.sql
    echo "	workclass  DOUBLE PRECISION," >> tmp.sql
    echo "	education  DOUBLE PRECISION," >> tmp.sql
    echo "	education_num  DOUBLE PRECISION," >> tmp.sql
    echo "	marital_status  DOUBLE PRECISION," >> tmp.sql
    echo "	occupation  DOUBLE PRECISION," >> tmp.sql
    echo "	relationship  DOUBLE PRECISION," >> tmp.sql
    echo "	race  DOUBLE PRECISION," >> tmp.sql
    echo "	sex  DOUBLE PRECISION," >> tmp.sql
    echo "	capital_gain  DOUBLE PRECISION," >> tmp.sql
    echo "	capital_loss  DOUBLE PRECISION," >> tmp.sql
    echo "	hours_per_week  DOUBLE PRECISION," >> tmp.sql
    echo "	native_country  DOUBLE PRECISION);" >> tmp.sql
    echo "\\\copy \"{{dataset}}_{{version}}\" FROM 'data/{{dataset}}/{{version}}_num.csv' DELIMITER ',' CSV HEADER;" >> tmp.sql
    $KDE_PSQL $KDE_DATABASE_URL -f tmp.sql

census2mysql version='original' dataset='census13':
    echo "DROP TABLE IF EXISTS \`{{dataset}}_{{version}}\`;" > tmp.sql
    echo "CREATE TABLE \`{{dataset}}_{{version}}\`(" >> tmp.sql
    echo "	age  DOUBLE PRECISION," >> tmp.sql
    echo "	workclass  VARCHAR(64)," >> tmp.sql
    echo "	education  VARCHAR(64)," >> tmp.sql
    echo "	education_num  DOUBLE PRECISION," >> tmp.sql
    echo "	marital_status  VARCHAR(64)," >> tmp.sql
    echo "	occupation  VARCHAR(64)," >> tmp.sql
    echo "	relationship  VARCHAR(64)," >> tmp.sql
    echo "	race  VARCHAR(64)," >> tmp.sql
    echo "	sex  VARCHAR(64)," >> tmp.sql
    echo "	capital_gain  DOUBLE PRECISION," >> tmp.sql
    echo "	capital_loss  DOUBLE PRECISION," >> tmp.sql
    echo "	hours_per_week  DOUBLE PRECISION," >> tmp.sql
    echo "	native_country  VARCHAR(64));" >> tmp.sql
    echo "SET GLOBAL local_infile = 'ON';" >> tmp.sql
    echo "SHOW GLOBAL VARIABLES LIKE 'local_infile';" >> tmp.sql
    echo "LOAD DATA LOCAL INFILE 'data/{{dataset}}/{{version}}.csv' INTO TABLE \`{{dataset}}_{{version}}\` FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;" >> tmp.sql
    $MYSQL --local-infile --protocol tcp -h$MYSQL_HOST --port $MYSQL_PORT -u$MYSQL_USER -p$MYSQL_PSWD $MYSQL_DB < tmp.sql

forest2postgres version='original' dataset='forest10':
    echo "DROP TABLE IF EXISTS \"{{dataset}}_{{version}}\";" > tmp.sql
    echo "CREATE TABLE \"{{dataset}}_{{version}}\"(" >> tmp.sql
    echo "	Elevation  DOUBLE PRECISION," >> tmp.sql
    echo "	Aspect  DOUBLE PRECISION," >> tmp.sql
    echo "	Slope  DOUBLE PRECISION," >> tmp.sql
    echo "	Horizontal_Distance_To_Hydrology  DOUBLE PRECISION," >> tmp.sql
    echo "	Vertical_Distance_To_Hydrology  DOUBLE PRECISION," >> tmp.sql
    echo "	Horizontal_Distance_To_Roadways  DOUBLE PRECISION," >> tmp.sql
    echo "	Hillshade_9am  DOUBLE PRECISION," >> tmp.sql
    echo "	Hillshade_Noon  DOUBLE PRECISION," >> tmp.sql
    echo "	Hillshade_3pm  DOUBLE PRECISION," >> tmp.sql
    echo "	Horizontal_Distance_To_Fire_Points  DOUBLE PRECISION);" >> tmp.sql
    echo "\\\copy \"{{dataset}}_{{version}}\" FROM 'data/{{dataset}}/{{version}}.csv' DELIMITER ',' CSV HEADER;" >> tmp.sql
    $PSQL $DATABASE_URL -f tmp.sql

forest2mysql version='original' dataset='forest10':
    echo "DROP TABLE IF EXISTS \`{{dataset}}_{{version}}\`;" > tmp.sql
    echo "CREATE TABLE \`{{dataset}}_{{version}}\`(" >> tmp.sql
    echo "	Elevation  DOUBLE PRECISION," >> tmp.sql
    echo "	Aspect  DOUBLE PRECISION," >> tmp.sql
    echo "	Slope  DOUBLE PRECISION," >> tmp.sql
    echo "	Horizontal_Distance_To_Hydrology  DOUBLE PRECISION," >> tmp.sql
    echo "	Vertical_Distance_To_Hydrology  DOUBLE PRECISION," >> tmp.sql
    echo "	Horizontal_Distance_To_Roadways  DOUBLE PRECISION," >> tmp.sql
    echo "	Hillshade_9am  DOUBLE PRECISION," >> tmp.sql
    echo "	Hillshade_Noon  DOUBLE PRECISION," >> tmp.sql
    echo "	Hillshade_3pm  DOUBLE PRECISION," >> tmp.sql
    echo "	Horizontal_Distance_To_Fire_Points  DOUBLE PRECISION);" >> tmp.sql
    echo "SET GLOBAL local_infile = 'ON';" >> tmp.sql
    echo "SHOW GLOBAL VARIABLES LIKE 'local_infile';" >> tmp.sql
    echo "LOAD DATA LOCAL INFILE 'data/{{dataset}}/{{version}}.csv' INTO TABLE \`{{dataset}}_{{version}}\` FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;" >> tmp.sql
    $MYSQL --local-infile --protocol tcp -h$MYSQL_HOST --port $MYSQL_PORT -u$MYSQL_USER -p$MYSQL_PSWD $MYSQL_DB < tmp.sql

power2postgres version='original' dataset='power7':
    echo "DROP TABLE IF EXISTS \"{{dataset}}_{{version}}\";" > tmp.sql
    echo "CREATE TABLE \"{{dataset}}_{{version}}\"(" >> tmp.sql
    echo "	Global_active_power  DOUBLE PRECISION," >> tmp.sql
    echo "	Global_reactive_power  DOUBLE PRECISION," >> tmp.sql
    echo "	Voltage  DOUBLE PRECISION," >> tmp.sql
    echo "	Global_intensity  DOUBLE PRECISION," >> tmp.sql
    echo "	Sub_metering_1  DOUBLE PRECISION," >> tmp.sql
    echo "	Sub_metering_2  DOUBLE PRECISION," >> tmp.sql
    echo "	Sub_metering_3  DOUBLE PRECISION);" >> tmp.sql
    echo "\\\copy \"{{dataset}}_{{version}}\" FROM 'data/{{dataset}}/{{version}}.csv' DELIMITER ',' CSV HEADER;" >> tmp.sql
    $PSQL $DATABASE_URL -f tmp.sql

power2mysql version='original' dataset='power7':
    echo "DROP TABLE IF EXISTS \`{{dataset}}_{{version}}\`;" > tmp.sql
    echo "CREATE TABLE \`{{dataset}}_{{version}}\`(" >> tmp.sql
    echo "	Global_active_power  DOUBLE PRECISION," >> tmp.sql
    echo "	Global_reactive_power  DOUBLE PRECISION," >> tmp.sql
    echo "	Voltage  DOUBLE PRECISION," >> tmp.sql
    echo "	Global_intensity  DOUBLE PRECISION," >> tmp.sql
    echo "	Sub_metering_1  DOUBLE PRECISION," >> tmp.sql
    echo "	Sub_metering_2  DOUBLE PRECISION," >> tmp.sql
    echo "	Sub_metering_3  DOUBLE PRECISION);" >> tmp.sql
    echo "SET GLOBAL local_infile = 'ON';" >> tmp.sql
    echo "SHOW GLOBAL VARIABLES LIKE 'local_infile';" >> tmp.sql
    echo "LOAD DATA LOCAL INFILE 'data/{{dataset}}/{{version}}.csv' INTO TABLE \`{{dataset}}_{{version}}\` FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;" >> tmp.sql
    $MYSQL --local-infile --protocol tcp -h$MYSQL_HOST --port $MYSQL_PORT -u$MYSQL_USER -p$MYSQL_PSWD $MYSQL_DB < tmp.sql

dmv2postgres version='original' dataset='dmv11':
    echo "DROP TABLE IF EXISTS \"{{dataset}}_{{version}}\";" > tmp.sql
    echo "CREATE TABLE \"{{dataset}}_{{version}}\"(" >> tmp.sql
    echo "	Record_Type  VARCHAR(64)," >> tmp.sql
    echo "	Registration_Class  VARCHAR(64)," >> tmp.sql
    echo "	State  VARCHAR(64)," >> tmp.sql
    echo "	County  VARCHAR(64)," >> tmp.sql
    echo "	Body_Type  VARCHAR(64)," >> tmp.sql
    echo "	Fuel_Type  VARCHAR(64)," >> tmp.sql
    echo "	Reg_Valid_Date  DOUBLE PRECISION," >> tmp.sql
    echo "	Color  VARCHAR(64)," >> tmp.sql
    echo "	Scofflaw_Indicator  VARCHAR(64)," >> tmp.sql
    echo "	Suspension_Indicator  VARCHAR(64)," >> tmp.sql
    echo "	Revocation_Indicator  VARCHAR(64));" >> tmp.sql
    echo "\\\copy \"{{dataset}}_{{version}}\" FROM 'data/{{dataset}}/{{version}}.csv' DELIMITER ',' CSV HEADER;" >> tmp.sql
    $PSQL $DATABASE_URL -f tmp.sql



dmv2mysql version='original' dataset='dmv11':
    echo "DROP TABLE IF EXISTS \`{{dataset}}_{{version}}\`;" > tmp.sql
    echo "CREATE TABLE \`{{dataset}}_{{version}}\`(" >> tmp.sql
    echo "	Record_Type  VARCHAR(64)," >> tmp.sql
    echo "	Registration_Class  VARCHAR(64)," >> tmp.sql
    echo "	State  VARCHAR(64)," >> tmp.sql
    echo "	County  VARCHAR(64)," >> tmp.sql
    echo "	Body_Type  VARCHAR(64)," >> tmp.sql
    echo "	Fuel_Type  VARCHAR(64)," >> tmp.sql
    echo "	Reg_Valid_Date  DOUBLE PRECISION," >> tmp.sql
    echo "	Color  VARCHAR(64)," >> tmp.sql
    echo "	Scofflaw_Indicator  VARCHAR(64)," >> tmp.sql
    echo "	Suspension_Indicator  VARCHAR(64)," >> tmp.sql
    echo "	Revocation_Indicator  VARCHAR(64));" >> tmp.sql
    echo "SET GLOBAL local_infile = 'ON';" >> tmp.sql
    echo "SHOW GLOBAL VARIABLES LIKE 'local_infile';" >> tmp.sql
    echo "LOAD DATA LOCAL INFILE 'data/{{dataset}}/{{version}}.csv' INTO TABLE \`{{dataset}}_{{version}}\` FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;" >> tmp.sql
    $MYSQL --local-infile --protocol tcp -h$MYSQL_HOST --port $MYSQL_PORT -u$MYSQL_USER -p$MYSQL_PSWD $MYSQL_DB < tmp.sql



data-gen skew='0.0' corr='0.0' dom='1000' col='2' seed='123':
    poetry run python -m TraditionalModel dataset gen -s{{seed}} -ddom{{dom}} -vskew{{skew}}_corr{{corr}} --params \
        "{'row_num': 1000000, 'col_num': {{col}}, 'dom': {{dom}}, 'skew': {{skew}}, 'corr': {{corr}}}"

wkld-gen-vood data version:
    poetry run python -m TraditionalModel workload gen -d{{data}} -v{{version}} -wvood --params \
        "{'attr': {'pred_number': 1.0}, \
        'center': {'vocab_ood': 1.0}, \
        'width': {'uniform': 0.5, 'exponential': 0.5}, \
        'number': {'train': 100000, 'valid': 10000, 'test': 10000}}" --no-label

wkld-gen-base data version name='base':
    poetry run python -m TraditionalModel workload gen -d{{data}} -v{{version}} -w{{name}} --params \
        "{'attr': {'pred_number': 1.0}, \
        'center': {'distribution': 0.9, 'vocab_ood': 0.1}, \
        'width': {'uniform': 0.5, 'exponential': 0.5}, \
        'number': {'train': 100000, 'valid': 10000, 'test': 10000}}"

wkld-gen-base-sth10 data version seed name:
    poetry run python -m TraditionalModel workload gen -s{{seed}} -d{{data}} -v{{version}} -w{{name}} --params \
        "{'attr': {'pred_number': 1.0}, \
        'center': {'distribution': 0.9, 'vocab_ood': 0.1}, \
        'width': {'uniform': 0.5, 'exponential': 0.5}, \
        'number': {'train': 10000, 'valid': 1000, 'test': 1000}}"

wkld-gen-mth10 data version name='base':
    #!/bin/bash
    for s in $(seq 0 9); do
        just wkld-gen-{{name}}-sth10 {{data}} {{version}} $s {{name}}_$s &
    done

wkld-merge data version name:
    poetry run python -m TraditionalModel workload merge -d{{data}} -v{{version}} -w{{name}}

wkld-label data version workload :
    poetry run python -m TraditionalModel workload label -d{{data}} -v{{version}} -w{{workload}}

wkld-quicksel data version workload count='5':
    poetry run python -m TraditionalModel workload quicksel -d{{data}} -v{{version}} -w{{workload}} --params \
        "{'count': {{count}}}" --overwrite

wkld-dump data version workload:
    poetry run python -m TraditionalModel workload dump -d{{data}} -v{{version}} -w{{workload}}




test-postgres dataset='census13' version='original' workload='base' stat_target='10000' train_version='original' seed='123':
    poetry run python -m TraditionalModel test -s{{seed}} -d{{dataset}} -v{{version}} -w{{workload}} -epostgres --params \
        "{'version': '{{train_version}}', 'stat_target': {{stat_target}}}" --overwrite

test-mysql dataset='census13' version='original' workload='base' bucket='1024' train_version='original' seed='123':
    poetry run python -m TraditionalModel test -s{{seed}} -d{{dataset}} -v{{version}} -w{{workload}} -emysql --params \
        "{'version': '{{train_version}}', 'bucket': {{bucket}}}" --overwrite

append-data-ind seed='123' dataset='census13' version='original' ap_size='0.2':
    poetry run python -m TraditionalModel dataset update -d{{dataset}} -s{{seed}} -v{{version}} --params \
        "{'type':'ind', 'batch_ratio':{{ap_size}}}"

append-data-cor seed='123' dataset='census13' version='original' ap_size='0.2':
    poetry run python -m TraditionalModel dataset update -d{{dataset}} -s{{seed}} -v{{version}} --params \
        "{'type':'cor', 'batch_ratio':{{ap_size}}}"

append-data-skew seed='123' dataset='census13' version='original' ap_size='0.2':
    poetry run python -m TraditionalModel dataset update -d{{dataset}} -s{{seed}} -v{{version}} --params \
        "{'type':'skew', 'batch_ratio':{{ap_size}}, 'skew_size':'0.0005'}"

wkld-gen-update-base-train-valid seed='123' dataset='census13' version='original' name='base_update' sample_ratio='0.05':
    poetry run python -m TraditionalModel workload gen -s{{seed}} -d{{dataset}} -v{{version}} -w{{name}} --no-label --params \
        "{'attr': {'pred_number': 1.0}, \
        'center': {'distribution': 0.9, 'vocab_ood': 0.1}, \
        'width': {'uniform': 0.5, 'exponential': 0.5}, \
        'number': {'train': 16000, 'valid': 1000, 'test': 0}}"
    poetry run python -m TraditionalModel workload update-label -s{{seed}} -d{{dataset}} -v{{version}} -w{{name}} --sample-ratio={{sample_ratio}}

wkld-gen-update-base-test seed data version name='base':
    poetry run python -m TraditionalModel workload gen -s{{seed}} -d{{data}} -v{{version}} -w{{name}} --params \
        "{'attr': {'pred_number': 1.0}, \
        'center': {'distribution': 0.9, 'vocab_ood': 0.1}, \
        'width': {'uniform': 0.5, 'exponential': 0.5}, \
        'number': {'train': 0, 'valid': 0, 'test': 10000}}"

### Data-driven model update
update-naru model dataset='census13' version='original' workload='base' seed='123' eq='1':
    poetry run python -m TraditionalModel update-train -s{{seed}} -d{{dataset}} -v{{version}} -w{{workload}} -enaru --overwrite --params \
        "{'model':'{{model}}', 'epochs':{{eq}}}"

update-deepdb model dataset='census13' version='original' workload='base' seed='123':
    poetry run python -m TraditionalModel update-train -s{{seed}} -d{{dataset}} -v{{version}} -w{{workload}} -edeepdb --overwrite --params \
        "{'model':'{{model}}'}"

report-error file dataset='census13':
    poetry run python -m TraditionalModel report -d{{dataset}} --params \
        "{'file': '{{file}}'}"

