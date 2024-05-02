from ast import literal_eval
from time import time

from docopt import docopt

from .workload.gen_workload import generate_workload
from .workload.gen_label import generate_labels, update_labels
from .workload.merge_workload import merge_workload
from .workload.dump_quicksel import dump_quicksel_query_files, generate_quicksel_permanent_assertions
from .dataset.dataset import load_table, dump_table_to_num
from .dataset.gen_dataset import generate_dataset
from .dataset.manipulate_dataset import gen_appended_dataset
from .estimator.postgres import test_postgres
from .estimator.mysql import test_mysql
from .estimator.utils import report_errors, report_dynamic_errors
from .workload.workload import dump_sqls

if __name__ == "__main__":
    args = docopt(__doc__, version="Le Carb 0.1")

    seed = args["--seed"]
    if seed is None:
        seed = int(time())
    else:
        seed = int(seed)

    if args["workload"]:
        if args["gen"]:
            generate_workload(
                seed,
                dataset=args["--dataset"],
                version=args["--dataset-version"],
                name=args["--workload"],
                no_label = args["--no-label"],
                old_version=args["--old-version"],
                win_ratio=args["--win-ratio"],
                params = literal_eval(args["--params"])
            )
        elif args["label"]:
            generate_labels(
                dataset=args["--dataset"],
                version=args["--dataset-version"],
                workload=args["--workload"]
            )
        elif args["update-label"]:
            update_labels(
                seed,
                dataset=args["--dataset"],
                version=args["--dataset-version"],
                workload=args["--workload"],
                sampling_ratio=literal_eval(args["--sample-ratio"])
            )
        elif args["merge"]:
            merge_workload(
                dataset=args["--dataset"],
                version=args["--dataset-version"],
                workload=args["--workload"]
            )
        elif args["quicksel"]:
            dump_quicksel_query_files(
                dataset=args["--dataset"],
                version=args["--dataset-version"],
                workload=args["--workload"],
                overwrite=args["--overwrite"]
            )
            generate_quicksel_permanent_assertions(
                dataset=args["--dataset"],
                version=args["--dataset-version"],
                params=literal_eval(args["--params"]),
                overwrite=args["--overwrite"]
            )
        elif args["dump"]:
            dump_sqls(
                dataset=args["--dataset"],
                version=args["--dataset-version"],
                workload=args["--workload"])
        else:
            raise NotImplementedError
        exit(0)

    if args["dataset"]:
        if args["table"]:
            load_table(args["--dataset"], args["--dataset-version"], overwrite=args["--overwrite"])
        elif args["gen"]:
            generate_dataset(
                seed,
                dataset=args["--dataset"],
                version=args["--dataset-version"],
                params=literal_eval(args["--params"]),
                overwrite=args["--overwrite"]
            )
        elif args["update"]:
            gen_appended_dataset(
                seed,
                dataset=args["--dataset"],
                version=args["--dataset-version"],
                params=literal_eval(args["--params"]),
                overwrite=args["--overwrite"]
            )
        elif args["dump"]:
            dump_table_to_num(args["--dataset"], args["--dataset-version"])
        else:
            raise NotImplementedError
        exit(0)



    if args["test"]:
        dataset = args["--dataset"]
        version = args["--dataset-version"]
        workload = args["--workload"]
        params = literal_eval(args["--params"])
        overwrite = args["--overwrite"]

        
        if args["--estimator"] == "postgres":
            test_postgres(seed, dataset, version, workload, params, overwrite)
        elif args["--estimator"] == "mysql":
            test_mysql(seed, dataset, version, workload, params, overwrite)
        else:
            raise NotImplementedError
        exit(0)

    if args["report"]:
        dataset = args["--dataset"]
        params = literal_eval(args["--params"])
        report_errors(dataset, params['file'])
        exit(0)
    
    if args["report-dynamic"]:
        dataset = args["--dataset"]
        params = literal_eval(args["--params"])
        report_dynamic_errors(dataset, params['old_new_file'], params['new_new_file'], params['T'], params['update_time'])
        exit(0)
