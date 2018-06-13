import os
import shutil
import pandas as pd
import defopt
from pathos.multiprocessing import ProcessingPool as Pool
import tqdm
import numpy as np

from src.utils import Quarter, generate_quarters, load_config_items, filename_from_config, ContingencyMatrix



def report_from_config(config, fn_in, dir_out, alpha):
    config_name = config.name
    contingency_matrices = pd.read_csv(fn_in)
    gr = contingency_matrices.groupby('q')
    tbl_report = []
    for q, t in gr:
        matrix = ContingencyMatrix(t)
        ror = matrix.ror(alpha=alpha)
        tbl_report.append({
            'q': q,
            'ROR': ror[0],
            'ROR_lower': ror[1][0],
            'ROR_upper': ror[1][1]
        })
    tbl_report = pd.DataFrame.from_records(tbl_report)



def main(
        *,
        dir_coincidence_matrices,
        config_dir,
        dir_out,
        alpha=0.05,
        clean_on_failure=False
):
    """

    :param str dir_coincidence_matrices:
        Input directory, where contingency matrices are stored
    :param str config_dir:
        Directory with config files
    :param str dir_out:
        Output directory
    :param double alpha:
        Used for confidence inteval
    :param bool clean_on_failure:
        ???

    :return: None

    """

    dir_out = os.path.abspath(dir_out)
    os.makedirs(dir_out, exist_ok=True)
    try:
        config_items = load_config_items(config_dir)
        print(f'Will analyze {len(config_items)} configurations: ' + ', '.join([c.name for c in config_items]))
        for config in tqdm.tqdm(config_items):
            dir_out = filename_from_config(config, dir_out, extension='')
            if os.path.exists(dir_out):
                print(f'File {dir_out} exists. Skipping')
                continue
            os.makedirs(dir_out, exist_ok=True)
            fn_in = filename_from_config(config, dir_coincidence_matrices)
            report_from_config(config, fn_in, dir_out, alpha=alpha)

    except Exception as err:
        if clean_on_failure:
            shutil.rmtree(dir_out)
        raise err



if __name__ == '__massssin__':
    defopt.run(main)


if __name__ == '__main__':
    contingency_matrices = pd.read_csv('/Users/boris/devel/faers/data/interim/contingency/belviq.csv')
    gr = contingency_matrices.groupby('q')
    for q, t in gr:
        t = ContingencyMatrix(t)
        break
    ror = t.ror_components()
    print(ror)