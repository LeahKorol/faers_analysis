import os
import shutil
import pandas as pd
import defopt
from pathos.multiprocessing import ProcessingPool as Pool
import tqdm
import numpy as np

from src.utils import Quarter, generate_quarters, load_config_items, filename_from_config, ContingencyMatrix


def create_contingency_matrix(fn_in, fn_out):
    df = pd.read_csv(fn_in)
    gr = df.groupby('q')
    matrices = []
    for q, tbl in tqdm.tqdm(sorted(gr)):
        curr = ContingencyMatrix.from_results_table(tbl)
        curr.tbl['q'] = q
        if len(matrices):
            curr += matrices[-1]
        matrices.append(curr)
    matrices = pd.concat([m.tbl for m in matrices]).reset_index().set_index('q').sort_index()
    matrices.to_csv(fn_out, index=True)
    return matrices




def main(
        *,
        dir_incidence,
        config_dir,
        dir_out,
        clean_on_failure=False
):
    """

    :param str dir_incidence:
        Input directory, where incidence tables are stored
    :param str config_dir:
        Directory with config files
    :param str dir_out:
        Output directory
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
            fn_out = filename_from_config(config, dir_out)
            if os.path.exists(fn_out):
                print(f'File {fn_out} exists. Skipping')
            fn_in = filename_from_config(config, dir_incidence)
            create_contingency_matrix(fn_in, fn_out)

    except Exception as err:
        if clean_on_failure:
            shutil.rmtree(dir_out)
        raise err



if __name__ == '__main__':
    defopt.run(main)
