import os
import shutil
import pandas as pd
import defopt
from pathos.multiprocessing import ProcessingPool as Pool
import tqdm
import numpy as np

from src.utils import Quarter, generate_quarters, load_config_items, filename_from_config


def load_data(dir_data, quarter_from, quarter_to):
    data = []
    total_rows = 0
    n_quarters = len(list(generate_quarters(quarter_from, quarter_to)))
    print(f"Loading {n_quarters} data files")
    for q in tqdm.tqdm(generate_quarters(quarter_from, quarter_to), total=n_quarters):
        fn_drug = os.path.join(dir_data, f'drug{q}.csv.zip')
        fn_reaction = os.path.join(dir_data, f'reac{q}.csv.zip')
        df_drug = pd.read_csv(
            fn_drug, dtype=str
        )[
            ['primaryid', 'caseid', 'drugname']
        ].dropna(
        ).drop_duplicates(subset=['primaryid', 'drugname'])
        df_reac = pd.read_csv(
            fn_reaction, dtype=str
        )[
            ['primaryid', 'caseid', 'pt']
        ].dropna(
        ).drop_duplicates(
        )
        df_merged = df_drug.merge(df_reac, on='caseid', suffixes=['drug', 'reac'])
        df_merged['q'] = str(q)
        df_merged['pt'] = df_merged.pt.str.upper()
        data.append(df_merged)
        total_rows += len(df_merged)
    data = pd.concat(data).set_index('caseid', drop=False)
    print(f'Analyzing {len(data):,d} rows')
    return data




def _actual_count_incidence(data, config):
    def classify_case(df_case, drugs, reactions):
        return {
            'exposure':  df_case.drugname.isin(drugs).any(),
            'outcome': df_case.pt.isin(reactions).any(),
            'q': df_case['q'].iloc[0]
        }

    pos = data['thread'].iloc[0] + 1
    tqdm.tqdm.pandas(desc=f'chunk {pos:2d}', mininterval=0.5, position=pos, leave=False)
    gr = data.groupby(
        data.index
    )
    res = gr.progress_apply(lambda t: classify_case(t, config.drugs, config.reactions))
    res = pd.DataFrame.from_records(res, index=res.index)
    return res


def count_incidence(data, config, fn_out, threads):
    data['thread'] = data.caseid.apply(lambda i: hash(i) % (threads ))
    chunks = data.groupby('thread')
    with Pool(threads) as pool:
        results = list(pool.imap(lambda tup: _actual_count_incidence(tup[1], config), chunks))
    print(f'len results {len(results)}')
    results = pd.concat(results)
    results.to_csv(fn_out, index=True)
    return results


def main(
        *,
        year_q_from,
        year_q_to,
        dir_in,
        config_dir,
        dir_out,
        threads=4,
        clean_on_failure=True
):
    """

    :param str year_q_from:
        XXXXqQ, where XXXX is the year, q is the literal "q" and Q is 1, 2, 3 or 4
    :param str year_q_to:
        XXXXqQ, where XXXX is the year, q is the literal "q" and Q is 1, 2, 3 or 4
    :param str dir_in:
        Input directory
    :param str config_dir:
        Directory with config files
    :param str dir_out:
        Output directory
    :param int threads:
        N of parallel threads
    :param bool clean_on_failure:
        ???

    :return: None

    """

    dir_out = os.path.abspath(dir_out)
    os.makedirs(dir_out, exist_ok=True)
    try:
        q_from = Quarter(year_q_from)
        q_to = Quarter(year_q_to)
        data = None
        config_items = load_config_items(config_dir)
        print(f'Will analyze {len(config_items)} configurations: ' + ', '.join([c.name for c in config_items]))
        for config in tqdm.tqdm(config_items):
            fn_out = filename_from_config(config, dir_out)
            if os.path.exists(fn_out):
                print(f'File {fn_out} exists. Skipping')
                continue
            if data is None:
                data = load_data(dir_in, q_from, q_to)
            count_incidence(data, config, fn_out, threads)

    except Exception as err:
        if clean_on_failure:
            shutil.rmtree(dir_out)
        raise err



if __name__ == '__main__':
    defopt.run(main)