import os
import shutil
import warnings
from collections import defaultdict

import pandas as pd
import defopt
from pathos.multiprocessing import ProcessingPool as Pool
import tqdm

from src.utils import Quarter, load_config_items, filename_from_config, generate_quarters


def load_quarter_data(dir_data, q):
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
    return df_merged


def mark_incidence_data(q, df, drug_names, reaction_types, skip_if_exists):
    tups = []
    for d in drug_names:
        column_name = f'drug {d}'
        if column_name in df and skip_if_exists:
            continue
        tups.append(('drugname', d, column_name))
    for r in reaction_types:
        column_name = f'reaction {r}'
        if column_name in df and skip_if_exists:
            continue
        tups.append(('pt', r,  column_name))

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            pos = int(str(q)[-1])
        except:
            pos = 5
        for tup in tqdm.tqdm(tups, desc=str(q), position=pos, leave=False):
            ref_column_name, item_name, new_column_name = tup
            df[new_column_name] = (df[ref_column_name] == item_name).values

    return df



def _actual_mark_incidence(q, dir_in, dir_out, drug_names, reaction_types, skip_if_exists):
    fn_out = os.path.join(dir_out, f'{str(q)}.csv')
    if os.path.exists(fn_out):
        df_curr = pd.read_csv(fn_out)
    else:
        df_curr = load_quarter_data(dir_in, q)
    df_curr = mark_incidence_data(q, df_curr, drug_names, reaction_types, skip_if_exists)
    df_curr.to_csv(fn_out, index=False)
    counters = dict()
    for d in drug_names:
        cn = f'drug {d}'
        counters[cn] = df_curr[cn].sum()
    for r in reaction_types:
        cn = f'reaction {r}'
        counters[cn] = df_curr[cn].sum()
    return counters




def main(
        *,
        year_q_from,
        year_q_to,
        dir_in,
        config_dir,
        dir_out,
        skip_if_exists=False,
        threads=4,
        clean_on_failure=True
):

    # --skip-if-exists --year-q-from=$(QUARTER_FROM) --year-q-to=$(QUARTER_TO) --dir-in=$(DIR_FAERS_DEDUPLICATED) --config-dir=$(CONFIG_DIR) --dir-out=$(DIR_MARKED_FILES) -t $(N_THREADS) --no-clean-on-failure
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
    :param bool skip_if_exists:
        If a column exists in the output file, leave it and skip the computation
    :param int threads:
        Threads in parallel processing
    :param bool clean_on_failure:
        ???

    :return: None

    """

    dir_out = os.path.abspath(dir_out)
    os.makedirs(dir_out, exist_ok=True)
    try:
        q_from = Quarter(year_q_from)
        q_to = Quarter(year_q_to)
        config_items = load_config_items(config_dir)
        drug_names = set()
        reaction_types = set()
        for config in config_items:
            drug_names.update(set(config.drugs))
            reaction_types.update(set(config.reactions))
        print(f'Will analyze {len(drug_names)} drugs and {len(reaction_types)} reactions')
        quarters = list(generate_quarters(q_from, q_to))
        with Pool(threads) as pool:
            individual_counters = list(
                tqdm.tqdm(
                    pool.imap(
                        lambda q: _actual_mark_incidence(
                            q, dir_in, dir_out, drug_names, reaction_types, skip_if_exists
                        ), quarters
                    ),
                    total=len(quarters)
                )
            )
        counters = defaultdict(int)
        for curr_counter in individual_counters:
            for c, v in curr_counter.items():
                counters[c] += v

        missing_items = sorted([k for k, v in counters.items() if v == 0])
        if missing_items:
            msg = 'Items with zero counts: ' + ', '.join(missing_items)
            warnings.warn(msg)
    except Exception as err:
        if clean_on_failure:
            shutil.rmtree(dir_out)
        raise err



if __name__ == '__main__':
    tup = defopt.run(main)