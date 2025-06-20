import logging
import os
import pickle
import shutil
from multiprocessing.dummy import Pool as ThreadPool

import defopt
import pandas as pd
import tqdm
import numpy as np
from src.utils import (
    Quarter,
    QuestionConfig,
    generate_quarters,
    read_demo_data,
    read_therapy_data,
)

logger = logging.getLogger("FAERS")


def get_relevant_cases(fn_marked, config, nrows=None):
    if fn_marked.endswith("csv"):
        df_marked = pd.read_csv(fn_marked, nrows=nrows)
    else:
        df_marked = pickle.load(open(fn_marked, "rb"))
        if nrows is not None:
            df_marked = df_marked.head(nrows)
    if "caseid" not in df_marked.columns:
        assert df_marked.index.name == "caseid"
        df_marked.reset_index(inplace=True)

    columns_bookkeeping = ["caseid"]
    columns_info = [
        c for c in df_marked if c.startswith("exposed ") or c.startswith("reacted ")
    ]
    dtypes = {c: str for c in columns_bookkeeping}
    for c in columns_info:
        dtypes[c] = bool

    df_marked = df_marked[columns_bookkeeping + columns_info]
    for c, dt in dtypes.items():
        df_marked[c] = df_marked[c].astype(dt)

    # columns_drugs = [f'drug {d}' for d in config.drugs]
    # columns_reactions = [f'reaction {r}' for r in config.reactions]
    col_exposed = f"exposed {config.name}"
    col_reacted = f"reacted {config.name}"
    drug_true = df_marked[col_exposed]
    reaction_true = df_marked[col_reacted]
    drug_naive = ~df_marked[col_exposed]
    ret = df_marked[columns_bookkeeping].copy()
    ret["true_true"] = (drug_true & reaction_true).values
    ret["true_false"] = (drug_true & (~reaction_true)).values
    ret["drug_naive_true"] = (drug_naive & reaction_true).values
    ret["drug_naive_false"] = (drug_naive & (~reaction_true)).values
    return ret.loc[
        ret.true_true | ret.true_false | ret.drug_naive_true | ret.drug_naive_false
    ]


def process_a_config(q_start, q_end, dir_marked_data, dir_raw_data, dir_out, config):
    os.makedirs(dir_out, exist_ok=True)
    quarters = list(generate_quarters(q_start, q_end))
    row = np.random.randint(1, 10)
    for q in tqdm.tqdm(quarters, position=row, leave=False, desc=config.name):

        dir_out_curr = os.path.join(dir_out, config.name)
        os.makedirs(dir_out_curr, exist_ok=True)
        fn_out = os.path.join(dir_out_curr, f"{q}.csv.zip")
        if os.path.exists(fn_out):
            logger.debug(f"Skipping {q} because {fn_out} already exists")
            continue
        fn_marked = os.path.join(dir_marked_data, f"{q}.pkl")
        fn_demo = os.path.join(dir_raw_data, f"demo{q}.csv.zip")
        fn_therapy = os.path.join(dir_raw_data, f"ther{q}.csv.zip")
        df_cases = get_relevant_cases(fn_marked, config)
        df_demo = read_demo_data(fn_demo)
        df_therapy = read_therapy_data(fn_therapy)
        df_cases = df_cases.merge(df_demo, on="caseid", how="left").merge(
            df_therapy, on="caseid", how="left"
        )
        df_cases.to_csv(fn_out, index=False, compression="zip")


def main(
    *,
    year_q_from,
    year_q_to,
    dir_marked_data,
    dir_raw_data,
    dir_config,
    dir_out,
    threads=4,
    clean_on_failure=False,
):
    """
    :param str year_q_from:
        XXXXqQ, where XXXX is the year, q is the literal "q" and Q is 1, 2, 3 or 4
    :param str year_q_to:
        XXXXqQ, where XXXX is the year, q is the literal "q" and Q is 1, 2, 3 or 4
    :param str dir_marked_data:
        Input directory, where marked report files are stored
    :param str dir_config:
        Input directory, where config files are stored
    :param str dir_raw_data:
        Input directory, where the raw demography data is stored
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
        configs = QuestionConfig.load_config_items(dir_config=dir_config)
        with ThreadPool(threads) as pool:
            _ = list(
                tqdm.tqdm(
                    pool.imap(
                        lambda config: process_a_config(
                            q_start=q_from,
                            q_end=q_to,
                            dir_marked_data=dir_marked_data,
                            dir_raw_data=dir_raw_data,
                            dir_out=dir_out,
                            config=config,
                        ),
                        configs,
                    ),
                    total=len(configs),
                )
            )
    except Exception as err:
        if clean_on_failure:
            shutil.rmtree(dir_out)
        raise err


if __name__ == "__main__":
    defopt.run(main)
