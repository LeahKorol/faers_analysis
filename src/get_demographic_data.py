import os
import shutil
from multiprocessing.dummy import Pool as ThreadPool

import defopt
import pandas as pd
import tqdm
import numpy as np
from src.utils import Quarter, QuestionConfig, generate_quarters


def get_relevant_cases(fn_marked, config, nrows=None):
    df_marked = pd.read_csv(
        fn_marked,
        nrows=nrows
    )
    columns_bookkeeping = ['caseid']
    columns_info = [c for c in df_marked if c.startswith('drug ') or c.startswith('reaction ')]
    dtypes = {c: str for c in columns_bookkeeping}
    for c in columns_info:
        dtypes[c] = bool
    df_marked = pd.read_csv(
        fn_marked,
        usecols=columns_bookkeeping + columns_info,
        dtype=dtypes
    )

    columns_drugs = [f'drug {d}' for d in config.drugs]
    columns_reactions = [f'reaction {r}' for r in config.reactions]
    drug_true = df_marked[columns_drugs].any(axis=1)
    reaction_true = df_marked[columns_reactions].any(axis=1)
    drug_naive = ~df_marked[columns_drugs].any(axis=1)
    ret = df_marked[columns_bookkeeping].copy()
    ret['true_true'] = (drug_true & reaction_true).values
    ret['true_false'] = (drug_true & (~reaction_true)).values
    ret['drug_naive_true'] = (drug_naive & reaction_true).values
    ret['drug_naive_false'] = (drug_naive & (~reaction_true)).values
    return ret.loc[
        ret.true_true | ret.true_false | ret.drug_naive_true | ret.drug_naive_false
        ]


def read_demo_data(fn_demo):
    dtypes = {
        'caseid': str,
        'event_dt_num': str,
        'age': float,
        'age_cod': str,
        'sex': str,
        'wt': float,
        'wt_cod': str
    }
    df_demo = pd.read_csv(
        fn_demo,
        dtype=dtypes,
        usecols=dtypes.keys()
    )

    to_year_conversion_factor = {
        'YR': 1.0,
        'DY': 365.25,
        'MON': 12,
        'DEC': 0.1,
        'WK': 52.2,
        'HR': 24 * 365.25
    }
    to_year_conversion_factor = pd.Series(to_year_conversion_factor)

    to_kg_conversion_factor = {
        'KG': 1.0,
        'LBS': 2.20462
    }
    to_kg_conversion_factor = pd.Series(to_kg_conversion_factor)
    df_demo.wt = df_demo.wt / to_kg_conversion_factor.reindex(df_demo.wt_cod.values).values
    df_demo.age = df_demo.age / to_year_conversion_factor.reindex(df_demo.age_cod.values).values
    df_demo['event_date'] = pd.to_datetime(df_demo.event_dt_num, dayfirst=False, errors='ignore')
    df_demo.drop(['age_cod', 'wt_cod', 'event_dt_num'], axis=1, inplace=True)
    return df_demo


def process_a_config(q_start, q_end, dir_marked_data, dir_raw_demography_data, dir_out, config):
    os.makedirs(dir_out, exist_ok=True)
    quarters = list(generate_quarters(q_start, q_end))
    row = np.random.randint(1, 10)
    for q in tqdm.tqdm(
        quarters, position=row, leave=False,
        desc=config.name
    ):
        fn_marked = os.path.join(dir_marked_data, f'{q}.csv')
        fn_demo = os.path.join(dir_raw_demography_data, f'demo{q}.csv.zip')
        df_cases = get_relevant_cases(fn_marked, config)
        df_demo = read_demo_data(fn_demo)
        df_cases = df_cases.merge(
            df_demo,
            on='caseid',
            how='left'
        )
        dir_out_curr = os.path.join(dir_out, config.name)
        os.makedirs(dir_out_curr, exist_ok=True)
        fn_out = os.path.join(dir_out_curr, f'{q}.csv.zip')
        df_cases.to_csv(fn_out, index=False, compression='zip')


def main(
        *,
        year_q_from,
        year_q_to,
        dir_marked_data,
        dir_raw_demography_data,
        dir_config,
        dir_out,
        threads=4,
        clean_on_failure=False
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
    :param str dir_raw_demography_data:
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
                            q_start=q_from, q_end=q_to, dir_marked_data=dir_marked_data,
                            dir_raw_demography_data=dir_raw_demography_data, dir_out=dir_out,
                            config=config
                        ),
                        configs
                    ),
                    total=len(configs)
                )
            )
    except Exception as err:
        if clean_on_failure:
            shutil.rmtree(dir_out)
        raise err


if __name__ == '__main__':
    defopt.run(main)
