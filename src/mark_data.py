import os
import shutil
import warnings
import pickle

import pandas as pd
import defopt
from pathos.multiprocessing import ThreadPool as Pool
import tqdm

from src import utils
from src.utils import Quarter, generate_quarters, QuestionConfig




def mark_drug_data(df, drug_names):
    df.drugname = df.drugname.apply(QuestionConfig.normalize_drug_name)
    for drug in sorted(drug_names):
        df[f'drug {drug}'] = df.drugname == drug
    drug_columns = [f'drug {drug}' for drug in drug_names]
    ret = df.groupby('caseid')[drug_columns].any()
    return ret


def mark_reaction_data(df, reaction_types):
    df.pt = df.pt.apply(QuestionConfig.normalize_reaction_name)
    for reaction in sorted(reaction_types):
        df[f'reaction {reaction}'] = df.pt == reaction
    reaction_columns = [f'reaction {reaction}' for reaction in reaction_types]
    ret = df.groupby('caseid')[reaction_columns].any()
    return ret


def mark_data(df_drug, df_reac, df_demo, config_items):
    cols_to_collect = list(df_demo.columns)
    df_merged = df_demo.join(df_reac).join(df_drug)
    for config in config_items:
        drugs_curr = set(config.drugs)
        drug_columns = [f'drug {drug}' for drug in drugs_curr]
        exposed = f'exposed {config.name}'
        df_merged[exposed] = df_merged[drug_columns].any(axis=1)
        cols_to_collect.append(exposed)
        if config.control is not None:
            control_columns = [f'drug {drug}' for drug in config.control]
            control = f'control {config.name}'
            df_merged[control] = df_merged[control_columns].any(axis=1)
            cols_to_collect.append(control)
        reaction_columns = [f'reaction {reaction}' for reaction in config.reactions]
        reacted = f'reacted {config.name}'
        df_merged[reacted] = df_merged[reaction_columns].any(axis=1)
        cols_to_collect.append(reacted)
    ret = df_merged # [cols_to_collect]
    return ret


def process_quarter(q, dir_in, dir_out, config_items, drug_names, reaction_types):
    DEBUG = None
    fn_drug = os.path.join(dir_in, f'drug{q}.csv.zip')
    df_drug = pd.read_csv(
        fn_drug, dtype=str, nrows=DEBUG
    )[
        ['primaryid', 'caseid', 'drugname']
    ].dropna(
    ).drop_duplicates(subset=['primaryid', 'drugname'])
    df_drug = mark_drug_data(df_drug, drug_names)

    fn_reac = os.path.join(dir_in, f'reac{q}.csv.zip')
    df_reac = pd.read_csv(
        fn_reac, dtype=str, nrows=DEBUG
    )[
        ['primaryid', 'caseid', 'pt']
    ].dropna(
    ).drop_duplicates(
    )
    df_reac = mark_reaction_data(df_reac, reaction_types)

    fn_demo = os.path.join(dir_in, f'demo{q}.csv.zip')
    df_demo = utils.read_demo_data(fn_demo, nrows=DEBUG).set_index('caseid')
    df_demo['q'] = str(q)
    df_marked = mark_data(df_drug=df_drug, df_reac=df_reac, df_demo=df_demo, config_items=config_items)
    pickle.dump(
        df_marked,
        open(os.path.join(
            dir_out,
            f'{q}.pkl'
        ), 'wb'
        )
    )



def main(
        *,
        year_q_from,
        year_q_to,
        dir_in,
        config_dir,
        dir_out,
        threads=1,
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
        config_items = QuestionConfig.load_config_items(config_dir)
        drug_names = set()
        reaction_types = set()
        for config in config_items:
            drug_names.update(set(config.drugs))
            if config.control is not None:
                drug_names.update(set(config.control))
            reaction_types.update(set(config.reactions))
        print(f'Will analyze {len(drug_names)} drugs and {len(reaction_types)} reactions')
        quarters = list(generate_quarters(q_from, q_to))
        with Pool(threads) as pool:
            _ = list(
                tqdm.tqdm(
                    pool.imap(
                        lambda q: process_quarter(q, dir_in=dir_in, dir_out=dir_out, config_items=config_items, drug_names=drug_names, reaction_types=reaction_types),
                        quarters
                    ),
                    total=len(quarters)
                )
            )
    except Exception as err:
        if clean_on_failure:
            shutil.rmtree(dir_out)
        raise err



if __name__ == '__main__':
    tup = defopt.run(main)