import os
import shutil
import pandas as pd
import defopt
from pathos.multiprocessing import ProcessingPool as Pool
import tqdm
import numpy as np

from src.utils import Quarter, generate_quarters, QuestionConfig, ContingencyMatrix


def count_quarter_incidence(q, dir_in, config_items):
    data = pd.read_csv(
        os.path.join(dir_in, f'{str(q)}.csv'),
        dtype={'primaryiddrug': str, 'caseid': 'str', 'primaryidreac': str}
    )
    ret = {}
    for config in config_items:
        cols_exposure = [f'exposed {d}' for d in config.drugs]
        exposure = data[['caseid'] + cols_exposure].set_index('caseid')
        exposure = exposure[cols_exposure].any(axis=1)
        exposure = exposure.groupby(exposure.index).apply(any)

        cols_outcome = [f'reaction {r}' for r in config.reactions]
        outcome = data[['caseid'] + cols_outcome].set_index('caseid')
        outcome = outcome[cols_outcome].any(axis=1)
        outcome = outcome.groupby(outcome.index).apply(any)

        cm = ContingencyMatrix.from_results_table(pd.DataFrame({'exposure': exposure, 'outcome': outcome}))
        cm.tbl['q'] = str(q)
        ret[config.name] = cm
    return ret


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
        config_items = QuestionConfig.load_config_items(config_dir)
        print(f'Will analyze {len(config_items)} configurations: ' + ', '.join([c.name for c in config_items]))
        quarters = list(generate_quarters(q_from, q_to))
        with Pool(threads) as pool:
            contingency_matrices = list(
                tqdm.tqdm(
                    pool.imap(
                        lambda q: count_quarter_incidence(
                            q, dir_in, config_items
                        ), quarters
                    ),
                    total=len(quarters), desc='Processing'
                )
            )
        for config in tqdm.tqdm(config_items, desc='Constructing'):
            cms = [i[config.name] for i in contingency_matrices]
            for cm in cms:
                if len(cm.tbl) != 4:
                    print('h')
                assert len(cm.tbl) == 4
            df = pd.concat([cm.tbl for cm in cms]).reset_index().set_index('q').sort_index()
            fn = config.filename_from_config(directory=dir_out, extension='.csv')
            df.to_csv(fn)




    except Exception as err:
        if clean_on_failure:
            shutil.rmtree(dir_out)
        raise err



if __name__ == '__main__':
    # config_items = load_config_items('/Users/boris/devel/faers/config')
    # count_quarter_incidence('2015q1', '/Users/boris/devel/faers/data/interim/marked_data', config_items)
    import sys
    sys.argv = ' src/count_incidence.py --year-q-from=2013q1 --year-q-to=2017q4  --dir-in=/Users/boris/devel/faers/data/interim/marked_data --config-dir=/Users/boris/devel/faers/config --dir-out=/Users/boris/devel/faers/data/interim/contingency -t 8 --no-clean-on-failure'.split()
    defopt.run(main)