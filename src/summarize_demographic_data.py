import os
import shutil
import warnings
from multiprocessing.dummy import Pool as ThreadPool

import defopt
import pandas as pd
import tqdm
import numpy as np
from src.utils import Quarter, QuestionConfig, generate_quarters
from glob import glob
from statsmodels.nonparametric.kde import KDEUnivariate
import statsmodels.api as sm



def regression(df_demo, name):
    true_true = df_demo.loc[df_demo.true_true][['age', 'wt', 'sex']].dropna()
    true_true['side_effect'] = True
    true_true['exposure'] = 1

    true_false = df_demo.loc[df_demo.true_false][['age', 'wt', 'sex']].dropna()
    true_false['side_effect'] = False
    true_false['exposure'] = 1

    false_true = df_demo.loc[df_demo.drug_naive_true][['age', 'wt', 'sex']].dropna()
    false_true['exposure'] = 0
    false_true['side_effect'] = 1

    false_false = df_demo.loc[df_demo.drug_naive_false][['age', 'wt', 'sex']].dropna()
    false_false['exposure'] = 0
    false_false['side_effect'] = 0


    df_regression = pd.concat([true_true, true_false, false_true, false_false], sort=False).reset_index(drop=True)
    df_regression['is_female'] = df_regression['sex'] == 'F'
    df_regression.drop('sex', axis=1, inplace=True)
    df_regression['intercept'] = 1.0
    regression_cols = [c for c in df_regression.columns if c != 'side_effect']
    df_regression.side_effect = df_regression.side_effect.astype(int)
    df_regression.is_female = df_regression.is_female.astype(int)

    logit = sm.Logit(df_regression['side_effect'], df_regression[regression_cols])
    result = logit.fit()
    html_summary = result.summary().as_html()
    return html_summary(title=name)


def summarize_config(config, dir_in, dir_out):
    dir_demo_data = config.filename_from_config(dir_in, extension='')
    files = glob(os.path.join(dir_demo_data, '*.csv.zip'))
    df_demo = pd.concat(
        [pd.read_csv(f) for f in files]
    )
    rows = []
    for label in ['true_true', 'true_false', 'drug_naive_true', 'drug_naive_false']:
        for variable in ['age', 'wt']:
            for sex in ['M', 'F', None]:
                sel = df_demo[label]
                if sex is not None:
                    sel = sel & (df_demo['sex'] == sex)
                if not sel.any():
                    row = {
                        'label': label,
                        'variable': variable,
                        'sex': sex if sex else 'all',
                        'n': 0, 'n_valid': 0
                    }
                    rows.append(row)
                    continue
                values = df_demo.loc[sel][variable]
                n = len(values)
                n_missing = pd.isna(values).sum()
                values = values.dropna()
                cutoff_min = 0
                if variable == 'age':
                    cutoff_max = 120
                else:
                    cutoff_max = 240
                values = values[(values > cutoff_min) & (values <= cutoff_max)]
                n_valid = len(values)
                mean_ = np.mean(values)
                std_ = np.std(values)
                median_ = np.median(values)

                if len(values) > 10:
                    x = np.arange(cutoff_min, cutoff_max)
                    try:
                        kde = KDEUnivariate(values)
                        kde.fit()
                        kde_values = kde.evaluate(x)
                    except:
                        kde_values = np.repeat(np.nan, len(x))
                else:
                    counts = pd.value_counts(values).sort_index()
                    x = counts.index.values
                    kde_values = counts

                row = {
                    'label': label,
                    'variable': variable,
                    'sex': sex if sex else 'all',
                    'n': n,
                    'n_missing': n_missing,
                    'n_valid': n_valid,
                    'mean': mean_,
                    'median': median_,
                    'std': std_,
                    'kde': (x.tolist(), kde_values.tolist())
                }
                rows.append(row)
    processed = pd.DataFrame(rows)
    processed = processed [  # give a nice order
        [
            'label', 'variable', 'sex', 'n', 'n_missing', 'n_valid', 'mean', 'median', 'std', 'kde'
        ]
    ]
    fn_out = config.filename_from_config(dir_out, extension='.csv')
    processed.to_csv(fn_out, index=False)

    html_regression = regression(df_demo, name=config.name)
    fn_out = config.filename_from_config(dir_out, extension='.html')
    open(fn_out, 'w').write(html_regression)


def main(
        *,
        dir_demography_data,
        dir_config,
        dir_out,
        threads=4,
        clean_on_failure=False
):
    """

    :param str dir_config:
        Input directory, where config files are stored
    :param str dir_demography_data:
        Input directory, where the processed demography data is stored
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
        configs = QuestionConfig.load_config_items(dir_config=dir_config)
        with ThreadPool(threads) as pool:
            _ = list(
                tqdm.tqdm(
                    pool.imap(
                        lambda config: summarize_config(
                            config=config, dir_in=dir_demography_data, dir_out=dir_out
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
