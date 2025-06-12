import os
import shutil
from glob import glob

import defopt
import numpy as np
import pandas as pd
import seaborn as sns
import statsmodels.api as sm
import tqdm
from matplotlib import pylab as plt
from statsmodels.stats.outliers_influence import variance_inflation_factor

from src.utils import QuestionConfig, html_from_fig


def regression_data(df_demo):
    true_true = df_demo.loc[df_demo.true_true][["age", "wt", "sex"]].dropna()
    true_true["side_effect"] = True
    true_true["exposure"] = 1

    true_false = df_demo.loc[df_demo.true_false][["age", "wt", "sex"]].dropna()
    true_false["side_effect"] = False
    true_false["exposure"] = 1

    false_true = df_demo.loc[df_demo.drug_naive_true][["age", "wt", "sex"]].dropna()
    false_true["exposure"] = 0
    false_true["side_effect"] = 1

    false_false = df_demo.loc[df_demo.drug_naive_false][["age", "wt", "sex"]].dropna()
    false_false["exposure"] = 0
    false_false["side_effect"] = 0

    df_regression = pd.concat(
        [true_true, true_false, false_true, false_false], sort=False
    ).reset_index(drop=True)
    df_regression["is_female"] = df_regression["sex"] == "F"
    df_regression.drop("sex", axis=1, inplace=True)
    df_regression["intercept"] = 1.0
    regression_cols = [c for c in df_regression.columns if c != "side_effect"]
    df_regression.side_effect = df_regression.side_effect.astype(int)
    df_regression.is_female = df_regression.is_female.astype(int)
    return df_regression, regression_cols, "side_effect"


def colinearity_analysis(df_regression, regression_cols, name=None):
    rows = []
    if name:
        row = f"{name}: "
    else:
        row = ""
    rows.append("<b> " + row + "variance inflation factors" + "</b>")
    rows.append("<table><tbody>")
    rows.append(
        """<tr>
			<th>variable</th>
			<th>VIF</th>
		</tr>
    """
    )

    mat = df_regression[regression_cols].values
    for i in range(len(regression_cols)):
        colname = regression_cols[i]
        if colname == "intercept":
            continue
        vif = variance_inflation_factor(mat, i)
        rows.append(f"<tr><td>{colname:30s}</td><td>{vif:.3f}</td></tr>")
    rows.append("</tbody></table>")
    return "\n".join(rows)


def regression_data_summary(df_regression, title=None):
    crosstab = pd.crosstab(
        df_regression["exposure"].astype(bool),
        df_regression["side_effect"].astype(bool),
    )
    html_crosstab = crosstab.to_html(
        formatters=[lambda v: f"{v:,d}" for _ in range(crosstab.shape[1])]
    )
    if title is None:
        title = ""
    ret = "<br><h2> crosstabulation %s</h2>\n" % title
    ret += html_crosstab + "<br>\n"
    return ret


from statsmodels.nonparametric.kde import KDEUnivariate


def plot_kde(data, ax=None, keep_x_axis=True, xlim=None, title=None):
    if ax is None:
        fig, ax = plt.subplots()
    if len(data) > 10000:
        data = data.sample(10000)
    if len(data) > 100:
        kde = KDEUnivariate(data)
        kde.fit()
        x = np.linspace(data.min(), data.max(), 1000)
        y = kde.evaluate(x)
        ax.fill_between(x, y, color="k", alpha=0.1)
        ax.plot(x, y, "-", color="k")
    else:
        counts = pd.value_counts(data).sort_index()
        ax.bar(counts.index, counts, color="k", width=0.1)
    sns.despine(ax=ax)
    ax.set_yticks([])

    if xlim is not None:
        ax.set_xlim(xlim)
    if not keep_x_axis:
        sns.despine(ax=ax, bottom=True)
        ax.set_xticks([])
    else:
        xticks = ax.get_xticks()
        ax.set_xticks([xticks[0], xticks[-1]])
    if title is not None:
        ax.set_title(title, x=0.01, y=1, ha="left", va="top", ma="left")
    return ax


def graph_summary_of_regression_data(df_regression):
    html_figures = []
    for variable in ["age", "wt"]:
        if variable == "wt":
            variable_name = "Weight"
        elif variable == "age":
            variable_name = "Age"
        else:
            raise RuntimeError()
        fig, axes = plt.subplots(2, 2)
        for side_effect, row_axes in zip([0, 1], axes):
            for exposure, ax in zip([0, 1], row_axes):
                data = df_regression.loc[
                    (df_regression.exposure == exposure)
                    & (df_regression.side_effect == side_effect)
                ]
                #             if len(data) > 10000:
                #                 data = data.sample(10000)
                data = data[variable]
                if exposure:
                    ttl_exp = "Exposed"
                else:
                    ttl_exp = "Not exposed"
                if side_effect:
                    ttl_se = "with side effect"
                    keep_x_axis = True
                    xlabel = variable_name
                else:
                    ttl_se = "no side effect"
                    keep_x_axis = False
                    xlabel = None
                if not data.empty:
                    the_range = f"{min(data):.1f}-{max(data):.1f}"
                else:
                    the_range = "----"
                ttl = f"{ttl_exp}; {ttl_se}\nN={len(data):,d}; range: {the_range}"
                if variable == "age":
                    mx = 100
                else:
                    mx = 250
                plot_kde(data, ax=ax, title=ttl, xlim=(0, mx), keep_x_axis=keep_x_axis)
                if xlabel:
                    ax.set_xlabel(xlabel)
        html_figures.append(html_from_fig(fig))
        plt.close(fig)

    return "\n<br>\n".join(html_figures)


def filter_regression_table(df_regression, percentile=99):
    df_regression = df_regression.loc[
        ((df_regression.age > 0) & (df_regression.age < 120))
        & ((df_regression.wt > 0) & df_regression.wt < 320)
    ]

    remaining = 100 - percentile
    the_values = df_regression.loc[df_regression.exposure.astype(bool)].wt
    if the_values.empty:
        return df_regression.head(0)  # keep the column info, just in case

    lower, upper = np.nanpercentile(the_values, [remaining / 2, (100 - remaining / 2)])
    sel_wt = (df_regression.wt >= lower) & (df_regression.wt <= upper)
    the_values = df_regression.loc[df_regression.exposure.astype(bool)].age
    if the_values.empty:
        return df_regression.head(0)  # keep the column info, just in case

    lower, upper = np.nanpercentile(the_values, [remaining / 2, (100 - remaining / 2)])
    sel_age = (df_regression.age >= lower) & (df_regression.age <= upper)
    sel = sel_wt & sel_age
    return df_regression.loc[sel]


def regression(df_demo, name):
    df_regression, regression_cols, column_y = regression_data(df_demo)

    summary_before = regression_data_summary(df_regression, title="before filtering")
    df_regression = filter_regression_table(df_regression, percentile=99)
    summary_after = regression_data_summary(df_regression, title="after filtering")
    summary_after = (
        summary_after
        + "<br>"
        + graph_summary_of_regression_data(df_regression)
        + "<br>"
    )

    if df_regression.empty:
        html_summary = "<h1>" + name + "</h1>\n EMPTY TABLE<br>"
    else:
        logit = sm.Logit(df_regression[column_y], df_regression[regression_cols])
        try:
            result = logit.fit()
        except np.linalg.linalg.LinAlgError:
            html_summary = (
                "<h1>" + name + "</h1>\n ERROR<br>" + summary_before + summary_after
            )
        else:
            html_summary = (
                "<h1>"
                + name
                + "</h1>\n"
                + summary_before
                + summary_after
                + result.summary(title=name).as_html()
                + "\n<br>\n"
                + colinearity_analysis(
                    df_regression=df_regression,
                    regression_cols=regression_cols,
                    name=None,
                )
            )

    return html_summary


def summarize_config(config, dir_in, dir_out):
    DEBUG = None
    dir_demo_data = config.filename_from_config(dir_in, extension="")
    files = glob(os.path.join(dir_demo_data, "*.csv.zip"))
    df_demo = pd.concat([pd.read_csv(f, nrows=DEBUG) for f in files])
    rows = []
    for label in ["true_true", "true_false", "drug_naive_true", "drug_naive_false"]:
        for variable in ["age", "wt"]:
            for sex in ["M", "F", None]:
                sel = df_demo[label]
                if sex is not None:
                    sel = sel & (df_demo["sex"] == sex)
                if not sel.any():
                    row = {
                        "label": label,
                        "variable": variable,
                        "sex": sex if sex else "all",
                        "n": 0,
                        "n_valid": 0,
                    }
                    rows.append(row)
                    continue
                values = df_demo.loc[sel][variable]
                n = len(values)
                n_missing = pd.isna(values).sum()
                values = values.dropna()
                cutoff_min = 0
                if variable == "age":
                    cutoff_max = 120
                else:
                    cutoff_max = 320
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
                    "label": label,
                    "variable": variable,
                    "sex": sex if sex else "all",
                    "n": n,
                    "n_missing": n_missing,
                    "n_valid": n_valid,
                    "mean": mean_,
                    "median": median_,
                    "std": std_,
                    "kde": (x.tolist(), kde_values.tolist()),
                }
                rows.append(row)
    processed = pd.DataFrame(rows)
    processed = processed[  # give a nice order
        [
            "label",
            "variable",
            "sex",
            "n",
            "n_missing",
            "n_valid",
            "mean",
            "median",
            "std",
            "kde",
        ]
    ]
    fn_out = config.filename_from_config(dir_out, extension=".csv")
    processed.to_csv(fn_out, index=False)

    html_regression = regression(df_demo, name=config.name)
    fn_out = config.filename_from_config(dir_out, extension=".html")
    open(fn_out, "w").write(html_regression)


def main(*, dir_demography_data, dir_config, dir_out, clean_on_failure=False):
    """

    :param str dir_config:
        Input directory, where config files are stored
    :param str dir_demography_data:
        Input directory, where the processed demography data is stored
    :param str dir_out:
        Output directory
    :param bool clean_on_failure:
        ???
    :return: None

    """

    dir_out = os.path.abspath(dir_out)
    os.makedirs(dir_out, exist_ok=True)
    try:
        configs = QuestionConfig.load_config_items(dir_config=dir_config)
        for config in tqdm.tqdm(configs):
            summarize_config(config=config, dir_in=dir_demography_data, dir_out=dir_out)

    except Exception as err:
        if clean_on_failure:
            shutil.rmtree(dir_out)
        raise err


if __name__ == "__main__":
    defopt.run(main)
