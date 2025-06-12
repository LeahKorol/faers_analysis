import os
import shutil
import warnings

import defopt
import numpy as np
import pandas as pd
import seaborn as sns
import tqdm
from matplotlib import pylab as plt

from src.utils import ContingencyMatrix, QuestionConfig


def plot_incidence(tbl_report, ax=None, figwidth=8, dpi=300):
    if ax is None:
        figsize = (figwidth, figwidth / 1.618)
        fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
    quarters = list(sorted(tbl_report.q.unique()))  # we assume no Q is missing
    quarters
    x = list(range(len(quarters)))
    reports = tbl_report.True_True + tbl_report.True_False
    reports = reports.diff()
    tbl_report["reports"] = reports
    ax.plot(x, reports, "o-")

    tkx = []
    lbls = []
    for i, q in enumerate(quarters):
        if q.endswith("1"):
            tkx.append(i)
            lbls.append(q.split("q")[0])

    ax.set_xticks(tkx)
    ax.set_xticklabels(lbls)
    sns.despine(ax=ax)
    ax.set_ylabel("Reported events", rotation=0, ha="right", y=0.9)
    ax.set_ylim(0, ax.get_ylim()[1])
    yticks = ax.get_yticks()
    lix_middle = int(len(yticks) / 2)
    yticks = [yticks[0], yticks[lix_middle], yticks[-1]]
    ax.set_yticks(yticks)
    return ax


def plot_ror(tbl_report, ax_ror=None, xticklabels=True, figwidth=8, dpi=300):
    figsize = (figwidth, figwidth / 1.618)
    if ax_ror is None:
        fig_ror, ax_ror = plt.subplots(figsize=figsize, dpi=dpi)
    quarters = list(sorted(tbl_report.q.unique()))  # we assume no Q is missing
    x = list(range(len(quarters)))

    ax_ror.plot(x, tbl_report.l10_ROR, "-o", color="C0", zorder=99)
    ax_ror.fill_between(
        x, tbl_report.l10_ROR_lower, tbl_report.l10_ROR_upper, color="C0", alpha=0.3
    )
    ax_ror.set_ylim(-2.1, 2.1)
    tkx = [-1, 0, 1]
    ax_ror.set_yticks(tkx)
    ax_ror.set_yticklabels([f"$\\times {10**t}$" for t in tkx])
    sns.despine(ax=ax_ror)
    ax_ror.spines["bottom"].set_position("zero")
    tkx = []
    lbls = []
    for i, q in enumerate(quarters):
        if q.endswith("1"):
            tkx.append(i)
            lbls.append(q.split("q")[0])

    ax_ror.set_xticks(tkx)
    if xticklabels:
        ax_ror.set_xticklabels(lbls)
    else:
        ax_ror.set_xticklabels([])
    ax_ror.text(
        x=max(x) + 0.15,
        y=tbl_report.l10_ROR.iloc[-1],
        s=f"${tbl_report.ROR.iloc[-1]:.2f}$",
        ha="left",
        va="center",
        color="gray",
    )

    ax_ror.text(
        x=max(x) + 0.1,
        y=tbl_report.l10_ROR_lower.iloc[-1],
        s=f"${tbl_report.ROR_lower.iloc[-1]:.2f}$",
        ha="left",
        va="top",
        size="small",
        color="gray",
    )

    ax_ror.text(
        x=max(x) + 0.1,
        y=tbl_report.l10_ROR_upper.iloc[-1],
        s=f"${tbl_report.ROR_upper.iloc[-1]:.2f}$",
        ha="left",
        va="bottom",
        size="small",
        color="gray",
    )
    ax_ror.set_ylabel("ROR", rotation=0, ha="right", y=0.9)
    ax_ror.set_xlim(0, max(x) + 1)
    return ax_ror


def generate_individual_figure(tbl_report):
    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(8, 8 / 3 * 2), dpi=300)
    plot_ror(tbl_report, axes[0], xticklabels=False)
    plot_incidence(tbl_report, ax=axes[1])
    fig.tight_layout()
    return fig


def save_fig(fig, dir_out, n, name, formats):
    for format in formats:
        dirname = os.path.join(dir_out, format)
        os.makedirs(dirname, exist_ok=True)
        if name:
            name = "_" + str(name)
        fn = os.path.join(dirname, f"figure_{n:03d}{name}.{format}")
        fig.savefig(fn, facecolor=fig.get_facecolor())
        plt.close(fig)


def summary_table(contingency_matrices, alpha, smoothing):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        gr = contingency_matrices.groupby("q", sort=True)
        tbl_report = []
        previous = None
        for q, t in gr:
            matrix = ContingencyMatrix(t)
            if previous is not None:
                matrix = matrix + previous
            previous = matrix
            ror = matrix.ror(alpha=alpha, smoothing=smoothing)
            curr = {
                "q": q,
                "ROR": ror[0],
                "ROR_lower": ror[1][0],
                "ROR_upper": ror[1][1],
            }
            for n, label in zip(
                matrix.ror_components(),
                ["True_True", "True_False", "False_True", "False_False"],
            ):
                curr[label] = n
            tbl_report.append(curr)
        tbl_report = pd.DataFrame.from_records(tbl_report).sort_values("q")
        tbl_report["l10_ROR"] = np.log10(tbl_report.ROR)
        tbl_report["l10_ROR_upper"] = np.log10(tbl_report.ROR_upper)
        tbl_report["l10_ROR_lower"] = np.log10(tbl_report.ROR_lower)
    return tbl_report


def generate_individual_report(
    config, fn_in, dir_reports, alpha, smoothing, title_in_figure=True
):
    # load the data
    df_contingency = pd.read_csv(fn_in)
    df_summary = summary_table(
        contingency_matrices=df_contingency, alpha=alpha, smoothing=smoothing
    )
    fig = generate_individual_figure(df_summary)
    if smoothing:
        name = config.name + f" Smoothing {smoothing}"
    else:
        name = config.name
    if title_in_figure:
        fig.suptitle(name)
    save_fig(fig=fig, dir_out=dir_reports, n=1, name=name, formats=["png", "tiff"])
    return df_summary


def main(
    *,
    dir_contingency,
    config_dir,
    dir_reports,
    alpha=0.05,
    smoothing=0,
    clean_on_failure=False,
    title_in_figure=True,
):
    """

    :param str dir_contingency:
        Input directory, where contingency matrices are stored
    :param str config_dir:
        Directory with config files
    :param str dir_reports:
        Output directory
    :param float alpha:
        Confidence interval alpha
    :param float smoothing:
        Smoothing
    :param bool clean_on_failure:
        ???
    :param bool title_in_figure:
        Should the figures contain titles?

    :return: None

    """

    dir_reports = os.path.abspath(dir_reports)
    os.makedirs(dir_reports, exist_ok=True)
    try:
        config_items = QuestionConfig.load_config_items(config_dir)
        print(
            f"Will analyze {len(config_items)} configurations: "
            + ", ".join([c.name for c in config_items])
        )
        results = dict()
        for config in tqdm.tqdm(config_items):
            fn_in = config.filename_from_config(dir_contingency)
            df_summary_curr = generate_individual_report(
                config=config,
                fn_in=fn_in,
                dir_reports=dir_reports,
                alpha=alpha,
                smoothing=smoothing,
                title_in_figure=title_in_figure,
            )
            df_summary_curr["config"] = config.name
            results[config.name] = df_summary_curr
        columns = ["q", "config", "ROR_lower", "ROR", "ROR_upper"]
        final_report = (
            pd.DataFrame(
                [
                    tbl.sort_values("q").iloc[-1][columns]
                    for _, tbl in sorted(results.items())
                ]
            )
            .sort_values("ROR")
            .reset_index(drop=True)
        )
        fn_out = os.path.join(dir_reports, f"report_smoothing{smoothing}.csv")
        final_report.to_csv(fn_out, index=False)
        fig, ax = plt.subplots(dpi=240)
        y = list(range(len(final_report)))
        for i, row in final_report.iterrows():
            ax.plot([row.ROR_lower, row.ROR_upper], [i, i], "-k")
        ax.plot(final_report.ROR, y, "ok")
        ax.set_yticks(y)
        ax.set_yticklabels(final_report.config)
        ax.axvline(1.0, ls="--", color="k")
        ax.set_xscale("log")
        sns.despine(ax=ax)
        fig.tight_layout()
        fig.savefig(os.path.join(dir_reports, f"report_smoothing{smoothing}.png"))
        plt.close(fig)

    except Exception as err:
        if clean_on_failure:
            shutil.rmtree(dir_reports)
        raise err


if __name__ == "__main__":
    import sys

    sys.argv = "src/generate_reports.py --dir-contingency=/Users/boris/devel/faers/data/interim/contingency --config-dir=/Users/boris/devel/faers/config --dir-reports=/Users/boris/devel/faers/data/processed/reports --smoothing 0".split()
    defopt.run(main)
