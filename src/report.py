# We will use a class instead of a set of functions, mainly for figure management
import pickle
from glob import glob

import defopt
import numpy as np
import pandas as pd
import seaborn as sns
import statsmodels.api as sm
import tqdm
from gevent import os
from matplotlib import pylab as plt
from statsmodels.stats.outliers_influence import variance_inflation_factor

from src.utils import html_from_fig, ContingencyMatrix, QuestionConfig


class Reporter:
    FORMATS = ['png']

    def __init__(self, config, dir_out, dir_raw_data):
        self.config = config
        self.title = config.name
        self.dir_out = os.path.join(dir_out, self.title)
        self.dir_raw_data = dir_raw_data
        for format_ in self.FORMATS:
            os.makedirs(
                os.path.join(
                    self.dir_out, format_
                ), exist_ok=True)
        self.figure_count = 0

    def subplots(self, nrows=1, ncols=1, figsize=(8, 6), dpi=360):
        return plt.subplots(nrows=nrows, ncols=ncols, figsize=figsize, dpi=dpi)

    def handle_fig(self, fig, caption=None):
        self.figure_count += 1
        if caption is None:
            caption = ''
        else:
            caption = ': ' + caption
        caption = f'Figure {self.figure_count}{caption}'
        html = html_from_fig(fig, caption=caption, width=600)
        for format_ in self.FORMATS:
            fn = os.path.join(self.dir_out, format_, f'figure_{self.figure_count:03d}.{format_}')
            fig.savefig(fn, dpi=360)
        return html

    def report(self, data, title, config, explanation=None, skip_lr=False):
        lines = []
        lines.append(f'<H1>{self.title}</H1>')
        if explanation is not None:
            lines.append(explanation)
        data = self.handle_controls(data, config)
        summary = self.summarize_data(data, title, config, skip_lr=skip_lr)
        lines.append(summary)
        fn = os.path.join(self.dir_out, f'report {self.config.name} {title}.html')
        open(fn, 'w').write('\n'.join(lines))
        print(f'Saved {fn}')

    def handle_controls(self, data, config):
        if config.control is None:
            return data
        control_col = f'control {config.name}'
        col_exposure = f'drug {config.name}'
        sel = data[control_col] | data[col_exposure]
        print(f'{config.name:40s}: Due to control handling, removing {(1-sel.mean())*100:.1f}% of lines')
        return data.loc[sel]

    def summarize_data(self, data, title, config, skip_lr=False):
        lines = ['<H2>%s</H2>' % title]
        lines.append(self.demographic_summary(data))
        lines.append(self.ror_dynamics(data))
        if not skip_lr:
            lines.append(self.regression_analysis(data))
        lines.append(self.true_true(data, config))

        return '\n'.join(lines)

    def true_true(self, data, config):
        lines = ['<h3> True-True cases </h3>']
        col_exposure = f'drug {config.name}'
        col_ouctome = f'reacted {config.name}'
        true_true_data = data.loc[
            data[col_exposure] & data[col_ouctome]
            ][['age', 'wt', 'sex', 'event_date', 'q']].reset_index()
        for c in ['age', 'wt']:
            true_true_data[c] = np.round(true_true_data[c], 1)
        true_true_data.index += 1
        lines.append(true_true_data.to_html())

        lines.append('<h3>True-False cases</h3>')
        true_false_data = data.loc[
            data[col_exposure] & (~data[col_ouctome])
            ][['age', 'wt', 'sex', 'event_date', 'q']].reset_index()
        for c in ['age', 'wt']:
            true_false_data[c] = np.round(true_false_data[c], 1)
        true_false_data.index += 1
        lines.append(true_false_data.to_html())

        return '\n'.join(lines)

    def regression_analysis(self, data_regression):
        config = self.config
        data_regression = data_regression.copy()
        col_exposure = f'drug {config.name}'
        col_ouctome = f'reacted {config.name}'

        data_regression['exposure'] = data_regression[col_exposure].astype(int)
        data_regression['outcome'] = data_regression[col_ouctome].astype(int)
        data_regression['is_female'] = (data_regression['sex'] == 'F').astype(int)
        data_regression['intercept'] = 1.0

        regression_cols = ['age', 'is_female', 'wt', 'exposure', 'intercept']
        outcome_col = 'outcome'
        data_regression = data_regression[regression_cols + [outcome_col]]
        logit = sm.Logit(data_regression[outcome_col], data_regression[regression_cols])
        try:
            result = logit.fit()
        except:
            result_summary = 'ERROR. Most probably, singular matrix<br>'
            or_estimates = '<br>'
        else:
            result_summary = result.summary2(title=config.name).as_html()
            or_estimates = result.conf_int().rename(columns={0: 'lower', 1: 'upper'})
            or_estimates['OR'] = result.params
            or_estimates = np.round(np.exp(or_estimates)[['lower', 'OR', 'upper']], 3)
            or_estimates = 'OR estimates<br>' + or_estimates.to_html()

        html_summary = '<h3>Logistic regression</h3>\n' \
                       + result_summary \
                       + '\n<br>\n' \
                       + or_estimates \
                       + self.colinearity_analysis(data_regression=data_regression, regression_cols=regression_cols,
                                                   name=None)
        return html_summary

    @staticmethod
    def colinearity_analysis(data_regression, regression_cols, name=None):
        rows = []
        if name:
            row = f'{name}: '
        else:
            row = ''
        rows.append('<b> ' + row + 'variance inflation factors' + '</b>')
        rows.append('<table><tbody>')
        rows.append('''<tr>
    			<th>variable</th>
    			<th>VIF</th>
    		</tr>
        ''')

        mat = data_regression[regression_cols].values
        for i in range(len(regression_cols)):
            colname = regression_cols[i]
            if colname == 'intercept':
                continue
            vif = variance_inflation_factor(mat, i)
            rows.append(f'<tr><td>{colname:30s}</td><td>{vif:.3f}</td></tr>')
        rows.append('</tbody></table>')
        return '\n'.join(rows)

    def demographic_summary(self, data):
        lines = []
        lines.append('<H3>Demographic data</H3>')
        lines.append(self.demographic_table(data))
        lines.append(self.contingency_table(data))
        return '\n'.join(lines)

    def contingency_table(self, data):
        lines = []
        lines.append('<h4>Contingency-matrix</h4>')
        cm = ContingencyMatrix.from_results_table(data, self.config)
        lines.append(cm.crosstab().to_html())
        return '\n'.join(lines)

    def count_serious_outcomes(self, outcome_cases):
        # We assume that if a case ID is listed in `outcome*.csv.zip` it is
        # a "serious" outcome
        outcome_files = glob(os.path.join(self.dir_raw_data, 'outc*.csv.zip'))
        serious_outcomes = set()
        for f in outcome_files:
            serious_outcomes.update(pd.read_csv(f, usecols=['caseid'], dtype=str).caseid.values)
        n_serious = np.sum([c in serious_outcomes for c in outcome_cases])
        return n_serious

    def demographic_table(self, data):
        config = self.config
        col_exposure = f'drug {config.name}'
        col_ouctome = f'reacted {config.name}'
        table_rows = []
        for exposure in 'all', True, False:
            if exposure == 'all':
                data_exposure = data
            else:
                data_exposure = data.loc[data[col_exposure] == exposure]
            for outcome in 'all', True, False:
                if (exposure == 'all') and (outcome != 'all'):
                    continue
                if outcome == 'all':
                    data_outcome = data_exposure
                else:
                    data_outcome = data_exposure.loc[data_exposure[col_ouctome] == outcome]
                for gender in ['all', 'F', 'M']:
                    if gender == 'all':
                        data_gender = data_outcome
                    else:
                        data_gender = data_outcome.loc[data_outcome.sex == gender]

                    data_row = data_gender
                    n = f'{len(data_row):,d}'
                    age_mean = data_row.age.mean()
                    age_std = data_row.age.std(ddof=1)
                    age_range = f'{data_row.age.min():.1f} - {data_row.age.max():.1f}'
                    weight_mean = data_row.wt.mean()
                    weight_std = data_row.wt.std(ddof=1)
                    weight_range = f'{data_row.wt.min():.1f} - {data_row.wt.max():.1f}'
                    if gender == 'all':
                        percent_female = 100 * (data_row.sex == 'F').mean()
                        percent_male = 100 * (data_row.sex == 'M').mean()
                        female_to_male = f'{percent_female:.1f} : {percent_male:.1f}'
                    else:
                        female_to_male = ''
                    table_rows.append([
                        str(exposure), str(outcome), gender,
                        n,
                        f'{age_mean:.1f}({age_std:.1f})',
                        age_range,
                        f'{weight_mean:.1f}({weight_std:.1f})',
                        weight_range,
                        female_to_male
                    ])
                table_rows.append(['--'] * len(table_rows[-1]))
        summary_table = pd.DataFrame(
            table_rows,
            columns=['Exposure', 'Outcome', 'Gender', 'N', 'Age', 'Age range', 'Weight', 'Weight range',
                     'Female : Male'
                     ]
        )
        html_table = summary_table.to_html(index=False)
        additional_rows = []
        cases_with_outcome = set(data.loc[data[col_ouctome]].index)
        n_serious = self.count_serious_outcomes(cases_with_outcome)
        p_serious = 100 * n_serious / len(cases_with_outcome)
        additional_rows.append(
            f'Of {len(data):,d} cases, {len(cases_with_outcome):,d} had a reaction. Of them {n_serious:,d} ({p_serious:.1f}%) were serious ones')

        ret = '<h4>Demographic summary</h4>\n' + html_table + '<br>\n'.join(additional_rows)

        return ret

    def ror_dynamics(self, data):
        lines = []
        lines.append('<H3>ROR data</H3>')
        config = self.config
        gr = data.groupby('q')
        rors = []
        ror_data = pd.DataFrame()
        for q, curr in sorted(gr):
            curr['q'] = q
            columns_to_keep = ['age', 'sex', 'wt', 'event_date', 'q'] + [
                c for c in curr.columns if c.endswith(config.name)
            ]
            ror_data = pd.concat((ror_data, curr[columns_to_keep]))
            contingency_matrix = ContingencyMatrix.from_results_table(ror_data, config)
            ror, (lower, upper) = contingency_matrix.ror()
            rors.append([q, lower, ror, upper])
        df_rors = pd.DataFrame(rors, columns=['q', 'ROR_lower', 'ROR', 'ROR_upper'])
        fig, ax = self.subplots()
        self.plot_ror(df_rors, ax_ror=ax)
        lines.append(self.handle_fig(fig, 'ROR dynamics'))
        return '\n'.join(lines)

    @staticmethod
    def plot_ror(tbl_report, ax_ror=None, xticklabels=True, figwidth=8, dpi=360):
        if ax_ror is None:
            figsize = (figwidth, figwidth * 0.5)
            fig_ror, ax_ror = plt.subplots(figsize=figsize, dpi=dpi)
        quarters = list(sorted(tbl_report.q.unique()))  # we assume no Q is missing
        x = list(range(len(quarters)))
        tbl_report['l10_ROR'] = np.log10(tbl_report.ROR)
        tbl_report['l10_ROR_lower'] = np.log10(tbl_report.ROR_lower)
        tbl_report['l10_ROR_upper'] = np.log10(tbl_report.ROR_upper)

        ax_ror.plot(x, tbl_report.l10_ROR, '-o', color='C0', zorder=99)
        ax_ror.fill_between(x, tbl_report.l10_ROR_lower, tbl_report.l10_ROR_upper, color='C0', alpha=0.3)
        ax_ror.set_ylim(-2.1, 2.1)
        tkx = [-1, 0, 1]
        ax_ror.set_yticks(tkx)
        ax_ror.set_yticklabels([f'$\\times {10**t}$' for t in tkx])
        sns.despine(ax=ax_ror)
        ax_ror.spines['bottom'].set_position('zero')
        tkx = []
        lbls = []
        for i, q in enumerate(quarters):
            if q.endswith('1'):
                tkx.append(i)
                lbls.append(q.split('q')[0])

        ax_ror.set_xticks(tkx)
        if xticklabels:
            ax_ror.set_xticklabels(lbls)
        else:
            ax_ror.set_xticklabels([])
        ax_ror.text(
            x=max(x) + 0.15,
            y=tbl_report.l10_ROR.iloc[-1],
            s=f'${tbl_report.ROR.iloc[-1]:.2f}$',
            ha='left', va='center', color='gray'
        )

        ax_ror.text(
            x=max(x) + 0.1,
            y=tbl_report.l10_ROR_lower.iloc[-1],
            s=f'${tbl_report.ROR_lower.iloc[-1]:.2f}$',
            ha='left', va='top', size='small', color='gray'
        )

        ax_ror.text(
            x=max(x) + 0.1,
            y=tbl_report.l10_ROR_upper.iloc[-1],
            s=f'${tbl_report.ROR_upper.iloc[-1]:.2f}$',
            ha='left', va='bottom', size='small', color='gray'
        )
        ax_ror.set_ylabel('ROR', rotation=0, ha='right', y=0.9)
        ax_ror.set_xlim(0, max(x) + 1)
        return ax_ror


def filter_illegal_values(data):
    sel = (
                  (data.wt > 0) & (data.wt < 360)
          ) & (
                  (data.age > 0) & (data.age < 120)
          )
    return data.loc[sel]


def filter_data_for_regression(data, config):
    percentile_ = 99.0
    percentile_lower = (100 - percentile_) / 2
    percentile_upper = 100 - percentile_lower

    col_exposure = f'drug {config.name}'
    data_exposed = data.loc[data[col_exposure]]
    age_from, age_to = np.percentile(data_exposed.age, [percentile_lower, percentile_upper])
    weight_from, weight_to = np.percentile(data_exposed.wt, [percentile_lower, percentile_upper])
    sel = (
                  (data.wt > weight_from) & (data.wt < weight_to)
          ) & (
                  (data.age > age_from) & (data.age < age_to)
          )
    assert sel.any()
    return data.loc[sel]


def main(
        *,
        dir_marked_data,
        dir_raw_data,
        config_dir,
        dir_reports
):
    """

    Generate reports

    :param str dir_marked_data:
        marked data directory
    :param str dir_raw_data:
        raw data directory
    :param str config_dir: 
        config directory
    :param str dir_reports:
        output directory
    :return:

    """

    config_items = QuestionConfig.load_config_items(config_dir)
    for config in tqdm.tqdm(config_items):
        files = sorted(glob(os.path.join(dir_marked_data, '*.pkl')))
        data = []
        for f in sorted(files):
            curr = pickle.load(open(f, 'rb'))
            q = os.path.splitext(os.path.split(f)[-1])[0]
            curr['q'] = q
            columns_to_keep = ['age', 'sex', 'wt', 'event_date', 'q'] + [
                c for c in curr.columns if c.endswith(config.name)
            ]
            data.append(curr[columns_to_keep])
        data = pd.concat(data)
        reporter = Reporter(config, dir_reports, dir_raw_data=dir_raw_data)
        reporter.report(data, '01 Initial data', explanation='Raw data', skip_lr=True, config=config)

        data = filter_illegal_values(data)
        reporter.report(data, '02 Filtered', config=config,
                        explanation='After filtering out weight and age values that make no sense')

        data = filter_data_for_regression(data, config)
        reporter.report(data, '03 Stratified for LR', config=config,
                        explanation='After filtering out age and weight values that do not fit 99 percentile of the exposed population')


if __name__ == '__main__':
    defopt.run(main)
