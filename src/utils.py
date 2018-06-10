import json
import os
from collections import namedtuple
from glob import glob

import pandas as pd
import numpy as np
import scipy.stats as stats
import re


class Quarter:
    def __init__(self, *args):
        if len(args) == 2:
            self.year = int(args[0])
            self.quarter = int(args[1])
        else:
            self.year, self.quarter = self.parse_string(args[0])

        self.__verify()

    @staticmethod
    def parse_string(s):
        m = re.search(r'^(\d\d\d\d)-?q(\d)$', s)
        if m is None:
            raise RuntimeError(f'Quarter definition "{s}" does not follow the format YYYYqQ')
        year, quarter = m.groups()
        year = int(year)
        quarter = int(quarter)
        return (year, quarter)

    def __verify(self):
        assert 1900 < self.year < 2100
        assert self.quarter in {1, 2, 3, 4}

    def increment(self):
        year = self.year
        quarter = self.quarter
        quarter += 1
        if quarter > 4:
            quarter = 1
            year += 1
        return Quarter(year, quarter)

    def __eq__(self, other):
        if other is self:
            return True
        return (self.year == other.year) and (self.year == other.year)

    def __lt__(self, other):
        if self.year == other.year:
            return self.quarter < other.quarter
        else:
            return self.year < other.year

    def __hash__(self):
        return hash((self.year, self.quarter))

    def __str__(self):
        return f'{self.year}q{self.quarter}'


def generate_quarters(start, end):
    while start < end:  # NOTE: not including *end*
        yield start
        start = start.increment()


class ContingencyMatrix:
    def __init__(self, tbl=None):
        if tbl is None:
            tbl = pd.DataFrame(columns=['exposure', 'outcome', 'n'])
        else:
            for c in ['exposure', 'outcome', 'n']:
                assert c in tbl.columns
        self.tbl = tbl[['exposure', 'outcome', 'n']]

    @classmethod
    def from_results_table(cls, tbl, column_exposure='is_drug', column_outcome='is_reaction'):
        contingency_table = tbl.groupby(
            [column_exposure, column_outcome]
        ).apply(len).reset_index().rename(
            columns={
                column_exposure: 'exposure',
                column_outcome: 'outcome',
                0: 'n'
            }
        )

        return cls(contingency_table)

    def get_count_value(self, exposure, outcome):
        sel = self.tbl.exposure == exposure
        sel = sel & (self.tbl.outcome == outcome)
        if sel.sum() == 0:
            return 0
        elif sel.sum() == 1:
            return self.tbl.loc[sel]['n'].iloc[0]
        else:
            raise RuntimeError()

    def ror_components(self):
        a = self.get_count_value(True, True)
        b = self.get_count_value(True, False)
        c = self.get_count_value(False, True)
        d = self.get_count_value(False, False)
        assert a + b + c + d == self.tbl.n.sum()
        return (a, b, c, d)

    def ror(self, alpha=0.05):
        # https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2938757/
        a, b, c, d = self.ror_components()
        ror = (a * d) / (b * c)
        if alpha is not None:
            # eq 2 from https://arxiv.org/pdf/1307.1078.pdf
            ln_ror = np.log(ror)
            standard_error_ln_ror = np.sqrt((1 / self.tbl['n']).sum())
            interval = np.multiply(stats.distributions.norm.interval(alpha), standard_error_ln_ror)
            ci_ln_ror = ln_ror + interval
            ci = np.exp(ci_ln_ror)
            return ror, ci
        else:
            return ror

    def __str__(self):
        ret = 'Contingency matrix\n' + str(self.tbl)
        return ret

    def __repr__(self):
        return self.__str__()


QuestionConfig = namedtuple('QuestionConfig', ['name', 'drugs', 'reactions'])


def load_config_items(dir_config):
    ret = []
    for f in glob(os.path.join(dir_config, '*.json')):
        name = os.path.split(f)[-1].replace('.json', '')
        config = json.load(open(f))
        ret.append(QuestionConfig(name, drugs=config['drug'], reactions=config['reaction']))
    return ret


def filename_from_config(config, dir_out, extension='csv'):
    config_name = config.name
    return os.path.join(dir_out, f'{config_name}.{extension}')
