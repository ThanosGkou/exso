import datetime
import logging
from pathlib import Path

import pandas as pd
from exso.DataBase.Status import Status
from exso.DataBase.Update import Update
from exso.IO.IO import IO
from exso.IO.Nodes import Node, DNA
from exso.IO.Tree import Tree
from exso.Utils.DateTime import DateTime
from exso.Utils.STR import STR

# *********************************************
date_lambda = lambda x: datetime.datetime.strftime(DateTime.date_magician(x, return_stamp = False), format="%d-%b-%y")
# *********************************************


# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
class Query:

    # *******  *******   *******   *******   *******   *******   *******
    def query(self, locator: None| str | Path | DNA | Node = None, start_date=None, end_date=None) -> pd.DataFrame | dict:

        node = self.tree.get_node(locator)
        df = node(is_multiindex = self.is_multiindex)
        if start_date:
            df = df.loc[start_date:]
        if end_date:
            df = df.loc[:end_date]
        return df.copy()

    # *******  *******   *******   *******   *******   *******   *******
    def combine(self, *locators):
        pass

    # *******  *******   *******   *******   *******   *******   *******
    def search(self, *fuzzy_string, n_best = 10, kind = 'property', ):
        if not self.tree.dna_is_initialized:
            self.tree.get_dna_chains()

        suggestion = self.tree.search(*fuzzy_string, n_best=n_best, kind=kind)
        return suggestion

###############################################################################################
###############################################################################################
###############################################################################################
class DataBase(Query, Update):
    def __init__(self, report,  db_timezone, db_suffix = None):
        ''' Utilized attributes of report object:
            .persistent_long
            .database_path
            .database_min_potential_datetime
            .database_max_potential.datetime
            .inherent_Tz
            # .period_covered
            .resolution
        '''

        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

        self.r = report
        self.is_multiindex = self.r.persistent_long

        self.dir = self.r.database_path
        if db_suffix:
            self.dir = self.dir.parent / (self.dir.name + db_suffix)

        self.timezone = db_timezone

        args_needed = {'dir': self.r.database_path,
                       'db_timezone':db_timezone,
                       'min_potential_datetime': self.r.database_min_potential_datetime,
                       'max_potential_datetime': self.r.database_max_potential_datetime,
                       'lake_inherent_tz':self.r.inherent_tz,
                       # 'period_covered': self.r.period_covered,
                       'resolution':self.r.resolution,}


        self.status = Status(**args_needed)
        self.status.refresh(self.dir)


        if self.status.exists:
            # this is robust, because it includes possible alterations to structure
            self.tree = Tree(root_path=self.dir, depth_mapping={0: 'report', 1: 'field', 2: 'file', 3: 'property'})
            # self.tree.make_tree()
        else:
            # this is a mostly valid thing, but sometimes, new fields are created (e.g. Market Schedule)
            # so, in a freshly built database, it requires feedback from the actual parsing
            root_dict = self.resemble_dict(self.r.cue_summary)
            self.tree = Tree(root_path = self.dir, root_dict=root_dict, depth_mapping={0: 'report', 1: 'field', 2: 'file', 3: 'property'})
            # self.tree.make_tree(from_dict = self.r.cue_summary)

        self.tree.make_dna_chains()

        # self.tree.visualize()
        # self.tree.make_dirs()

        # sugg = self.search("PROTERGIA", n_best=10,)
        # df = self.query(locator = sugg.dna[0])

    # *******  *******   *******   *******   *******   *******   *******
    def resemble_dict(self, cue_dict):
        new_dict = {}
        for field, subfields_list in cue_dict.items():
            new_dict[field] = {}
            for subfield in subfields_list:
                new_dict[field][subfield] = {}
        return new_dict




# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
class Diagnostic:
    def __init__(self):
        pass
    @staticmethod
    def make_file_report(filepath, timezone_as_read = None, convert_to_timezone = None, print_report = False):
        report = {}
        df = IO.read_file(filepath, timezone_as_read=timezone_as_read, convert_to_timezone=convert_to_timezone)

        start = df.index[0]
        end = df.index[-1]
        resol = df.index[1] - df.index[0]
        head = df.head().copy()
        tail = df.tail().copy()
        descr = df.describe()
        n_rows, n_cols = df.shape
        cols = df.columns.to_list()

        nans = df.isna().sum()

        df = df.sort_index()
        unsorted = False
        if df.index[0] != start or df.index[-1] != end:
            unsorted = False
            start = df.index[0]
            end = df.index[-1]

        duplicated = df[df.index.duplicated(keep=False)].copy()
        n_duplications = duplicated.shape[0]

        df = df.reset_index(drop = False).rename({'index':'datetime'}, axis = 1)
        df['diff'] = df['datetime'].diff()
        after_leaps = df[df['diff'] != resol].index[1:]
        df.pop('diff')

        df.set_index('datetime', inplace = True, drop=True)
        before_leaps = after_leaps - 1

        before_after = pd.DataFrame()
        if after_leaps.empty:
            has_missing = False
        else:
            has_missing = True
            before_after['before'] = df.loc[before_leaps, 'datetime']
            before_after['after'] = df.loc[after_leaps, 'datetime'].values

        ideal_dates = pd.date_range(start, end, freq=resol)
        total_records_missing = len(ideal_dates) - n_rows

        report['start'] = start
        report['end'] = end
        report['n_rows'] = n_rows
        report['n_cols'] = n_cols
        report['columns'] = cols
        report['head'] = head
        report['tail'] = tail
        report['description'] = descr
        report['was_unsorted']  = unsorted
        report['n_duplicates'] = n_duplications
        report['duplication_dataframe'] = duplicated
        report['nan_count'] = nans
        report['has_missing_records'] = has_missing
        report['n_missing_records']  =total_records_missing
        report['boundaries_of_missing'] = before_after
        if not print_report:
            return report
        for k, v in report.items():
            print('*' * 50)
            print(k)
            if isinstance(v, pd.DataFrame):
                STR.df_to_string(v, indent=3)
            else:
                print(v)
            print()


        # import missingno as msno
        # import matplotlib.pyplot as plt
        # msno.matrix(df,fontsize=10,labels=True,freq='Y')
        # plt.show()
        # input('--- END of integrity report')

        return report

