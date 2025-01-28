import datetime
import tqdm
import logging
import traceback
from pathlib import Path

import pandas as pd
import exso
from exso.IO.IO import IO
from exso.IO.Nodes import Node, DNA
from exso.IO.Tree import Tree
from exso.Utils.DateTime import DateTime
from exso.Utils.Misc import Misc
from exso.Utils.STR import STR

# *******  *******   *******   *******   *******   *******   *******
date_lambda = lambda x: datetime.datetime.strftime(DateTime.date_magician(x, return_stamp = False), format="%d-%b-%y")

# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
class Update:
    def __init__(self):
        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

    # *******  *******   *******   *******   *******   *******   *******
    def get_update_requirements(self):
        self.logger.info("Getting update requirements.")
        self.requirements  = {}

        if self.status.up_to_date:
            self.logger.info('\tDatabase is up-to-date. No update requiremnets.')
            return self.requirements

        if self.status.exists:
            start_date = self.status.dates.max.observed.datetime + pd.Timedelta(self.r.resolution)
            end_date = self.status.dates.max.potential.datetime
            if start_date > end_date:
                self.logger.info('\tDatabase is up-to-date. No update requiremnets.')
                return self.requirements

        else:
            if not self.r.requires_tz_handling:
                start_date = self.status.dates.min.potential.date
                end_date = self.status.dates.max.potential.date

            else:
                start_date = self.status.dates.min.potential.datetime
                end_date = self.status.dates.max.potential.datetime

        self.requirements['start'] = start_date
        self.requirements['end']  = end_date
        self.logger.info('\tUpdate requirements are: From: {} to {}.'.format(start_date, end_date))

        # todo: conform dates, datetimes, etc
        drange = pd.date_range(pd.Timestamp(start_date).tz_localize(None).date(),
                               pd.Timestamp(end_date).tz_localize(None).date(), freq='D')

        drange_str = list(map(lambda x: DateTime.make_string_date(x, sep=""), drange))

        self.requirements['range'] = {'date':drange, 'str':drange_str}
        return self.requirements


    # *******  *******   *******   *******   *******   *******   *******
    def update(self, lobby, locator: None| str | Path | DNA | Node = None, mode = 'slow'):

        if lobby == {}:
            self.logger.info("Came to database update with empty lobby {}. Returning idle.")
            return

        basenode = self.tree.get_node(locator)
        lobbytree = Tree(root_path=basenode.path,
                         root_dict = lobby,
                         depth_mapping = {k:v for k,v in self.tree.depth_mapping.items() if k >= basenode.depth})

        # todo: rewrite __fast and __slow update, to be modular per file, so I can choose for each file, depending on its size
        #    whether to do a robust or a fast upsert
        #   for now, I check the whole directory size

        dirsize_thresh_MB = 50
        if self.status.exists == False:
            self.__fast_update(self.tree, lobbytree)

        else:
            dirsize = sum(d.stat().st_size for d in basenode.path.rglob('*') if d.is_file()) * 1e-06
            if mode == 'fast' or dirsize >= dirsize_thresh_MB:
                self.__fast_update(self.tree, lobbytree)
            elif mode == 'slow':
                self.__slow_update(self.tree, lobbytree)


    # *******  *******   *******   *******   *******   *******   *******
    def __fast_update(self, basetree, lobbytree):
        file_nodes = basetree.get_nodes_whose('kind', equals='file')
        pbar = tqdm.tqdm(file_nodes,
                         desc="\tDatabase Update (fast)",
                         **exso._pbar_settings)
        for fn in pbar:

            lobby_df = lobbytree.get_node(fn.dna)()
            pbar.set_postfix_str(s=fn.name)

            if self.is_multiindex == False:
                lobby_df = self.force_timezone_to(lobby_df, timezone=None)

            if self.status.exists and (self.is_multiindex is False):
                head = IO.read_file(basetree[fn.dna].path, nrows = 2)

                base_df_mock = pd.DataFrame(columns = head.columns)
                df, _ = lobby_df.align(base_df_mock, join = 'outer', axis = 1)
            else:
                df = lobby_df

            df = self.__cleaning_pipeline(df, drop_trivial_cols = False)

            IO.write_file(fn.path, df, mode='a')

    # *******  *******   *******   *******   *******   *******   *******
    def __slow_update(self, basetree, lobbytree ):

        basenode = basetree.root

        basenode()

        file_nodes = basetree.get_nodes_whose('kind', equals='file')
        pbar = tqdm.tqdm(file_nodes,
                         desc="\tDatabase Update (robust)",
                         **exso._pbar_settings)
        for fn in pbar:

            base_df = fn()
            lobby_df = lobbytree.get_node(fn.dna)()
            if self.is_multiindex == False:
                lobby_df = self.force_timezone_to(lobby_df, timezone=None)

            df = pd.concat([base_df, lobby_df], axis = 0)
            df = self.__cleaning_pipeline(df)

            IO.write_file(fn.path, df, mode= 'w')

    # *******  *******   *******   *******   *******   *******   *******
    def __cleaning_pipeline(self, df, drop_trivial_cols = True):
        df = df.sort_index()
        df = df[~df.index.duplicated()]

        if self.is_multiindex == False:
            df = df[~df.index.isna()]
            thresh = 0.9 # percentage of values that are not float-convertable. If more than thresh, the column is considered unfloatable
            df = self.__insert_missing_records(df)
            ensure_float = Misc.Infer_types_of_object(df, thresh=thresh)
            df = ensure_float.df
            if not ensure_float.unfloatable.empty:
                self.logger.warning("While trying to convert object columns that have a float/non-float ratio above {}, some columns were below the threshold and thus deemed as unfloatable.".format(thresh))
                self.logger.info("Unfloatable columns: \n\n" + STR.df_to_string(ensure_float.unfloatable))

        if drop_trivial_cols:
            df = df.dropna(how='all', axis='columns')

        return df

    # *******  *******   *******   *******   *******   *******   *******
    def __insert_missing_records(self, df):
        current_dates = df.index
        start = current_dates[0]
        end = current_dates[-1]

        ideal_dates = pd.date_range(start, end, freq=self.r.resolution)
        ideal_size = ideal_dates.size

        if ideal_size == df.shape[0]:
            pass
        try:
            df = df.reindex(ideal_dates)  # no fillna
        except:
            print('ERROR. Failed to reindex properly.')
            print()
            print(traceback.format_exc())
            print()
            df.to_csv("failed_reindexing.csv")

            raise Warning
        return df

    # *******  *******   *******   *******   *******   *******   *******
    def force_timezone_to(self, df, timezone):

        dff = df.copy()
        if hasattr(df.index, 'tzinfo'):
            current_tz = dff.index.tzinfo
        else:
            current_tz = None

        if current_tz:
            if current_tz == timezone:
                pass
            else:
                dff = df.tz_convert(timezone)
        else:
            dff = df.tz_localize(timezone, ambiguous = 'infer')

        return dff

    # *******  *******   *******   *******   *******   *******   *******
    def assert_matching(self, lobby, node):

        if node.kind in ['file', 'property']:
            assert isinstance(lobby, pd.DataFrame)
        elif node.kind == 'field':
            assert isinstance(lobby, dict)
            assert isinstance(list(lobby.values())[0], pd.DataFrame)
        elif node.kind == 'report':
            assert isinstance(lobby, dict)
            assert isinstance(list(lobby.values())[0], dict)
            assert isinstance(list(lobby[list(lobby.keys())[0]].values())[0], pd.DataFrame)
    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******
