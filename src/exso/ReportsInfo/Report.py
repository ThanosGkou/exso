import datetime
import logging
import re
import sys

import numpy as np
import pandas as pd
from exso import Files
from exso.ReportsInfo.Interpretation import Metadata, ReadingSettings, ParsingSettings, TimeSettings
from exso.Utils.DateTime import DateTime
from exso.Utils.Misc import Misc
from exso.Utils.STR import STR

# *********************************************

date_lambda = lambda x: datetime.datetime.strftime(DateTime.date_magician(x, return_stamp = False), format="%d-%b-%y")

###############################################################################################
decimal_f = '{:,}'
t1 = '\t'
t2 = 2*t1
t3 = 3*t1
t4 = 4*t1
n = '\n'
star = '*'*50
halfstar = '*'*25

# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
class Pool:
    def __init__(self):
        self.logger = logging.getLogger( __name__ + '.' + self.__class__.__name__)
        self.logger.info("Initializing Reports Pool")

        system_tz = Files.system_tz
        config_dir = Files.files_dir

        self.system_tz = system_tz
        self.config_dir = config_dir

        self.config_file = self.config_dir / 'ReportsInfo.xlsx'

        self.logger.info("System timezone: {}".format(self.system_tz))
        self.logger.info("Config Directory: {}".format(self.config_dir))
        self.logger.info("Reports Pool File: {}".format(self.config_file))

        self.display_columns = ['report_name', 'publisher', 'group', 'available_from', 'available_until',
                                'is_implemented', 'official_comment', 'resolution',]

        self.initialize()

    # *******  *******   *******   *******   *******   *******   *******
    def initialize(self):
        xl = self.load_config_file()
        dfs_dict = self.read_sheets(xl, sheets = ['Metadata', 'Read Settings', 'Parse Settings', 'Time Settings'])
        dfs_dict = self.rename_columns(dfs_dict)
        self.cols_allocation = {sheet_name: df.columns.to_list() for sheet_name, df in dfs_dict.items()}

        self.allmighty_df = self.collapse(dfs_dict)
        self.logger.info("Reports Pool Dataframe: \n\n" + STR.df_to_string(self.allmighty_df))

    # *******  *******   *******   *******   *******   *******   *******
    def load_config_file(self):
        if not self.config_file.is_file():
            print('Config filepath is not a vaild file path ("{}")'.format(self.config_file))
            self.logger.fatal(t2 + 'Fatal: Filetype Properties File doesnot exist: ("{}")'.format(self.config_file))
            sys.exit(1)

        xl = pd.ExcelFile(self.config_file)
        self.logger.info(t2 + 'Successfully Loaded configuration file ("{}")'.format(self.config_file))

        return xl

    # *******  *******   *******   *******   *******   *******   *******
    def read_sheets(self, xl, sheets):
        sheets_dfs = {sheet_name: xl.parse(sheet_name, header=0) for sheet_name in sheets}
        return sheets_dfs

    # *******  *******   *******   *******   *******   *******   *******
    @staticmethod
    def rename_columns(dfs):

        _renamer = lambda x: "_".join(re.sub('[().]', '', x.strip()).lower().split(' '))
        dfs = {sheet_name: df.rename(columns = dict(zip(df.columns, df.columns.map(_renamer)))) for sheet_name, df in dfs.items()}

        return dfs

    # *******  *******   *******   *******   *******   *******   *******
    def collapse(self, dfs):
        # every df has a "filetype" column, with identical values. drop after concatenation
        single_df = pd.concat([*dfs.values()], axis = 1)
        single_df = single_df.loc[:, ~single_df.columns.duplicated()]
        return single_df

    # *******  *******   *******   *******   *******   *******   *******
    def convert_dtypes(self):

        types = {col:str for col in self.allmighty_df.columns}
        types.update({'requires_update':bool, 'is_implemented':bool, 'start_hour':int, 'time_lag_days':int,
                      'header_row':int,
                      'long_format':bool, 'persistent_long':bool,
                      'available_from_date':datetime.datetime, 'available_until_date':datetime.datetime,
                      })

        df = self.allmighty_df.copy()
        int_cols = [k for k, v in types.items() if v == int]
        bool_cols = [k for k,v in types.items() if v == bool]
        str_cols = [k for k,v in types.items() if v == str]
        df[int_cols]= df[int_cols].fillna(0)
        df[bool_cols] = df[bool_cols].astype(float).fillna(0).astype(bool)
        df[str_cols] = df[str_cols].astype(str)
        return df

    # *******  *******   *******   *******   *******   *******   *******
    def extract(self, report_name: str|None = None, alias: str|None = None):
        if not report_name and not alias:
            raise ValueError("Not both of: report_name and alias can be None.")

        if not report_name:
            scan_by = 'alias'
            value = alias
        if not alias:
            scan_by = 'report_name'
            value = report_name

        df = self.allmighty_df[self.allmighty_df[scan_by] == value].copy()
        self.logger.info(
            "Filetype Input Dataframe:\n" + STR.iterprint(Misc.single_row_df_to_dict(df), return_text=True))

        return df
    # *******  *******   *******   *******   *******   *******   *******
    def get_all(self):
        return self.allmighty_df[self.display_columns].copy()
    # *******  *******   *******   *******   *******   *******   *******
    def get_available(self, only_names = False):
        df = self.get_filtered(col = 'is_implemented', val = True)
        if only_names:
            return df['report_name'].values
        else:
            return df

    # *******  *******   *******   *******   *******   *******   *******
    def list_groups(self):
        groups = self.allmighty_df.groupby('group').agg(list)['report_name'].to_dict()
        return groups
    # *******  *******   *******   *******   *******   *******   *******
    def get_text_description(self):
        return dict(zip(self.allmighty_df['report_name'].values, self.allmighty_df['official_comment'].values))
    # *******  *******   *******   *******   *******   *******   *******
    def get_filtered(self, col, val):
        if not isinstance(col, list):
            col = [col]
        if not isinstance(val, list):
            val = [val]

        df = self.allmighty_df.copy()
        for column, value in zip(col, val):
            df = df[df[column]==value].copy()

        df = df[self.display_columns].copy()

        return df


###############################################################################################
###############################################################################################
###############################################################################################
class Report(Metadata, ReadingSettings, ParsingSettings, TimeSettings):
    def __init__(self, report_pool_obj, report_name, root_lake, root_base, api_allowed = True):
        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.logger.info("Instantiated report object for report-type: {}".format(report_name))

        self.api_allowed = api_allowed
        self.root_lake = root_lake
        self.root_base = root_base
        self.report_name = report_name
        self.rp = report_pool_obj
        self.config_dir = self.rp.config_dir

        self.logger.info("API access allowed? {}".format(self.api_allowed))


        self.df = self.get_report_data()
        self.json = self.df.to_dict(orient='records')[0]

        self.get_metadata(self.json)
        self.get_reading_settings(self.json)
        self.get_parsing_settings(self.json, self.sheet_tags)
        self.get_time_settings(self.json, system_tz=self.rp.system_tz)

        self.derive_directories(root_lake, root_base)


        self.make_bidirectional_mapping()
        self.available_until = self._TimeSettings__interpret_available_until(self.report_name, self.publisher, self.datalake_path, self.available_until, api_allowed=api_allowed)
        self.database_min_potential_datetime, self.database_max_potential_datetime = self.get_database_min_max_datetimes(self.available_from, self.available_until)

    # *******  *******   *******   *******   *******   *******   *******
    def get_text_description(self):
        return {'report_name':self.report_name, 'description':self.json['official_comment']}
    # *******  *******   *******   *******   *******   *******   *******
    def get_report_data(self, report_name = None):
        if not report_name:
            report_name = self.report_name

        self.logger.info("Extracting report-type info.")

        if report_name in self.rp.allmighty_df['report_name'].values:
            df = self.rp.extract(report_name=report_name)
            self.logger.info("The requested report type exists in the reports pool dataframe. Proceeding.")
            self.logger.info('\n' + STR.df_to_string(df))
            df = df.replace(np.NaN, None)
            return df

        elif report_name in self.rp.allmighty_df['alias'].values:
            df = self.rp.extract(alias = report_name)
            self.logger.info("The requested report type exists in the reports pool dataframe. Proceeding.")
            self.logger.info('\n' + STR.df_to_string(df))
            df = df.replace(np.NaN, None)
            return df

        else:
            print('Error: Requested report type: "{}" doesn not exist in the info file'.format(report_name))
            print('Available report types are: {}'.format(STR.df_to_string(self.rp.get_available())))
            self.logger.error("The requested report-type ('{}')does Not exist in the report pool dataframe".format(report_name))
            self.logger.error('Available report types are: \n\n{}'.format(STR.df_to_string(self.rp.get_available())))
            sys.exit()

    # *******  *******   *******   *******   *******   *******   *******
    def derive_directories(self, root_lake, root_base):

        # self.datalake_path = root_lake / self.publisher / self.alias
        self.datalake_path = root_lake / self.publisher / self.report_name
        # self.database_path = root_base / self.publisher / self.alias
        self.database_path = root_base / self.publisher / self.report_name

    # *******  *******   *******   *******   *******   *******   *******
    def make_bidirectional_mapping(self):
        self.get_sheet_locator = dict(zip(self.sheet_tags, self.sheet_locators))
        self.get_sheet_tag = dict(zip(self.sheet_locators, self.sheet_tags))


# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******