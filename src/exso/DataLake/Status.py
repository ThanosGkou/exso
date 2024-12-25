import copy
import datetime
import glob
import logging
import os
import re
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from exso.DataLake.APIs import ZipHandler
from exso.Utils.DateTime import DateTime
from exso.Utils.Misc import Misc
from exso.Utils.Paths import Paths
from exso.Utils.STR import STR

date_lambda = lambda x: datetime.datetime.strftime(DateTime.date_magician(x, return_stamp = False), format="%d-%b-%y")

# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
class Status:
    ''' This is the class that scans the datalake files, extracts relevant information, checks its continuity, and its update requirements (with an on/off assessment at this stage)
        This is a stand-alone class, that can be instantiated directly. It requires a call to .initialize() after instantiation

        The .refresh() method, updates the variable attributes of the object (e.g. after an update has occured)


    '''
    history = []

    def __init__(self, dir:str|Path, min_potential_date, max_potential_date, eligibility, time_lag_days:int, sheet_tags:list, period_covered:str):
        '''
        
        :param dir:
        :param min_potential_date:
        :param max_potential_date:
        :param eligibility:
        :param time_lag_days:
        :param sheet_tags:
        :param period_covered:
        '''

        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.logger.info("Status Check.")

        self.dir = dir
        self.eligibility = eligibility
        self.time_lag_days = time_lag_days
        self.sheet_tags = sheet_tags
        self.period_covered = period_covered

        self._temp_min_potential_date = min_potential_date
        self._temp_max_potential_date = max_potential_date

        self.initialize()

    # *******  *******   *******   *******   *******   *******   *******
    def initialize(self):
        if isinstance(self.dir, str):
            self.dir = Path(self.dir)

        self.logger.info('\n-->Checking Datalake Status ("{}u)'.format(self.dir))

        self.up_to_date = False
        self.exists = False

        self.make_dir_if_not_exists()
        self.file_df = pd.DataFrame({'filepaths': [], 'filenames': [], 'dates': []}, index=[])
        self.init_dates()

    # *******  *******   *******   *******   *******   *******   *******
    def init_dates(self):
        dates = {'min': {'observed': {'date': None},
                         'potential': {'date': self._temp_min_potential_date}},
                 'max': {'observed': {'date': None},
                         'potential': {'date': self._temp_max_potential_date}},
                 'range': {'observed': {'date': pd.DatetimeIndex([]),
                                        'str': None},
                           'ideal': {'date': pd.date_range(self._temp_min_potential_date, self._temp_max_potential_date,
                                                           freq=self.period_covered),
                                     'str': None}
                           },
                 'missing': {'date': None,
                             'str': None},
                 }
        self.logger.info("Initial dates dict: {}".format(dates))
        self.dates = Misc.Dict2Object(dates)

    # *******  *******   *******   *******   *******   *******   *******
    def refresh(self, timeslice:dict={}, use_lake_version:int|str = 'latest'):

        self.logger.info("\n--> Refreshing DataLake. Dir: {}".format(self.dir))
        self.logger.info("\tEligibility dict:" + STR.iterprint(self.eligibility, return_text=True) + '\n' +
                         '\tSheet-Nicknames:' + str(self.sheet_tags))
        self.logger.info("timeslice: {}".format(timeslice))
        self.logger.info(f"{use_lake_version = }")

        self.use_lake_version = use_lake_version

        filepaths = self.get_lake_files()

        self.file_df = self.get_file_df(filepaths)

        if Status.history:
            self.diff = self.compare_to_previous()

        self.file_df = self.get_timeslice(self.file_df, **timeslice)

        if self.file_df.empty:
            self.logger.warning('\tNo eligible files were found in the datalake.')

        else:
            self.exists = True
            self.file_df = self.get_lake_version(self.file_df, lake_version=self.use_lake_version)
            self.filepaths = self.file_df['filepaths'].values
            self.accommodate_time_lag()
            self.get_dates_info()

            self.check_if_up_to_date()
            self.check_if_complete()
            self.logger.info('\n-->Comparing the files found in datalake, with the potential range of the filetype.')

        self.history.append(copy.deepcopy(self))
    # *******  *******   *******   *******   *******   *******   *******
    def compare_to_previous(self):

        prev = Status.history[-1]

        dff = pd.merge(prev._file_df_all, self._file_df_all, how='outer', left_index=True, right_index=True, suffixes=("_old", '_new'))

        show = dff[dff['latest_available_new'] != dff['latest_available_old']].drop_duplicates(
            subset='dates_new', keep='last')

        version_addition = pd.DataFrame({'dates': show['dates_new']}, index=show.index)
        version_addition['added_versions'] = show['latest_available_new'] - show['latest_available_old'].fillna(0)

        self._refresh_overview = version_addition

    # *******  *******   *******   *******   *******   *******   *******
    def make_dir_if_not_exists(self):

        self.logger.info("Asserting datalake directory exists (creating if non-existing)")

        if not self.dir.exists():
            self.logger.warning(
                '\tDatalake path does not exist, or is empty. Will Create it, and dwnloaded data will still be stored in it.')
        else:
            self.logger.info('\tDatalake path exists.')

        self.dir.mkdir(exist_ok=True, parents=True)

    # *******  *******   *******   *******   *******   *******   *******
    def get_lake_files(self, _dir=None, eligibility=None):
        ''' Get filepaths that match name-eligibility criteria'''

        self.logger.info('Analyzing Datalake Cache')

        if not _dir:
            _dir = self.dir

        if not eligibility:
            eligibility = self.eligibility

        self.raw_lake_content = list(_dir.glob('*'))

        rule = Paths.make_glob_filter(str(_dir), eligibility)

        zh = ZipHandler.ZipHandler(zipped_dir = _dir, extract_to_dir = None, must_contain = os.path.split(rule)[-1], must_not_contain = None)
        zh.run()

        filepaths = glob.glob(rule)

        if len(filepaths) == 0:
            if len(list(_dir.glob('*'))) > 0:
                warnings.warn("Possibly there is a problem with the glob rule. No files were found in the datalake.")

        self.logger.info('\t\tUsed filtering rule --> ' + rule)
        self.logger.info("Initially perceived number of eligible files: {}".format(len(filepaths)))

        return filepaths

    # *******  *******   *******   *******   *******   *******   *******
    def get_file_df(self, filepaths):
        ''' Create a dataframe with columns: [filepaths, filenames, dates],
                                and index: str_dates
        '''

        filenames = list(map(lambda x: os.path.split(x)[-1], filepaths))
        str_dates = list(map(lambda x: re.findall(r'\d{8}', x)[0], filenames))

        dates = DateTime.date_magician(str_dates, return_stamp=False)

        df = pd.DataFrame({'filepaths': filepaths, 'filenames': filenames, 'dates': dates}, index=str_dates)

        df['true_version'] = df.groupby('dates').cumcount() + 1
        vcounts = df.index.value_counts().sort_index().to_frame(name='latest_available')
        df = pd.merge(df, vcounts, left_index=True, right_index=True)
        self._file_df_all = df.copy()

        if df.empty:
            self.logger.warning('\tNo eligible files were found in the datalake.')
        else:
            self.logger.info("Found {} preliminary eligible files".format(df.shape[0]))

        return df

    # *******  *******   *******   *******   *******   *******   *******
    def get_timeslice(self, file_df, start_date=None, end_date=None, dates_iterable=None, str_dates=None):
        ''' Get a slice of the file dataframe
            either slice with start-end dates, or by iterable datetime-like, or by iterable str-date like.
        '''

        self.logger.info('Getting Timeslice of datalake.')
        sliced = file_df.copy()

        if start_date:
            self.logger.info('\tKeeping dates after: {}'.format(start_date))
            sliced = sliced[sliced['dates'] >= DateTime.date_magician(start_date, return_stamp=True)].copy()

        if end_date:
            self.logger.info('\tKeeping dates before: {}'.format(end_date))
            sliced = sliced[sliced['dates'] <= DateTime.date_magician(end_date, return_stamp=True)].copy()

        if not isinstance(dates_iterable, type(None)):
            self.logger.info('\tKeeping dates within: {}'.format(dates_iterable))
            sliced = sliced[sliced['dates'].isin(dates_iterable)].copy()

        if not isinstance(str_dates, type(None)):
            self.logger.info('\tKeeping dates (str) within: {}'.format(str_dates))
            sliced = sliced[sliced.index.isin(str_dates)].copy()

        self.logger.info("Number of lake files after time-slicing: {}".format(len(sliced)))
        return sliced

    # *******  *******   *******   *******   *******   *******   *******
    def get_lake_version(self, file_df, lake_version: int | str = 'latest'):
        ''' Quite often, there are two or more files for the same date.
            This allows to select which version you want.
        :param file_df: the file dataframe with: index = strdates (str(YYYYMMDD)), columns: ['dates', 'filepaths']
        :param lake_version: integer or 'first' or 'last'
        :return: sliced dataframe where:
                the file version is either the one specified, or the latest available PRIOR to the one specified

        File versions are notated with a "_##" prior to the file extension (e.g. 20220101_ISPResults_02.xlsx)
        BUT: sometimes, while only e.g. 2 files are available for a given date, their versions are not always _01 and _02, but may be e.g. _02 _07.
        That's why I need a true-version assessment

        '''

        df = file_df.copy()
        self.logger.info('Deduplicating datalake files, i.e. keeping a unique version for each date.')
        self.logger.info('Keeping lake files of true_version = {}'.format(lake_version))


        max_version = df['true_version'].max()

        if lake_version == 'latest':
            keep_version = max_version
        elif lake_version == 'first':
            keep_version = 1
        else:
            keep_version = lake_version
        self.logger.info("Maximum overall observed file version = {}".format(max_version))
        self.logger.info("Will keep files of version {}, or the latest available PRIOR to this version".format(keep_version))


        df['version_diff'] = df['latest_available'] - keep_version

        df = pd.concat([df.loc[((df['version_diff'] >= 0) & (df['true_version'] == keep_version))],
                         df.loc[((df['version_diff'] < 0) & (df['true_version'] == df['latest_available']))]],
                        axis=0).sort_index()

        self.logger.info('Eligible Files after deduplication: {}'.format(df.shape[0]))
        self.logger.info('Head/tail of file_df: \n\n{}'.format(STR.df_to_string(df.head()) + '\n\n' + STR.df_to_string(df.tail())))
        # this is only capable of extracting the first or the last
        # df = df[~df.index.duplicated(keep='last')].copy()

        return df

    # *******  *******   *******   *******   *******   *******   *******
    def get_dates_info(self):
        ''' after aplying all file dataframe slicing, identify the observed start, end, and range of dates in the lake
            then, update the "dates" attribute
        '''
        file_df = self.file_df

        upd = {'min': {'observed': {'date': file_df['dates'].iloc[0].date()}},
               'max': {'observed': {'date': file_df['dates'].iloc[-1].date()}},
               'range': {'observed': {'date': pd.DatetimeIndex(file_df['dates'].values),
                                      'str': np.array(file_df.index)}}
               }

        self.dates.update(upd)

        self.logger.info("Final dates dict: {}".format(self.dates))

    # *******  *******   *******   *******   *******   *******   *******
    def check_if_up_to_date(self):
        self.logger.info(
            "\tChecking if cache is up-to-date, judging ONLY based on if, the lake contains the latest potentially available file.")

        self.logger.info(
            "Datalake OBSERVED dates:\nStart date: {}\nEnd date:   {}".format(date_lambda(self.dates.min.observed.date),
                                                                              date_lambda(
                                                                                  self.dates.max.observed.date)))
        self.logger.info("Datalake POTENTIAL dates:\nStart date: {}\nEnd date:   {}".format(
            date_lambda(self.dates.min.potential.date), date_lambda(self.dates.max.potential.date)))

        max_date_check = pd.Timestamp(self.dates.max.observed.date) >= pd.Timestamp(self.dates.max.potential.date)
        min_date_check = pd.Timestamp(self.dates.min.observed.date) <= pd.Timestamp(self.dates.min.potential.date)

        if max_date_check and min_date_check:
            self.logger.info('\n\t--> The Datalake cache IS up to date.\n')
            self.up_to_date = True
        else:
            self.logger.info('\n\t--> The Datalake cache iS NOT up to date.\n')
            self.up_to_date = False


    # *******  *******   *******   *******   *******   *******   *******
    def accommodate_time_lag(self):
        if self.time_lag_days:
            self.logger.info(
                "Accommodating time lag: making files of date e.g. 2020/11/25, refer to dates of 2020/11/24")
            self.file_df['dates'] = self.file_df['dates'] - pd.Timedelta(self.time_lag_days,
                                                                         'D')  # - pd.Timedelta(1,'D')

    # *******  *******   *******   *******   *******   *******   *******
    def check_if_complete(self):
        laked_observed = self.dates.range.observed.date

        laked_potential = self.dates.range.ideal.date.copy()
        laked_potential = laked_potential[laked_potential <= laked_observed[-1]]

        self.logger.info(
            '\tChecking if Cache is Complete, judging from one-to-one comparison.\n\tSome missing dates, are to be expected, since during the testing periods, file updates were irregular.\n')

        missing = np.setdiff1d(laked_potential, laked_observed, assume_unique=True)

        missing = pd.to_datetime(missing)
        if self.time_lag_days:
            missing += pd.Timedelta("{}D".format(self.time_lag_days))

        self.dates.update({'missing': {'date': missing,
                                       'str': np.array(DateTime.make_string_date(list(missing), sep=''))}})

        dates_that_should_be_but_arent = list(map(date_lambda, missing))

        self.logger.info('\tDates that are missing compared to the Perfect, Fully updated cache:' + '\n' + str(
            dates_that_should_be_but_arent))
        self.logger.info('\tIf above, says: "Cache IS up-to-date, but here, recent dates are missing, search it a bit.')
        self.logger.info(
            '\tFor older dates (e.g. Jun-2020, etc.) there was actual unavailability of ADMIE. \n\tIF you think that some of these missing files, are not due to ACTUAL UNAVAILABILITY, check manually online and manually place the file in the datalake.')

