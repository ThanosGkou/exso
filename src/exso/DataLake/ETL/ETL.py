from __future__ import annotations

import datetime
import logging
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import numpy as np
import pandas as pd
from colorama import Fore
from exso.DataLake.ETL.FileReaders import Readers
from exso.Utils.DateTime import DateTime
import exso
from tqdm import tqdm

# warnings.filterwarnings('error')
date_lambda = lambda x: datetime.datetime.strftime(DateTime.date_magician(x, return_stamp = False), format="%d-%b-%y")
###############################################################################################
###############################################################################################
###############################################################################################
class Loader:
    def __init__(self, report_object, file_df ):
        '''
        The __init__ is not used, but it can be used if provided with:

        :param report_object: Report.Report instance
        :param file_df: pd.DataFrame with index = str_dates [20220131, ...], and columns: ['filepaths'] and ['dates'] (although not used here)

        The class is inherited by the Pipeline class, which directly calls the methods of Loader, without instantiating a Loader object
        '''

        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.r = report_object
        self.file_df = file_df
        self.str_dates = self.file_df.index
        self.filepaths = self.file_df['filepaths']

    # *******  *******   *******   *******   *******   *******   *******
    def decide_reading_engine(self):
        file_format = self.r.file_format
        if file_format == 'xlsx':
            engine = 'openpyxl'
        elif file_format == 'xls':
            engine = None
        elif file_format == 'csv':
            engine = None
        else:
            raise NotImplementedError
        self.engine = engine
        return engine

    # *******  *******   *******   *******   *******   *******   *******
    def get_reader(self):
        self.kwargs = {'sheet_name': self.r.sheet_locators,
                       'header': self.r.header,
                       'skiprows': None,
                       'usecols': self.r.use_cols,
                       'index_col': None,
                       'engine': self.engine}

        if self.r.report_name in ['BalancingEnergyProduct', 'BalancingCapacityProduct']:
            self._reader = Readers.adhoc_reader
        elif self.r.publisher == 'entsoe':
            self._reader = Readers.csv_reader
        else:
            self._reader = Readers.standard_reader

    # *******  *******   *******   *******   *******   *******   *******
    def readAll(self, threads=None):
        ''' For each date, read the sheets specified according to 'reading_config' attribute
                                memory_sheets = {20210320:  {0: whole dataframe of sheet1
                                                            ..: whole dataframe of sheet ...,
                                                             n: whole dataframe of sheet n
                                                             }
        '''

        self.failed_dates = []
        t0 = time.time()

        if threads:
            in_memory = self.multi_threaded(n_threads=2)
        else:
            in_memory = self.single_threaded()

        in_memory = {date: self.rename_locator_keys_to_tag_keys(date_dicts) for date, date_dicts in in_memory.items()}

        tot_time = time.time() - t0
        time_per_file = np.nan if self.file_df.empty else round(tot_time/self.file_df.shape[0],3)
        self.logger.info( "--> Reading completed. Parsed {} files in: {:,} sec ({} sec/file on average)".format(self.file_df.shape[0],
                                                                                                  round(tot_time, 2),
                                                                                                  time_per_file))
        self.report_failed_dates()
        self.data = in_memory

        return in_memory

    # *******  *******   *******   *******   *******   *******   *******
    def report_failed_dates(self):
        if self.failed_dates:
            self.file_df = self.file_df[~self.file_df.index.isin(self.failed_dates)].copy()
            self.logger.warning(
                "Some files failed to be read. They will be ignored.\nDates of failed files are: " + str(
                    self.failed_dates))
        else:
            self.logger.info('\tAll files extracted successfuly (extract = load raw files into memory)')

    # *******  *******   *******   *******   *******   *******   *******
    def single_threaded(self):

        tqdm._instances.clear()
        progress_bar = tqdm(range(len(self.str_dates)),
                            desc='\tReading Progress',
                            **exso._pbar_settings)

        in_memory = {}
        for i, date, fp in zip(progress_bar, self.str_dates, self.filepaths):

            progress_bar.set_postfix_str(s=date_lambda(date))
            try:
                dfs = self._reader(self.kwargs, fp)
                in_memory[date] = dfs

            except:
                self.failed_dates.append(date)
                print(traceback.format_exc())
                self.logger.warning("Failed to read: {}. Exception: {}".format(fp, traceback.format_exc()))

        return in_memory

    # *******  *******   *******   *******   *******   *******   *******
    def multi_threaded(self, n_threads):

        reader = partial(self._reader, self.kwargs)
        with ThreadPoolExecutor(n_threads) as executor:
            futures = list(tqdm(executor.map(reader, self.filepaths),
                                desc="\tReading Progress (mt)",
                                **exso._pbar_settings
                                )
                           )

        in_memory = dict(zip(self.str_dates, futures))

        return in_memory

    # *******  *******   *******   *******   *******   *******   *******
    def rename_locator_keys_to_tag_keys(self, dfs):
        ''' Sheet Locators are integer indices of actual excel sheets, or (but not used to avoid mixing), the exact sheet-names (str) of the excel book
            Sheet-tags are aliases given to each one of the sheet-locators.
            "fields" have the name of these sheet-tags, and this is also how it shows on the final database
            Report (dir) > Field (dir) > Subfield (file) > Property (csv column)

            The actual reader object, reads the sheet-locators (unique, exact identifiers of excel sheets)
            Thus, after loading them into memory, the locators are given their corresponding tag-names
        '''

        for sheet_locator in self.r.sheet_locators:
            sheet_tag = self.r.get_sheet_tag[sheet_locator]
            dfs[sheet_tag] = dfs.pop(sheet_locator)
        return dfs




###############################################################################################
###############################################################################################
###############################################################################################
class Parser:
    #     wfp = os.path.join(self.f.meta.datalake_path, '_parserwarnings.log')
    #     warnings_report = STR.df_to_string(pd.DataFrame.from_dict(self.parser.warnings).T.value_counts())
    #     with open(wfp, 'w') as f:
    #         f.write(warnings_report)
    #     self.get_field_summary_as_is()
    def __init__(self, report_object, file_df , data):
        '''
        The __init__ is not used, but it can be used if provided with:

        :param report_object: Report.Report instance
        :param file_df: pd.DataFrame with index = str_dates [20220131, ...], and columns: ['filepaths'] and ['dates'] (although not used here)
        :data: dict of dicts of dataframes: {20220101:{field1:df1, fieldN:dfN}, ..., 20220102:{field1:df1, field2:df2}}

        The class is inherited by the Pipeline class, which directly calls the methods, without instantiating a Loader object
        '''

        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.r = report_object
        self.file_df = file_df
        self.str_dates = self.file_df.index
        self.filepaths = self.file_df['filepaths']
        self.data = data
        self.dates_per_period, _ = self.datetime_constructor()
        self.parser = self.get_parser()


    # *******  *******   *******   *******   *******   *******   *******
    def get_field_summary_as_is(self):
        ''' Some times, during parsing a report, additional fields/subfields may be added along the way
            e.g. From ISPResults, the field "Market_Schedule"  is derived/created
            e.g. From UnitAvailabilities, the subfield "Hydro_Unavailability_Reason" is created.

            Thus, these need to be added to the field_summary dict-structure

            Steps:
             1. get a random (the first) str-date of what was parsed
             2. for that date (or any other) get its structure
                {field1:{subfield1: df1, ...., subfieldM: dfM}, ..., fieldN:{subfield1:df1, ..., subfieldK:dfK}}
            3. extract the field_summary (the structure without the dataframes:
                {field1:[subfield1, ..., subfield1], ....,  fieldN:[subfield1, subfieldK]}

        '''
        some_str_date = self.file_df.index[0]
        structure_as_is = self.data[some_str_date].copy()
        field_summary = {}
        for field, subfields_dfs in structure_as_is.items():
            field_summary[field] = []
            for subfield, subfield_df in subfields_dfs.items():
                field_summary[field].append(subfield)

        self.field_summary = field_summary

    # *******  *******   *******   *******   *******   *******   *******
    def get_parser(self):

        if self.r.time_axis == 'horizontal':
            import exso.DataLake.Parsers.ParsersHorizontal as RelevantParsers
        else:
            if self.r.long_format == True:
                import exso.DataLake.Parsers.ParsersVerticalLong as RelevantParsers
            else:
                import exso.DataLake.Parsers.ParsersVerticalWide as RelevantParsers

        if self.r.publisher == 'entsoe':
            if 'generation_per_plant' in self.r.report_name.lower():
                parser = RelevantParsers.GenerationPerPlant
            else:
                parser = RelevantParsers.ENTSO
        else:
            parser = getattr(RelevantParsers, self.r.report_name)

        info_for_parser = {'report_name': self.r.report_name,
                           'cue_dict': self.r.cue_dict,
                           'total_expected': self.file_df.shape[0],
                           'renamer_mapping': self.r.renamer_mapping,
                           'replacer_mapping': self.r.replacer_mapping,
                           'regex_mapping': self.r.regex_mapping,
                           }

        parser = parser(**info_for_parser)

        if self.r.report_name == 'SystemRealizationSCADA':
            parser.set_ccgt_modes_switch(retain_modes=True)
        self.parser = parser
        return parser

    # *******  *******   *******   *******   *******   *******   *******
    def __transform_and_joinAll(self, threads = None):

        t0 = time.time()
        tqdm._instances.clear()
        progress_bar = tqdm(range(self.file_df.shape[0]),
                            desc='\tParsing Progress',
                            **exso._pbar_settings)

        joined_data = {}
        for i in progress_bar:

            str_date = self.file_df.index[i]
            progress_bar.set_postfix_str(s=date_lambda(str_date))
            content = self.data[str_date]

            for sheet_tag, sheet_df in content.items():
                if sheet_tag not in joined_data.keys():
                    joined_data[sheet_tag] = {}
                sheet_subdfs = self.parser.pipe(sheet_tag, sheet_df, self.dates_per_period[ str_date])  # parse_property_sheet(df_sheet, property=property)


                for subfield, df in sheet_subdfs.items():
                    if subfield not in joined_data[sheet_tag].keys():
                        joined_data[sheet_tag][subfield] = df
                    else:
                        joined_data[sheet_tag][subfield] = pd.concat([joined_data[sheet_tag][subfield], df], axis = 0)


        joined_data = self.parser.expost(joined_data)  # this is probably in-place dictionary append. to confirm

        tot_time = time.time() - t0
        self.logger.info("--> Parsing completed. Parsed {} files in: {:,} sec".format(self.file_df.shape[0], round(tot_time, 2)))

        self.data = joined_data
        return joined_data
    # *******  *******   *******   *******   *******   *******   *******
    def transformAll(self, threads = None):

        data = {}
        t0 = time.time()
        tqdm._instances.clear()
        progress_bar = tqdm(range(self.file_df.shape[0]),
                            desc='\tParsing Progress',
                            **exso._pbar_settings)

        for i in progress_bar:
            str_date = self.file_df.index[i]
            progress_bar.set_postfix_str(s=date_lambda(str_date))
            content = self.data[str_date]
            data[str_date] = {}

            for sheet_tag, sheet_df in content.items():
                sheet_subdfs = self.parser.pipe(sheet_tag, sheet_df, self.dates_per_period[str_date])  # parse_property_sheet(df_sheet, property=property)
                data[str_date][sheet_tag] = sheet_subdfs

            data[str_date] = self.parser.expost(data[str_date])  # this is probably in-place dictionary append. to confirm

        tot_time = time.time() - t0
        self.logger.info("--> Parsing completed. Parsed {} files in: {:,} sec".format(self.file_df.shape[0], round(tot_time, 2)))

        self.data = data
        return data
    
    # *******  *******   *******   *******   *******   *******   *******
    def datetime_constructor(self):
        self.logger.info("\tCreating dates/datetimes for each period-covered.")

        dates_flat_series = []
        dates_per_period_dict = {}

        for i in range(self.file_df.shape[0]):

            str_date = self.file_df.index[i]
            if self.r.requires_tz_handling == False:
                period_dates = [self.file_df.loc[str_date, 'dates']]

            else:
                period_dates = DateTime.dates_constructor(str_date,
                                                          resolution=self.r.resolution,
                                                          start_hour=self.r.start_hour,
                                                          period_covered=self.r.period_covered,
                                                          timezone=self.r.inherent_tz)

            dates_flat_series = np.concatenate([dates_flat_series, period_dates])
            dates_per_period_dict[str_date] = period_dates

        self.logger.info('\t\tDates/datetimes Constructor finished.')

        return dates_per_period_dict, dates_flat_series

    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******


###############################################################################################
###############################################################################################
###############################################################################################
class Joiner:
    def __init__(self, report_object, file_df, data):
        '''
        The __init__ is not used, but it can be used if provided with:

        :param report_object: Report.Report instance ## not really used, but....
        :param file_df: pd.DataFrame with index = str_dates [20220131, ...], and columns: ['filepaths'] and ['dates'] (although not used here)
        :data: dict of dicts of dicts dataframes: {20220101:{field1:{subfield1:df1, subfield2: df2}, fieldN:{subfield1:df1, subfield2:df2}}

        The class is inherited by the Pipeline class, which directly calls the methods, without instantiating a Loader object
        '''

        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.r = report_object
        self.file_df = file_df
        self.str_dates = self.file_df.index
        self.filepaths = self.file_df['filepaths']
        self.data = data

    # *******  *******   *******   *******   *******   *******   *******
    def joinAll(self):
        the_first_str_date = list(self.data.keys())[0]
        internal_structure = self.data[the_first_str_date]
        joined_data = {field: {} for field in internal_structure.keys()}

        t0 = time.time()

        fields = list(self.data[the_first_str_date].keys())
        all_assets = [[(f, sf) for sf in self.data[the_first_str_date][f].keys()] for f in fields]
        assets_flat = []
        for ass in all_assets:
            for subass in ass:
                assets_flat.append(subass)


        pbar = tqdm(assets_flat,
                    desc="\tJoining",
                    **exso._pbar_settings)

        for field, subfield in pbar:
            dfs = [self.data[d][field][subfield] for d in self.file_df.index]
            joined_data[field][subfield] = pd.concat([*dfs], axis = 0)
            pbar.set_postfix_str(s=field)

        tf = time.time() - t0
        self.logger.info( "--> Joining completed. Joined {} files in: {:,} sec ({} sec/file on average)".format(self.file_df.shape[0],
                                                                                                  round(tf, 2), round( tf / self.file_df.shape[0], 3)))
        self.data = joined_data

        # TODO: Check for consecutive nans from end to start.
        #   e.g. If the LAST 3 days are all NaN, completely delete them from the database, in order to avoid update-confusions
        return joined_data
    # *******  *******   *******   *******   *******   *******   *******

