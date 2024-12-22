import datetime
import logging
import sys
import traceback
from abc import abstractmethod

import numpy as np
import pandas as pd
from exso.DataLake.Parsers.Operations import Operations
from exso.Utils.DateTime import DateTime

# *********************************************
date_lambda = lambda x: datetime.datetime.strftime(DateTime.date_magician(x, return_stamp = False), format="%d-%b-%y")
logger = logging.getLogger(__name__)
# *********************************************

def error_handler(func):
    def wrap(*args, **kwargs):

        try:
            res = func(*args, **kwargs)

        except:
            arch_parser = args[0]
            arch_parser.log_now = True
            arch_parser.log_all_operations = True
            # if exception, force log every step
            line = '\n' + '*'*50
            logger.error(line*10 + "\nException Occured while parsing.\n'Re-running last example with full-logging enabled.")

            try:
                res = func(*args, **kwargs)
            except:
                logger.error('\n\n Exiting. Exception was: {}\n\n'.format(traceback.format_exc()))

                print('\n\n--> ParsingError. See logs.')
                print()
                print(traceback.format_exc())
                print()
                sys.exit()



        return res

    return wrap

# *********************************************
# *********************************************
# *********************************************
class Archetype(Operations):

    def __init__(self, report_name:str, cue_dict:dict, total_expected:int, renamer_mapping:dict, replacer_mapping:dict, regex_mapping:dict):
        '''
        The same parser object is used for all fields (sheets) of all files of a specific report-type
        But each time, the "pipe" method is called prividing different arguments.
        i.e. different sheet name (field), different sheet-raw dataframe, period-dates

        '''

        #TODO: Make custom warnings with specific IDs. So, I can check, if a warning has already been made,
        #   stop making it again, and just report it with emphasis in the end

        ''' tell me how many files are you expecting, in order to make a debugging log each n iterations'''
        self.total_parsed = 0
        self.n_debugging_logs = 5
        self.make_log_every = int(total_expected / self.n_debugging_logs) + 1
        self.dates_logged = []
        self.log_now = True
        self.log_all_operations = False

        self.wc = 1 #warning counter
        self.warnings = {}

        self.logger = logging.getLogger('Initialized Parser for: '+report_name)
        self.report_name = report_name
        self.cue_dict = cue_dict

        ''' all the below can be injected into the allmight config.xlsx file'''
        self.indicator_col_index = 0
        self.drop_last = False
        self.replacer_mapping = replacer_mapping
        self.renamer_mapping = renamer_mapping

        self.regex_mapping = regex_mapping
        self.dropna_settings = {'how':None, 'thresh':None, 'axis':'columns'} #
        self.action_if_empty_df = None #[None, 'return_empty', 'return_compat'
        self.empty_df_filler = {'arbitrary_columns':None, # None, or list of arbitary lenght, with arbitrary columns
                                  'fill_value':np.nan}

        self.drop_col_settings = {'col_names':None, 'startswith':'MOCK', 'error_action':'ignore'}
        self.last_column_trigger = None
        self.skip_rows_at_final = 1

        self.param_setter()
        self.param_updater()

    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******
    @error_handler
    def pipe(self, field:str, df: pd.DataFrame, period_dates: pd.DatetimeIndex)-> dict:
        '''
        This function, takes a raw dataframe, resulted from raw-reading an excel SHEET ("field"), and returns
        a dict of dataframes, one for each "cue-point"/subfield.

        :param field: str String tag/nickname of an actual excel sheet of the current report-type
        :param df: dataframe containining uncleaned data of field/sheet, as-read
        :param period_dates: pd.DatetimeIndex consisting of tz-aware timestamps covering the time duration of this specific sheet.
                                              Size is not standard (e.g. daylight-saving, different resolution (it may even be a single timestamp)
                            period_dates are NOT used to inject as a datetimeindex (this is done outside of parsers)
                            It is used to compensate for bad practices of data publishers regarding (mostly) daylight-saving reporting

        :return: dict of pd.DataFrames. keys = subfields of that field, values = dataframes of the subfields
        '''


        self.field = field
        self.period_dates = period_dates
        self.date = period_dates[0].date()
        self.str_date = date_lambda(self.date)

        df = self.pre_proc(df)
        subfields_dfs = self.split_cue_points(df)
        subfields_dfs = self.transposeAll(subfields_dfs)
        subfields_dfs = self.rolling_post_proc(subfields_dfs)

        self.total_parsed += 1

        if divmod(self.total_parsed, self.make_log_every)[1] != 0:
            self.log_now = False

        return subfields_dfs

    # *******  *******   *******   *******   *******   *******   *******
    @abstractmethod
    def param_setter(self):
        pass
    # *******  *******   *******   *******   *******   *******   *******
    @abstractmethod
    def param_updater(self):
        pass
    # *******  *******   *******   *******   *******   *******   *******
    @abstractmethod
    def pre_proc(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    # *******  *******   *******   *******   *******   *******   *******
    @abstractmethod
    def split_cue_points(self, df: pd.DataFrame)-> dict:
        pass

    # *******  *******   *******   *******   *******   *******   *******
    @abstractmethod
    def transposeAll(self, subfields_dfs):
        pass

    # *******  *******   *******   *******   *******   *******   *******
    @abstractmethod
    def rolling_post_proc(self, subfields_dfs:dict):
        '''
            This function is being called for each field, after the pre-proc > split > transpose operations.
            It has "visibility" of all sunbfields dfs of that (one at a time) field.
            So, it is possible to, e.g. create a new "Total" subfield, which will merge all subfields - subfields-dfs
        '''
        pass

    # *******  *******   *******   *******   *******   *******   *******
    @abstractmethod
    def expost(self, fields_dfs:dict):
        '''
            This function is called, after completing all parsing of that report file.
            So, it has visisbility to all FIELD and their subfields.
            This way, it is possible to clean nan of all data, or combine subfields belonging to different fields (sheets).
            e.g. in ISP see market schedule creation

            NaN policy: When the data is being actually reported but is empty, this means zero, and so it is converted to zero.
            Afterwards (outside the parser) completely missing dates are investigated, and these data is represented with NaN - empty values
        '''

        pass


    # *******  *******   *******   *******   *******   *******   *******
