import datetime
import inspect
import logging
import os
import re
import time

import numpy as np
import pandas as pd
from exso.Utils.DateTime import DateTime
from exso.Utils.STR import STR

# *********************************************
date_lambda = lambda x: datetime.datetime.strftime(DateTime.date_magician(x, return_stamp = False), format="%d-%b-%y")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# *********************************************
# *********************************************
# *********************************************
def logging_sampler(func):
    def wrap(*args, **kwargs):
        func_name = func.__name__
        stack = inspect.stack()
        parser_object = args[0]

        runtime = time.time()
        res = func(*args, **kwargs)
        runtime = str(round(time.time() - runtime, 2)) + ' sec'

        if parser_object.log_all_operations == False:
            return res


        parser_full_name = parser_object.__class__.__module__ + '.' + parser_object.__class__.__name__

        ascendants = [asc.__name__ for asc in type(parser_object).__mro__[:-1]]
        parser_inheritance_history = " <-- ".join(ascendants)


        traces = []
        for fn in [1,2,3,4]:
            s = stack[fn]
            s_dict = {'called_from':os.path.split(s.filename)[-1],
                      'within_method':s.function,
                      'lineno':s.lineno,
                      'code':s.code_context[0].strip()}
            if s_dict['within_method'] == 'wrap':
                pass
            else:
                traces.append(s_dict)

        traces_text = []
        for tr in traces[::-1]:
            tr_text = (f"File: {tr['called_from']}", f"Method: {tr['within_method']}", f"LineNo: {tr['lineno']}", f"Code: {tr['code']}")
            traces_text.append(tr_text)

        def aligner(*args):
            t = ""
            argssss = args[0]
            t += '{:<30}'.format(argssss[0])
            t += '{:<35}'.format(argssss[1])
            t += '{:<20}'.format(argssss[2])
            t += '{:<50}'.format(argssss[3])
            return t

        def code_calls_printer(code_calls):

            t = "\n"
            for cc in code_calls:
                t += aligner(cc) + '\n'
            return t

        signature = {'Parser':parser_full_name, 'Method':func_name, 'Inheritance':parser_inheritance_history}

        nstar100 = '\n' + '*'*100
        date_block = nstar100 * 3 + '  Date of Parsing: {}'.format(parser_object.date) + 2 * nstar100
        end_block = nstar100 * 3

        logger.debug('Sample logging of Parsing Operations')
        logger.debug(date_block)
        # logger.debug('\t' + traces[2]['code'])
        logger.debug('\t  ' + traces[1]['code'])
        logger.debug('\t    ' + traces[0]['code'])

        logger.debug('Signature:\n\n'+STR.iterprint(signature,return_text=True),stack_info=False,stacklevel=5)
        logger.debug(code_calls_printer(traces_text),stack_info=False,stacklevel=5)
        logger.debug(nstar100)

        logger.debug('\nInput Data')
        logger.debug('Keyword Arguments:')
        logger.debug(STR.iterprint(kwargs, return_text=True))
        logger.debug("\nPayload (df, or dict of dfs)")
        logger.debug('\n\n'+STR.iterprint({'data':args[1]},return_text=True))
        logger.debug(nstar100)


        logger.debug("\nOutput data")
        logger.debug("\tRuntime: {}".format(runtime))
        logger.debug('\n'+STR.iterprint({'data':res}, return_text=True))
        logger.debug('\n'+end_block)

        return res

    return wrap
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
class Operations:
    # *******  *******   *******   *******   *******   *******   *******
    @logging_sampler
    def baptize_indicator_column(self, df, indicator_col_index = 0, field = ""):

        df.columns = df.columns[:indicator_col_index].to_list() + ['indicator'] + df.columns[indicator_col_index+1:].to_list()
        return df

    # *******  *******   *******   *******   *******   *******   *******
    @logging_sampler
    def drop_nans(self, df, settings:dict={}, field = ""):
        '''settings = { 'how': 'all', 'thresh': None, 'axis': 'columns'}'''

        if settings['how'] and settings['thresh']:
            raise AttributeError("\nWHen dropping nans, either 'how' will be defined, or 'thresh'. Not both.")

        if settings['how']:
            df = df.dropna(how = settings['how'], axis = settings['axis'])

        elif settings['thresh']:
            df = df.dropna(thresh = settings['thresh'], axis = settings['axis'])

        return df

    # *******  *******   *******   *******   *******   *******   ******
    @logging_sampler
    def apply_replacements(self, df, replacer_mapping={}, regex = False, field = ""):
        df = df.replace(replacer_mapping, regex = regex)
        return df

    # *******  *******   *******   *******   *******   *******   *******
    @logging_sampler
    def truncate_upto_last_column_trigger(self, df, last_column_trigger=None, drop_last = False, guide_row = 0, field = ""): # must be unique in the row
        first_row = df.iloc[guide_row].values.astype(str)
        mask = np.array(list(map(lambda x: bool(re.search(last_column_trigger, x)), first_row)))

        last_col_index = np.argwhere(mask == True)[-1][0] + 1
        if drop_last:
            last_col_index = last_col_index - 1

        df = df.iloc[:, :last_col_index].copy()
        return df

    # *******  *******   *******   *******   *******   *******   *******
    @logging_sampler
    def drop_columns_if(self, df, col_names: None|list = None, startswith: None|str|list = None, error_action = 'ignore', field = "", subfield = ""):

        df = df[df.columns[~df.columns.isna()]].copy() # added for DispatchSchedung Results, but does not harm

        if isinstance(col_names, list):
            df = df.drop(col_names, axis = 1,errors=error_action)


        if not startswith:
            return df

        if startswith:
            n_nan_cols = df.columns.isna().sum()
            non_nan_cols = df.columns[~df.columns.isna()].to_list()
            n_non_nan_cols = len(non_nan_cols)
            if n_nan_cols:
                if n_non_nan_cols:
                    self.warnings["warning {}".format(self.wc)] = {'type':"PartialNaNColumns",
                                                                  'date':self.date,
                                                                  'msg':'Tried to drop columns based on if they startwith "{}". '
                                                                        '\nBut df contained some ({} out of {}) nan column-names. Ignored them, and applied to the rest.'.format(startswith, n_nan_cols, df.shape[1])}
                    self.wc += 1
                    cols_to_apply_to = non_nan_cols

                else:
                    self.warnings["warning {}".format(self.wc)] = {'type': "FullyNaNColumns",
                                                                  'date': self.date,
                                                                  'msg': 'Tried to drop columns based on if they startwith "{}". '
                                                                         '\nBut df contained all nan column-names. Returned the original df.'.format( startswith)}
                    self.wc += 1
                    return df
            else:
                cols_to_apply_to = df.columns.to_list()

        if isinstance(startswith, str):
            startswith = [startswith]

        for sw in startswith:
            if sw:
                drop = [c for c in cols_to_apply_to if c.startswith(sw)]
                df = df.drop(drop, axis='columns', errors=error_action)


        return df

    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******
    @logging_sampler
    def dimensionalize_empty_df(self, df, new_columns: None|list = None, fill_values = 0):
        '''takes an empty df (that may or may not have the column-names information and returns a zero-filled df of compatible shape'''

        if isinstance(new_columns, type(None)):
            cols = df.columns
        else:
            cols = new_columns

        shape_x = len(self.period_dates)
        shape_y = len(cols)

        placeholder_data = np.empty((shape_x, shape_y))
        placeholder_data[:,:] = fill_values

        df = pd.DataFrame(columns=cols,
                          data = placeholder_data)

        return df
    # *******  *******   *******   *******   *******   *******   *******
    @logging_sampler
    def empty_df_handler(self, df, action_if_empty:None|str=None, arbitrary_columns:None|list=None, fill_value = 0, field = "", subfield = ""):

        if not action_if_empty:
            pass
        elif action_if_empty == 'return_empty':
            return pd.DataFrame()
        elif action_if_empty == 'return_compat':
            df = self.dimensionalize_empty_df(df, new_columns=arbitrary_columns, fill_values=fill_value)
        return df

    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******
    def _make_operators(self, start_incl:bool=True, end_incl:bool=True):
        if start_incl:
            start_operator = '>='
        else:
            start_operator = '>'

        if end_incl:
            end_operator = '<='
        else:
            end_operator = '<'

        if end_operator == '<=':
            resid_operator = '>'
        elif end_operator == '<':
            resid_operator = '>='

        return start_operator, end_operator, resid_operator

    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******
    @logging_sampler
    def _get_start_end_indices(self, df, start_cue:str=None, end_cue:str=None, field = "", subfield = ""):
        #todo: define a "QueNotFoundError"
        ''' rationale:
            if start_cue is None, use the first row as start. if not, search to find where the first row is.
            If end_cue is None, search to find the following empty cell of the indicator column
            if end_cue == last, go to the last index
            if end_cue == 'sth', go and search it.
            '''

        if not start_cue:
            id_start = 0
        else:
            start_locator = df[df.indicator == start_cue]
            if start_locator.empty:
                self.warnings["warning {}".format(self.wc)] = {'type': 'QueNotFound',
                                                      'date': self.date,
                                                      'msg': "\nStart-locator given was: '{}', but could not be found in indicator array: \n\t\t{}\n\n--> Returned None,None for start,end ids".format(
                                                          start_cue, df['indicator'].values)}
                self.wc += 1
                return None, None
            else:
                id_start = start_locator.index[0]

        if end_cue == 'last':
            id_end = df.index[-1]

        elif not end_cue: # search for the next nan you will find row-wise

            end_locator = df[df.indicator.isna()]
            if end_locator.empty: # this shouldn't happen. I am returning the last index anyway
                self.warnings["warning {}".format(self.wc)] = {'type':'QueNotFound',
                                                  'date':self.date,
                                                  'msg':"End-locator given was: None, but no NaNs were be found in indicator array: {}\n--> Returned the last index as end_index".format(df['indicator'].values)}
                self.wc += 1
                id_end = df.index[-1]

            else:
                id_end = df[df.indicator.isna()].index[0]

        else:
            end_locator = df[df.indicator == end_cue]
            if end_locator.empty:
                self.warnings["warning {}".format(self.wc)] = {'type':'QueNotFound',
                                                  'date':self.date,
                                                  'msg':"\nEnd-locator given was: '{}', but could not be found in indicator array: \n\t\t{}\n\n--> Returned None,None for start,end ids".format(end_cue, df['indicator'].values)}
                self.wc += 1
                return None, None

            else:
                id_end = end_locator.index[0]

        return id_start, id_end

    # *********************************************
    @logging_sampler
    def transposer(self, df, skip_rows_at_final:int=1, field = "", subfield = ""):

        df = df.T.copy().reset_index(drop=True)
        df.columns = df.iloc[max(skip_rows_at_final - 1, 0)].values
        df = df.iloc[skip_rows_at_final:].copy().reset_index(drop=True)

        return df

    # *********************************************
    @logging_sampler
    def col_renamer(self, df, renamer_mapping={}, field = "", subfield = ""):
        df = df.rename(renamer_mapping, axis=1, errors='ignore')
        return df

    # *********************************************
    @logging_sampler
    def apply_datetime_index(self, df, period_dates=[]):
        try:
            df.index = period_dates
        except:
            df = self.handle_irregular(df, period_dates)
            try:
                df.index = period_dates
            except:
                try:
                    # xbid on 2024-03-10 is fucked up. Applied this general-looking fi
                    df = df.drop(index=df.index[df.index.duplicated(keep='first')])
                    df = df.reindex(period_dates)
                except:

                    logger.error("Could not attach period_dates to dataframe, even after handling irregular")
                    logger.error(f"Period dates: (shape = {period_dates.size}). {period_dates = }")
                    logger.error(f'Dataframe size: {df.shape}')
                    logger.error(f'Dataframe:\n\n{df}')

        return df

    # *********************************************
    @logging_sampler
    def handle_irregular(self, df, period_dates=[] ):

        timestep_in_hours = period_dates[1] - period_dates[0]
        regular_day_timesteps = int(pd.Timedelta(1,'D') / timestep_in_hours)
        scale = int(regular_day_timesteps/24)

        slicing_hour = 3
        slicing_step = slicing_hour * scale

        if period_dates.shape[0] > df.shape[0]:
            # October dst, uncatched by report datafile
            df = pd.concat([df.iloc[:slicing_step], df.iloc[slicing_step:slicing_step + scale], df.iloc[slicing_step:]], axis = 0)

        elif period_dates.shape[0] < df.shape[0]:
            # March dst, uncatched by report datafile
            df = df.iloc[:-scale]

        return df