import datetime
import logging
import re
import time

import numpy as np
import os
import pandas as pd
import sys
from colorama import Fore
from tqdm import tqdm


# ********   *    ********   *    ********   *    ********   *   ********
# ********   *    ********   *    ********   *    ********   *   ********
class Misc:
    @staticmethod
    def get_progress_bar(desc, total, iterable = None):

        tqdm._instances.clear()
        if isinstance(iterable,type(None)):

            pbar = tqdm(range(total), desc=desc, colour=Fore.WHITE, bar_format="{l_bar}%s{bar}%s{r_bar}" % (Fore.LIGHTCYAN_EX, Fore.LIGHTYELLOW_EX))
        else:
            pbar = tqdm(iterable, desc=desc, colour=Fore.WHITE,
                        bar_format="{l_bar}%s{bar}%s{r_bar}" % (Fore.LIGHTCYAN_EX, Fore.LIGHTYELLOW_EX))
        return pbar

    # ********   *    ********   *    ********   *    ********   *   ********
    # ********   *    ********   *    ********   *    ********   *   ********
    @staticmethod
    def getLogger(name, filepath, mode = 'a', level = logging.INFO):
        logger = logging.getLogger(name)
        filehandler = logging.FileHandler(filename=filepath, mode=mode)
        filehandler.setLevel(level)
        filehandler.setFormatter(logging.Formatter('[%(asctime)s-%(levelname)s]  %(name)s: %(message)s'))
        logger.addHandler(filehandler)
        return logger

    # ********   *    ********   *    ********   *    ********   *   ********
    # ********   *    ********   *    ********   *    ********   *   ********
    @staticmethod
    def get_logger(root_dir, level=20):
        logger = logging.getLogger(name=__name__)
        log_dir = os.path.join(root_dir, '_logfiles')
        os.makedirs(log_dir, exist_ok=True)

        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H_%M')

        logfile = os.path.join(log_dir, f'{now}.log')

        logging.basicConfig(filename=logfile,
                            level=level,
                            format='%(levelname)s: %(asctime)s %(message)s',
                            datefmt='%d/%m/%Y %H:%M',
                            filemode='w')
        return logger

        # logject = Log(logger, left_indent=24)
        # return logject

    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******
    class Dict2Object:
        def __init__(self, attr_dict):
            for k, v in attr_dict.items():
                if isinstance(v, dict):
                    self.__setattr__(k, Misc.Dict2Object(v))
                else:
                    self.__setattr__(k, v)

        # *******  *******   *******   *******   *******   *******   *******
        def __call__(self, *args, **kwargs):
            out = {}
            for k, v in self.__dict__.items():
                if isinstance(v, Misc.Dict2Object):
                    out[k] = v()
                else:
                    try:
                        out[k] = float(v)
                    except:
                        print(k)
                        print(v)
                        pass
            return out

        # *******  *******   *******   *******   *******   *******   *******
        def __repr__(self):
            return self.to_dict()

        # *******  *******   *******   *******   *******   *******   *******
        def __str__(self):
            return str(self.to_dict())

        # *******  *******   *******   *******   *******   *******   *******
        def to_dict(self, base_obj = None):
            if not base_obj:
                base_dict = self.__dict__
            else:
                base_dict = base_obj.__dict__

            t = {}
            for k, v in base_dict.items():
                if isinstance(v, Misc.Dict2Object):
                    t[k] = self.to_dict(v)
                else:
                    t[k] = v
            return t
        # *******  *******   *******   *******   *******   *******   *******
        def __getitem__(self, item):
            return self.__dict__[item]
        # *******  *******   *******   *******   *******   *******   *******
        def update(self, update_dict):
            for k,v in update_dict.items():
                if isinstance(v,dict):
                    self.__getattribute__(k).update(v)
                else:
                    self.__dict__[k] = v

        # *******  *******   *******   *******   *******   *******   *******

    # ********   *    ********   *    ********   *    ********   *   ********
    # ********   *    ********   *    ********   *    ********   *   ********
    @staticmethod
    def stopwatch(func):
        def wrap(*args, **kwargs):
            # print('Started at: {}'.format(datetime_lambda(datetime.datetime.now())))
            t = time.time()
            res = func(*args, **kwargs)
            print('\n\t\t' + '{}: Completed in ({}) sec'.format(func.__name__.title(), round(time.time() - t, 4)))
            # print(t2 + 'Ended at: {}'.format(datetime_lambda(datetime.datetime.now())))
            return res

        return wrap
    # ********   *    ********   *    ********   *    ********   *   ********
    @staticmethod
    def single_row_df_to_dict(df):

        if df.empty:
            print("Report type doesn not exist. Check typos.")
            sys.exit()

        assert df.shape[0] == 1
        dicted = dict(zip(df.columns.to_list(), df.values.flatten()))
        return dicted

    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def df_to_excel(df, filepath, aggregation = 'sum', freqs = ['H','D','M','Y'], default = 'sum'):

        '''
            freqs: list of valid pandas-frequencies: ['Y','M','D','H','30min', etc] or STR

            agggregation:
                         --> str ('mean','sum','min','max')
                            Then, all columns of the dataframe, will be resampled for each of the freqs,
                            according to this aggregation method.

                        --> single-depth dictionary per columns:
                            e.g. if a dataframe has columns: [load, price]
                            then, you can enter {'load':'sum', 'price':'mean'}
                            --> this will be applied for all frequencies

                            if some columns are not mentioned as keys, the default will be applied.
        '''
        if isinstance(aggregation,str):
            agg_dict = dict(zip(df.columns.to_list(),[aggregation]*df.shape[1]))
        else:
            agg_dict = aggregation.copy()
            keys = list(aggregation.keys())
            cols = df.columns.to_list()

            for c in cols:
                if c not in keys:
                    agg_dict[c] = default

        if isinstance(freqs,str):
            freqs = [freqs]

        sheet_names = {'Y':'Year', 'M':"Month",'D':'Day','H':'Hour'}
        with pd.ExcelWriter(filepath) as writer:
            for f in freqs:

                if f == 'H':
                    df_temporal = df.copy()
                else:
                    agg_keys = list(agg_dict.keys())
                    for k in agg_keys:
                        if k not in df.columns:
                            agg_dict.pop(k)
                    df_temporal = df.resample(f).agg(agg_dict)

                df_temporal.to_excel(writer, sheet_name=sheet_names[f])

    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def string_to_date(string, order='year-month-day', sep='/'):

        if sep != '':
            components = string.split(sep)
            for c in range(len(components)):
                if len(components[c]) == 1:
                    components[c] = '0' + components[c]

        else:
            components = [
             string[:4], string[4:6], string[6:]]

        components = list(map(lambda x: int(x), components))
        year, month, day = components
        return pd.Timestamp(year, month, day)

    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def conform_string_date(date):
        sep = ''
        for separator in ('.', '-', '/'):
            if re.search(separator, date):
                sep = separator

        if sep == '':
            return ( date, sep)
        components = date.split(sep)
        for c in range(len(components)):
            if len(components[c]) == 1:
                components[c] = '0' + components[c]
            return (sep.join(components), sep)


    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def bilateral_compare(d1, d2, get = ''):

        present_in_both = np.intersect1d(d1, d2)
        total_present = np.union1d(d1, d2)
        present_only_in_first = np.setdiff1d(d1,present_in_both)
        present_only_in_second = np.setdiff1d(d2,present_in_both)

        if get == 'first_but_not_second':
            return present_only_in_first
        elif get == 'second_but_not_first':
            return present_only_in_second
        elif get == 'union':
            return total_present
        elif get == 'intersection':
            return present_in_both
        # print("In Both (unique):", len(present_in_both))
        # print('Only In First:', len(present_only_in_first))
        # print('Only In second:', len(present_only_in_second))
        # print('Total:', len(total_present))

        # print(present_only_in_first)
        # print(present_only_in_second)

    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******
    class Infer_types_of_object:
        ''' takes a datafrmae and a trheshold.
            Finds columns that are of type object (if any)
            '''
        def __init__(self, df, thresh = 0.9):

            self.obj_cols = df.dtypes[df.dtypes == object].index.to_list()
            if not self.obj_cols:
                self.df = df
                self.unfloatable = pd.DataFrame()
                self.float_summary = pd.DataFrame()
            else:
                self.df = df.copy()
                self.dff = self.df[self.obj_cols].copy()

                self.float_summary = self.count_floats()

                self.unfloatable = self.float_summary[self.float_summary < thresh].dropna()

                self.convert_to_float(thresh=thresh)

                self.update_df()

        # *******  *******   *******   *******   *******   *******   *******
        def count_floats(self):
            ''' takes a dataframe and:
                first creates a boolean datafrmae, with values = True if the value [i,j] is float-compatible
                Then calculates for each column, the ratio of float-convertible / total'''
            is_float = self.dff.copy()
            is_float[self.obj_cols] = np.vectorize(self.vector_scan)(is_float[self.obj_cols])
            is_float = is_float.astype(int)

            not_float_slice = self.dff[is_float == False].dropna(how='all')

            float_summary = pd.DataFrame()
            float_summary['float_count'] = is_float.sum()
            float_summary['float_ratio'] = float_summary['float_count'] / is_float.shape[0]
            return float_summary

        # *******  *******   *******   *******   *******   *******   *******
        def convert_to_float(self, thresh=0.1):
            ''' This method force-converts each object column to float, if their ratio of float-convertible / total is > than the threshold.
                (non float-convertible values in majority-float-convertible columns are set to NaN
            '''

            convert_cols = self.float_summary.index[self.float_summary['float_ratio'] >= thresh].to_list()

            if convert_cols:
                self.dff[convert_cols] = np.vectorize(self.vector_scan)(self.dff[convert_cols], 'value')

            self.converted_cols = convert_cols

        # *******  *******   *******   *******   *******   *******   *******
        def update_df(self):
            self.df[self.converted_cols] = self.dff[self.converted_cols].values

        # *******  *******   *******   *******   *******   *******   *******
        @staticmethod
        def vector_scan(x, ret=bool):
            ''' takes a value x and tries to convert it to float
                If it makes it, then returns the float(x) or True, depending on ret argument
                If the float(x) throws an exception, it returns NaN or False depending on ret argument
                '''
            try:
                new_x = float(x)
                # if np.isnan(x): # if value is nan, force the exception: its not
                #     if ret ==bool:
                #         return True
                #     else:
                #         return np.nan
                    # sys.exit()
                if ret == bool:
                    return True
                else:
                    return new_x
            except:
                # the value could not be converted to float.
                if ret == bool:
                    return False
                else:
                    return np.nan

        # *******  *******   *******   *******   *******   *******   *******
        # *******  *******   *******   *******   *******   *******   ******

    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
