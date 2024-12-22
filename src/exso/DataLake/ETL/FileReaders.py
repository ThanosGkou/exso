import re
import traceback
import warnings

import openpyxl
import numpy as np, pandas as pd
from pathlib import Path
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
class Readers:
    @staticmethod
    def csv_reader(kwargs, filepath):
        if 'sheet_name' in kwargs:
            kwargs.pop('sheet_name')
        df = pd.read_csv(filepath, **kwargs)
        return {0:df}

    # *******  *******   *******   *******   *******   *******   *******
    @staticmethod
    def imbabe_reader(kwargs, filepath):

        df_sheets = pd.read_excel(filepath, **kwargs)
        # from 03-Apr-2023, added 5 lines on top. So, I cant read it correctly with header = 0 anymore.
        # I drop rows contaning less than 5 non-nan values
        df = df_sheets[0].dropna(axis='rows', thresh=5) #

        if df.iloc[0,0] == 'Period': # the header = 0 lead to unimportant row being the df columns
            df.columns = df.iloc[0,:]
            df = df.iloc[1:].reset_index(drop=True)
            df.index.name = None
            df.columns.name = None

        df_sheets[0] = df
        return df_sheets
    # *******  *******   *******   *******   *******   *******   *******

    @staticmethod
    def standard_reader(kwargs, filepath):
        ''' Future-proofing against:
            FutureWarning: Defining usecols with out of bounds indices is deprecated and will raise a ParserError in a future version.
        '''
        warnings.filterwarnings("ignore", category=UserWarning, module=re.escape('openpyxl.styles.stylesheet'))
        if 'IMBABE' in Path(filepath).name:
            df_sheets = Readers.imbabe_reader(kwargs, filepath)
            return df_sheets

        try:
            df_sheets = pd.read_excel(filepath, **kwargs) # at later pandas maybe this will sometimes break instead of warn
        except:

            try: # None reader can read xls and xlsx. openpyxl reader can only read xlsx
                engine_as_passed = kwargs['engine']
                kwargs['engine'] = None
                df_sheets = pd.read_excel(filepath, **kwargs)
            except:
                kwargs['engine'] = engine_as_passed

                usecols = kwargs['usecols']
                if usecols:
                    index_col = usecols[0] - 1
                    usecols = None
                else:
                    index_col = None
                kwargs['usecols'] = usecols
                kwargs['index_col'] = index_col
                try:
                    df_sheets = pd.read_excel(filepath, **kwargs)
                except:
                    warnings.warn("ERROR reading filepath")
                    print()
                    print(f'{filepath = }')
                    print(f'{kwargs = }')


        warnings.filterwarnings("default", category=UserWarning, module=re.escape('openpyxl.styles.stylesheet'))
        return df_sheets

    # *******  *******   *******   *******   *******   *******   *******
    @staticmethod
    def adhoc_reader(kwargs, filepath):

        def vector_eval(x):
            try:
                nominator, denominator = x[1:].split('/')
                denominator = re.sub(r'\^', '**', denominator)

                results = eval(nominator + '/' + denominator)
                return results
            except:
                return x

        sheet_locators = kwargs['sheet_name']
        skip_rows = kwargs['skiprows']
        use_cols = kwargs['usecols']
        header_row = kwargs['header']
        wb = openpyxl.load_workbook(filepath)
        dfs = {}
        for sheet_loc in sheet_locators:

            ws = wb[wb.sheetnames[sheet_loc]]
            df = pd.DataFrame(ws.values)

            if skip_rows:
                df = df.iloc[skip_rows:].copy()
            if use_cols:
                df = df.iloc[:, use_cols].copy()

            df.columns = df.iloc[header_row].copy()
            df = df.iloc[header_row + 1:].copy()
            if use_cols:
                df.iloc[:, use_cols] = np.vectorize(vector_eval)(df.iloc[:, use_cols])
            else:
                df.iloc[:, :] = np.vectorize(vector_eval)(df.iloc[:, :])

            df = df.reset_index(drop=True)
            df = Readers.correct_stupidity(df)

            df.index.name = None
            df.columns.name = None
            dfs[sheet_loc] = df.copy()

        return dfs

    # *******  *******   *******   *******   *******   *******   *******
    @staticmethod
    def correct_stupidity(df):
        #admie keeps on changing formats, headers, names, ...
        if df.isna().sum().sum() == 0:
            return df
        else:
            df = df.dropna(thresh=3, axis='rows')
            # put the first row to header, and remove completely one row
            df.columns = df.iloc[0]
            df = df.iloc[1:].copy().reset_index(drop = True)

            return df

    # *******  *******   *******   *******   *******   *******   *******
