import datetime
import logging
import re
import sys
import traceback

import numpy as np
import pandas as pd
import plotly.express
from exso.DataLake.Parsers.ParsersArch import Archetype
from exso.Utils.DateTime import DateTime

# *********************************************

date_lambda = lambda x: datetime.datetime.strftime(DateTime.date_magician(x, return_stamp = False), format="%d-%b-%y")
logger = logging.getLogger(__name__)

# *********************************************
def error_handler(func):
    def wrap(*args, **kwargs):

        text = ' '.join(func.__name__.split('_')[1:]) + ' ' + str(args) + ' | ' + str(kwargs)

        try:
            res = func(*args, **kwargs)
            return res

        except Exception:
            print('ERROR')
            print(text)
            logger.error(msg="\n\n\nError !!. info:\n\n" + text)
            logger.error(msg=traceback.format_exc())
            print('\t\t\tException was:\n\n' +   traceback.format_exc())
            raise InterruptedError

    return wrap

###############################################################################################
###############################################################################################
###############################################################################################
class ArchetypeWide(Archetype):
    # *******  *******   *******   *******   *******   *******   *******
    def param_setter(self):
        pass

    # *******  *******   *******   *******   *******   *******   *******
    def pre_proc(self, df):
        if self.replacer_mapping:
            df = self.apply_replacements(df, self.replacer_mapping, regex =False, field = self.field)

        if self.regex_mapping:
            df = self.apply_replacements(df, self.regex_mapping, regex =True, field = self.field)

        if self.dropna_settings:
            df = self.drop_nans(df, settings=self.dropna_settings, field=self.field)

        return df

    # *******  *******   *******   *******   *******   *******   *******
    def split_cue_points(self, df):
        return {self.field: df}

    # *******  *******   *******   *******   *******   *******   *******
    def transposeAll(self, subfields_dfs):
        return subfields_dfs

    # *******  *******   *******   *******   *******   *******   *******
    def rolling_post_proc(self, subfields_dfs):
        for subfield, df in subfields_dfs.items():

            df = self.apply_datetime_index(df, self.period_dates)

            if self.renamer_mapping:
                self.col_renamer(df, renamer_mapping = self.renamer_mapping, field = self.field, subfield = subfield)

            if self.drop_col_settings:
                df = self.drop_columns_if(df,
                                          col_names=self.drop_col_settings['col_names'],
                                          startswith=self.drop_col_settings['startswith'],
                                          error_action=self.drop_col_settings['error_action'],
                                          field=self.field,
                                          subfield=subfield)

            df = df.fillna(0)

            subfields_dfs[subfield] = df
        return subfields_dfs

    # *******  *******   *******   *******   *******   *******   *******
    def expost(self, subfields_dfs):
        return subfields_dfs

    # *******  *******   *******   *******   *******   *******   *******

###############################################################################################
###############################################################################################
###############################################################################################
class IMBABE(ArchetypeWide):
    def param_setter(self):
        self.drop_col_settings.update(col_names= ['Period', 'STARTDATE', 'ENDDATE'],
                                      startswith= 'Unnamed')

###############################################################################################
class BalancingEnergyProduct(IMBABE):
    pass

###############################################################################################
class BalancingCapacityProduct(IMBABE):
    pass

###############################################################################################
class RESMV(ArchetypeWide):
    def param_setter(self):
        self.renamer_mapping = {'ΗΜΕΡΑ':'Day', 'ΩΡΑ':'Hour',
                                'B/A ΕΝΕΡΓΕΙΑ KWh':'Biogas_MWh',  'B/A ΕΓΚ ΙΣΧΥΣ MW':'Biogas_MWinstalled',
                                'ΜΥΗΣ ΕΝΕΡΓΕΙΑ KWh':'SmallHydro_MWh', 'ΜΥΗΣ ΕΓΚ ΙΣΧΥΣ MW':'SmallHydro_MWinstalled',
                                'ΣΗΘΥΑ ΕΝΕΡΓΕΙΑ KWh': 'CHP_MWh', 'ΣΗΘΥΑ ΕΓΚ ΙΣΧΥΣ MW':'CHP_MWinstalled',
                                'Φ/Β ΕΝΕΡΓΕΙΑ KWh':'PV_MWh',  'Φ/Β ΕΓΚ ΙΣΧΥΣ MW':'PV_MWinstalled'}

        self.dropna_settings.update(how= None, thresh = 2, axis='rows')

    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******
    def pre_proc(self, df):

        df = df.rename(columns=self.renamer_mapping)

        df = df.dropna(axis = 'rows', thresh=2) # admie has 6-7 rows with text in first col, after all the date

        df[df.columns[df.columns.str.contains('MWh')]]/=1000

        return df

    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******
###############################################################################################
class HVCUSTCONS(ArchetypeWide):
    def param_setter(self):
        self.renamer_mapping = {'ΗΜΕΡΑ':'Day', 'ΩΡΑ':'Hour',
                                'ΚΑΤΑΝΑΛΩΣΗ ΠΕΛΑΤΩΝ ΥΤ (MWh)':'HV_Load',
                                'ΒΟΗΘΗΤΙΚΑ ΜΟΝΑΔΩΝ ΥΤ (MWh)': 'HV_AssistiveLoad'}


        self.dropna_settings.update(how= None, thresh = 2, axis='rows')

    # *******  *******   *******   *******   *******   *******   *******
    def pre_proc(self, df):

        df = df.rename(columns=self.renamer_mapping)

        df = df.dropna(axis = 'rows', thresh=2) # admie has 6-7 rows with text in first col, after all the date

        return df


    # *******  *******   *******   *******   *******   *******   *******
###############################################################################################
class RESMVLVPROD(ArchetypeWide):
    def param_setter(self):
        self.renamer_mapping = {'ΗΜΕΡΑ':'Day', 'ΩΡΑ':'Hour',
                                'ΕΝΕΡΓΕΙΑ (MWh)':'RESMVLV',}


        self.dropna_settings.update(how= None, thresh = 2, axis='rows')

    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******
    def pre_proc(self, df):

        df = df.rename(columns=self.renamer_mapping)

        df = df.dropna(axis = 'rows', thresh=2) # admie has 6-7 rows with text in first col, after all the date

        return df

    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******

###############################################################################################
###############################################################################################
###############################################################################################
class MonthlyNTC(ArchetypeWide):
    '''Incoming data looks something like this
        INTERCONNECTION :           Unnamed: 1     ALBANIA  Unnamed: 3 NORTH MACEDONIA  Unnamed: 5    BULGARIA  Unnamed: 7  Unnamed: 8      TURKEY       ITALY
0   From EXECUTION DATE :                  NaN  01.04.2024  22.04.2024      01.04.2024  22.04.2024  01.04.2024  22.04.2024  26.04.2024  01.04.2024  01.04.2024
1     To EXECUTION DATE :                  NaN  21.04.2024  30.04.2024      21.04.2024  30.04.2024  21.04.2024  25.04.2024  30.04.2024  30.04.2024  30.04.2024
2         From Hour (CET)        To Hour (CET)         NaN         NaN             NaN         NaN         NaN         NaN         NaN         NaN         NaN
3                00:00:00             01:00:00         300         400             300         500         600         700        1050          50         500
4                01:00:00             02:00:00         300         400             300         500         600         700        1050          50         500


    '''
    renamer_ = {'TEIAS': 'TURKEY',
                'FYROM': 'NORTH MACEDONIA'}
    # *******  *******   *******   *******   *******   *******   *******
    @staticmethod
    def conform_date_format(date:str, tzinfo = None):
        d, m, y = date.split('.')
        return pd.Timestamp(int(y), int(m), int(d),tzinfo=tzinfo)

    # *******  *******   *******   *******   *******   *******   *******
    @staticmethod
    def conform_borders(df):
        df = df.copy()
        df = df.drop(columns = df.columns[df.columns.str.startswith('Unnamed')])
        _undigitized = [c.split('.')[0].strip() for c in df.columns]
        borders = list(set(_undigitized))

        return borders

    # *******  *******   *******   *******   *******   *******   *******
    def pre_proc(self, df):
        orig = df.copy()
        df = df.iloc[:,2:].drop(index = 2)
        df = df.dropna(thresh = 2, axis = 'columns')
        df.index = ['start', 'end' ] + df.index[2:].to_list()
        for k,v in self.renamer_.items():
            df.columns = df.columns.str.replace(k,v)
        # rearranged = [[re.sub(k,v, string_col) for k,v in self.renamer_.items()] for string_col in df.columns]
        # df = df.rename(columns = self.renamer_).copy()

        borders = self.conform_borders(df)


        new_cols = []
        for i in range(df.shape[1]):
            orig_col = df.columns[i]
            if orig_col.startswith("Unnamed"):
                new_cols.append(new_cols[i-1] + f'_{i}')
            else:
                new_cols.append(orig_col)
        df.columns = new_cols

        outcome = pd.DataFrame(columns=borders, index=self.period_dates)
        if 'FYROM' in outcome.columns:
            input('GFLAKFNLJSANFLAJKn')

        for col in df.columns:
            relevant_border = [b for b in borders if b in col][0]
            try:
                applicable_from = self.conform_date_format(df.loc['start', col], tzinfo = self.period_dates.tzinfo)
            except:
                print()
                print('Error in date')

                print(df.loc['start'])
                print()
                print("original")
                print(orig)
                input('XXXXX')


            applicable_until = self.conform_date_format(df.loc['end', col], tzinfo=self.period_dates.tzinfo)
            applicable_until += pd.Timedelta('23h')
            # applicable_n_days = (applicable_until + pd.Timedelta('2h') - applicable_from).days
            if applicable_from == pd.Timestamp(2023, 9, 4, 0, 0, 0, tzinfo=self.period_dates.tzinfo):
                if applicable_until.month == 10:
                    applicable_from = pd.Timestamp(2023, 10,4 ,0,0,0,tzinfo=self.period_dates.tzinfo)

            applicable_n_days = (applicable_until + pd.Timedelta('2h') - applicable_from).days
            applicable_profile = df.loc[3:, col].values


            # applicable_n_days = int(outcome.loc[applicable_from:applicable_until].shape[0] / applicable_profile.size)

            fill_values = np.repeat(applicable_profile,applicable_n_days)
            s1 = fill_values.size
            s2 = outcome.loc[applicable_from:applicable_until].shape[0]

            if s2 == 0:
                # e.g. in the file specifying NTCs for Novermber 2023, values for italy are given for 01/Oct/23 until 31/Oct/23.
                # --> fucking ignore this
                continue
            if s1 + 20 < s2 or s1 -20 > s2:
                print('')
                print('The problem')
                print(orig)
                print()
                print(f'{applicable_from = }')
                print(f'{applicable_until = }')
                print(f'{applicable_n_days = }')
                print(f'{s1 = }')
                print(f'{s2 = }')

                input('=X')


            switches = DateTime.get_dst_switches(from_year=outcome.index[0].year, to_year=outcome.index[0].year
                                                 ,timezone='CET',return_datetime=True, keep='upstream')

            if outcome.loc[applicable_from:applicable_until].index.size != fill_values.size:
                correct_index = outcome.loc[applicable_from:applicable_until].index
                if correct_index.isin(switches.dst_switch).size > 0:
                    datetime_where_switch = correct_index[correct_index.isin(switches.dst_switch)][0]
                    arg_where_switch = np.argwhere(correct_index == datetime_where_switch)[0][0]

                    if correct_index.size > fill_values.size:
                        fill_values = np.concatenate([fill_values[:arg_where_switch], [fill_values[arg_where_switch]], fill_values[arg_where_switch:]])
                    else:
                        fill_values = np.concatenate([fill_values[:arg_where_switch], fill_values[arg_where_switch+1:]])
                else:
                    print()
                    print("ERROR")
                    print( f'{relevant_border = }, {col = }, {applicable_from = }, {applicable_until = }, {applicable_profile.size = }, {fill_values.shape = }')
                    raise ValueError("Uncompatible sizes in MonthlyNTC:")

            outcome.loc[applicable_from:applicable_until, relevant_border] = fill_values



        return outcome

    # *******  *******   *******   *******   *******   *******   *******
    def rolling_post_proc(self, subfields_dfs):
        for subfield, df in subfields_dfs.items():
            if self.renamer_mapping:
                self.col_renamer(df, renamer_mapping = self.renamer_mapping, field = self.field, subfield = subfield)

            df = df.astype(float).fillna(0)
            subfields_dfs[subfield] = df
        return subfields_dfs


###############################################################################################
###############################################################################################
###############################################################################################
class ENTSO(ArchetypeWide):
    # *******  *******   *******   *******   *******   *******   *******
    def param_setter(self):
        pass
    # *******  *******   *******   *******   *******   *******   *******
    def pre_proc(self, df):
        df.index = df.iloc[:,0]
        df = df.iloc[:, 1:].copy()
        df.index = pd.to_datetime(df.index)
        df = super().pre_proc(df)
        return df
    #     if self.replacer_mapping:
    #         df = self.apply_replacements(df, self.replacer_mapping, regex =False, field = self.field)
    #
    #     if self.regex_mapping:
    #         df = self.apply_replacements(df, self.regex_mapping, regex =True, field = self.field)
    #
    #     if self.dropna_settings:
    #         df = self.drop_nans(df, settings=self.dropna_settings, field=self.field)
    #
    #     return df

    # *******  *******   *******   *******   *******   *******   *******
    # def split_cue_points(self, df):
    #     return {self.field: df}

    # *******  *******   *******   *******   *******   *******   *******
    # def transposeAll(self, subfields_dfs):
    #     return subfields_dfs

    # *******  *******   *******   *******   *******   *******   *******
    def rolling_post_proc(self, subfields_dfs):
        for subfield, df in subfields_dfs.items():

            # df = self.apply_datetime_index(df, self.period_dates)

            if self.renamer_mapping:
                self.col_renamer(df, renamer_mapping = self.renamer_mapping, field = self.field, subfield = subfield)

            if self.drop_col_settings:
                df = self.drop_columns_if(df,
                                          col_names=self.drop_col_settings['col_names'],
                                          startswith=self.drop_col_settings['startswith'],
                                          error_action=self.drop_col_settings['error_action'],
                                          field=self.field,
                                          subfield=subfield)

            # df = df.fillna(0)

            subfields_dfs[subfield] = df
        return subfields_dfs

    # *******  *******   *******   *******   *******   *******   *******
    # def expost(self, subfields_dfs):
    #     return subfields_dfs
    # *******  *******   *******   *******   *******   *******   *******
###############################################################################################
###############################################################################################
###############################################################################################
class GenerationPerPlant(ENTSO):
    def pre_proc(self, df):
        df = df.iloc[2:].copy()
        df = super().pre_proc(df)
        return df
