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

        orig_dates = self.period_dates.copy()
        orig_timedelta = orig_dates[2] - orig_dates[1]
        dates_r2 = pd.date_range(self.period_dates[0],
                                 self.period_dates[-1] + orig_timedelta - pd.Timedelta(self.r2),
                                 freq=self.r2)

        for subfield, df in subfields_dfs.items():
            if self.period_dates.size + 20 < df.shape[0]:
                if isinstance(self.r2, str):
                    apply_dates = dates_r2
                else:
                    raise ValueError
            else:
                apply_dates = orig_dates

            df = self.apply_datetime_index(df, apply_dates)

            if self.renamer_mapping:
                df = self.col_renamer(df, renamer_mapping = self.renamer_mapping, field = self.field, subfield = subfield)

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
                'FYROM': 'NORTH MACEDONIA',
                # they are not identical. the latter has some turkish characters
                'TURKIYE':'TURKEY',
                'TURKΙΥΕ': 'TURKEY',}

    # issue in file: 20250601_20250630_MonthlyNTC_01.xlsx, column M
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
            raise ValueError("value 'FYROM' encountered, while it should have been properly handled at this point.\n"
                             f"Date: {self.period_dates[0]}, Field: {self.field}")

        # Why it doesnt fucking work with a dict?

        df = self.fix_the_mess_in_dates(df, borders)

        for col in df.columns:
            relevant_border = [b for b in borders if b in col][0]
            applicable_from = df.loc['start', col]
            applicable_until = df.loc['end', col]

            applicable_n_days = (applicable_until + pd.Timedelta('2h') - applicable_from).days
            applicable_profile = df.loc[3:, col].values

            fill_values = np.repeat(applicable_profile,applicable_n_days)

            size_of_values = fill_values.size
            size_of_index = outcome.loc[applicable_from:applicable_until].shape[0]

            if size_of_index == 0:
                # e.g. in the file specifying NTCs for Novermber 2023, values for italy are given for 01/Oct/23 until 31/Oct/23.
                # --> fucking ignore this
                continue
            if size_of_values + 20 < size_of_index or size_of_values -20 > size_of_index:
                print('')
                print('The problem')
                print(orig)
                print()
                print(f'{applicable_from = }')
                print(f'{applicable_until = }')
                print(f'{applicable_n_days = }')
                print()
                print(f'{size_of_values = }')
                print(f'{size_of_index = }')
                print()
                print(outcome)
                raise ValueError

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
    def fix_the_mess_in_dates(self, df, borders):
        verified_period_start = self.period_dates[0]
        tz = self.period_dates[0].tzinfo
        dt_end_of_day = pd.Timedelta('23h')

        ###############################
        ############################### ADHOC ISSUES
        ###############################
        # June 2025
        df = df.replace('15.06.1026', '15.06.2026')
        df = df.replace('15.06.1025', '15.06.2025')

        # November 2025: ALBANIA start = 01.11.2025, end = 01.30.2025 ...
        df = df.replace('01.30.2025', '30.11.2025')
        df.loc['start'] = [self.conform_date_format(d, tzinfo = tz) for d in df.loc['start']]
        df.loc['end'] = [self.conform_date_format(d, tzinfo=tz) + dt_end_of_day for d in df.loc['end']]

        ###############################
        ############################### END OF ADHOC ISSUES
        ###############################

        _starters = pd.to_datetime(df.loc['start'])
        _enders = pd.to_datetime(df.loc['end'])

        start_issues = df.loc['start'][(_starters.dt.month != verified_period_start.month) | (_starters.dt.year != verified_period_start.year)]
        end_issues = df.loc['end'][(_enders.dt.month != verified_period_start.month) | (_enders.dt.year != verified_period_start.year)]

        # start_issues = df.loc['start'][pd.to_datetime(df.loc['start']).dt.month != verified_period_start.month]
        # end_issues = df.loc['end'][pd.to_datetime(df.loc['end']).dt.month != verified_period_start.month]

        if not (start_issues.empty):# and end_issues.empty):
            logger.warning(f"Start Issues encountered in {verified_period_start}")
            logger.warning(df.head())
            logger.warning(f'\n{start_issues}\n')
            bad = df.loc['start', start_issues.index]
            fixed = [pd.Timestamp(year=verified_period_start.year,
                                  month=verified_period_start.month,
                                  day = d.day, tzinfo=tz) for d in bad]
            df.loc['start', start_issues.index] = fixed

        if not end_issues.empty:
            logger.warning(f"End Issues encountered in {verified_period_start}")
            logger.warning(df.head())
            logger.warning(f'\n{end_issues}\n')

            bad = df.loc['end', end_issues.index]
            fixed = [pd.Timestamp(year=verified_period_start.year,
                                  month=verified_period_start.month,
                                  day=d.day, hour = 23, tzinfo=tz) for d in bad]


            df.loc['end', end_issues.index] = fixed

        for col in df.columns:
            start = df.loc['start', col]
            end = df.loc['end', col]
            if start > end:
                df.loc['end', col] = start + dt_end_of_day

        return df

    # *******  *******   *******   *******   *******   *******   *******
    def rolling_post_proc(self, subfields_dfs):
        for subfield, df in subfields_dfs.items():
            if self.renamer_mapping:
                self.col_renamer(df, renamer_mapping = self.renamer_mapping, field = self.field, subfield = subfield)

            df = df.astype(float).fillna(0)
            subfields_dfs[subfield] = df
        return subfields_dfs

class DailyAuctionsSpecificationsATC(ArchetypeWide):
    @staticmethod
    def parse_ATC(lake, dates):

        all_files = list(lake.status.dir.glob('[!~]*.xls*'))
        all_dates_str = [f.name.split('_')[0] for f in all_files]

        counts = pd.Series(all_dates_str).value_counts(sort=False)
        _files_df = pd.DataFrame(data={'dates_str': all_dates_str,
                                       'filepath': all_files})

        # In general, each str-date has 2 file versions, which actually are one for imports one for exports
        # For some dates (e.g. 2020-07-15) there may be not 2 but 4 file versions
        # I dont care: while looping, the last-dates will overwrite the first ones in the dict
        try:
            assert all(counts == 2)
        except:
            logging.warning(msg='More than two versions for ATC files:')
            problematic_str_date = counts[counts > 2].index
            logging.warning(_files_df[_files_df.dates_str.isin(problematic_str_date)]['filepath'].values)

        dfs = {'Imports': {},
               'Exports': {}}
        for d in dates:
            str_date = DateTime.make_string_date(d, sep='')
            datetime = DateTime.date_magician(str_date, return_stamp=True)
            candidates = _files_df[_files_df.dates_str == str_date]
            d_datetime = pd.date_range(datetime, datetime + pd.Timedelta('23h'), freq='h', tz='CET')

            if candidates.empty:
                dfs['Imports'][str_date] = pd.DataFrame(index=d_datetime)
                dfs['Exports'][str_date] = pd.DataFrame(index=d_datetime)

                continue

            for fp in candidates['filepath'].values:
                df = pd.read_excel(fp, header=0)
                description = df.columns[0]
                print()
                print(df.head())
                print(df.shape)
                df.columns = df.iloc[1, :].values
                df = df.iloc[2:, 1:].reset_index(drop=True).dropna(axis='rows', how='all')
                if df.shape[0] == 25:
                    # there is a 'total' row
                    df = df.iloc[:24]

                try:
                    df.index = d_datetime

                except:
                    print('\n\n')
                    print(f'Failed to assign df index at: {str_date}\n'
                          f'{description = }')
                    input('??')
                if 'IMP' in description:
                    dfs['Imports'][str_date] = df
                elif 'EXP' in description:

                    dfs['Exports'][str_date] = df

        final = {}
        for direction, dicts in dfs.items():
            direction_df = pd.DataFrame()
            for str_date, df in dicts.items():
                direction_df = pd.concat([direction_df, df], axis=0)

            final[direction] = direction_df

        final = {'ATC': final}
        return final

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
