import datetime
import logging
import os
import re
import sys

# import tabula
import numpy as np
import pandas as pd
import tqdm
from colorama import Fore
from exso.Utils import Misc, DateTime
''' This is wip. Should not be used. Only works partially'''
# *******  *******   *******   *******   *******   *******   *******
date_lambda = lambda x: datetime.datetime.strftime(DateTime.DateTime.date_magician(x, return_stamp = False), format="%d-%b-%y")
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
class UnitsMaintenanceSchedule:

    def __init__(self, datalake, base_dir):
        self.logger = logging.getLogger()

        self.lake = datalake
        self.base_dir = base_dir
        self.file_df = datalake.status.file_df

        self.file_df['month'] = self.file_df['dates'].dt.month
        self.file_df['year'] = self.file_df['dates'].dt.year
        df = self.file_df.copy()
        most_recent_dates = df.groupby('year')['dates'].max()

        self.file_df = self.file_df[self.file_df.dates.isin(most_recent_dates.values)].copy()
        self.file_df = self.file_df.iloc[1:]

        self.readAll()
        self.transformAll()


        input('here')
        self.results_dir = results_dir
        self.dates_and_files = self.preproc.files_to_read
        self.dates_constructor()
        os.makedirs(results_dir,exist_ok=True)

        self.start_date = self.preproc.start_date_obj
        self.end_date = self.preproc.end_date_obj

        df = self.parse_single(self.dates_and_files[0][1], self.dates_and_files[0][1]) # only the latest
        df_dense = self.sparse_to_dense(df)


        self.export(df_dense, csv=True, year=df_dense.index[0].year)

    # *******  *******   *******   *******   *******   *******   *******
    def readAll(self):

        self.failed_dates = []
        in_memory = {}
        tqdm.tqdm._instances.clear()
        pbar = tqdm.tqdm(range(self.file_df.shape[0]), desc='Reading Progress', colour='white',
                            bar_format="{l_bar}%s{bar}%s{r_bar}" % (Fore.LIGHTCYAN_EX, Fore.LIGHTYELLOW_EX))

        for i in pbar:
            str_date = self.file_df.index[i]
            pbar.set_postfix_str(s="Reached: {}".format(date_lambda(str_date)))

            filepath = self.file_df.loc[str_date, 'filepaths']
            in_memory[str_date] = self.reader(filepath)

        self.data1 = in_memory

    # *******  *******   *******   *******   *******   *******   *******

    def reader(self, filepath):
        table = tabula.read_pdf(filepath, pages=1)[0]
        return table

    # *******  *******   *******   *******   *******   *******   *******
    def transformAll(self):
        tqdm.tqdm._instances.clear()
        pbar = tqdm.tqdm(range(self.file_df.shape[0]), desc='Parsing Progress', colour='white',
                         bar_format="{l_bar}%s{bar}%s{r_bar}" % (Fore.LIGHTCYAN_EX, Fore.LIGHTYELLOW_EX))
        self.data2 = {}
        for i in pbar:
            str_date = self.file_df.index[i]
            pbar.set_postfix_str(s=date_lambda(str_date))
            df = self.data1[str_date]
            self.data2[str_date] = self.transformer(df)

    # *******  *******   *******   *******   *******   *******   *******
    def transformer(self, df):

        df = df.dropna(axis=0, how='all')
        while 'ID' not in df.columns:
            df = pd.DataFrame(df.values[1:], columns=df.iloc[0]).reset_index(drop=True)

        df = df.drop(index = df[df['ID'].isna()].index)
        df = df.drop(index = df[df['UNIT'] == 'NO MAINTENANCE'].index)

        orig = df.copy()
        try:

            df = df.rename(columns={'CC\rRange': 'CC_range',
                                    'CC Range': 'CC_range'})

            col_indix_merge = [5,6,7,8,9,10]
            all_text = df.iloc[:, col_indix_merge].apply(lambda row: '_'.join(row.values.astype(str)), axis=1).values

            duration_days = list(map(lambda x: re.findall(r'\d+(?= )', x), all_text)) # digits followed by space
            duration_days = list(map(lambda x: int(x[0]) if len(x) > 0 else np.nan, duration_days))
            start_end_dates = list(map(lambda x: re.findall(r'\d+/\d+/\d+', x), all_text))

            start_dates = [x[0] if len(x) > 0 else pd.NaT for x in start_end_dates]
            end_dates = [x[1] if len(x) ==2 else pd.NaT for x in start_end_dates ]

            df['Start'] = start_dates
            df['Finish'] = end_dates
            df['Duration'] = duration_days

            df = df[['ID', 'UNIT', 'MW', 'CC_range', 'N/M', 'Duration', 'Start', 'Finish']].copy()

            df = df.rename(columns={'Duration': 'Duration_days'})
            df = df.replace('MEGALOPOLI5', 'MEGALOPOLI_V')
            df = df.replace('KORINTHOS POWER', 'KORINTHOS_POWER')
            df = df.set_index('ID')

        except Exception as ex:
            print()
            print('Failed')
            print('Original data')
            print(orig)
            print()
            print('Final data')
            print(df)
            print()
            print('All text')
            print(all_text)
            print()
            print('\durations')
            print(duration_days)
            print()
            print('\Startends')
            print(start_end_dates)
            print()
            print('\Start dates')
            print(start_dates)
            print()
            print('\End dates')
            print(end_dates)
            # print('Original Columns')
            # for c in orig.columns:
            #     print('\t' + '"' + str(c) + '"')
            # print('\n\n')
            # print('Final  Columns')
            # for c in df.columns:
            #     print('\t' + '"' + str(c) + '"')

            sys.exit()

        return df


    # *******  *******   *******   *******   *******   *******   *******
    def joinAll(self):
        pass

    ################################################################################################ Merge and Post-Proc
    def export(self, df, subdir = None, csv = True, year = 2022):

        if subdir:
            results_dir = os.path.join(self.results_dir,subdir)
            os.makedirs(results_dir,exist_ok=True)
        else:
            results_dir = self.results_dir

            try:
                file = os.path.join(results_dir, 'UnitsMaintenanceSchedule_{}'.format(year) + '.csv')
                # df = v.tz_localize(None)
                if csv:
                    df.to_csv(file)
                else:
                    df.to_excel(file)

            except Exception as ex:
                print(n,star,' I/O Error ')
                print('Error occured while saving {}'.format(file))
                print('Exception was:', ex)
                # print('Shape of df = ', v.shape, '  or', v.to_numpy().reshape(-1,48).shape)
                # print('Shape of local dates:', self.local_dates.shape)
                print(star,n)

    ################################################################################################ Merge and Post-Proc
    def dates_constructor(self):

        self.start_hour = 0
        dates_as_built = []
        daily_dates = {}
        for i in range(len(self.dates_and_files)):
            str_date = self.dates_and_files[i][0]
            day_dates = self.day_dates_constructor(str_date)
            dates_as_built = np.concatenate([dates_as_built, day_dates])
            daily_dates[str_date] = day_dates

        self.daily_dates = daily_dates
        self.isp_local_dates = dates_as_built

    ################################################################################################ Merge and Post-Proc
    def day_dates_constructor(self, date):
        date_obj = Misc.date_magician(date) + pd.Timedelta(self.start_hour, 'h')
        add_hours = 23 - self.start_hour
        start_d = date_obj.tz_localize('Europe/Athens')
        end_d = date_obj + pd.Timedelta(add_hours, 'h')
        end_d = end_d.tz_localize('Europe/Athens')
        day_dates = pd.date_range(start_d, end_d, freq='H')
        return day_dates

    ################################################################################################ Merge and Post-Proc
    def parse_single(self, strdate, file):
        ''' only works after 2021'''

        table = tabula.read_pdf(file, pages=1)[0]
        table = table.dropna(axis=0, how='all')
        while 'ID' not in table.columns:
            table = pd.DataFrame(table.values[1:], columns=table.iloc[0]).reset_index(drop=True)

        try:
            if 'Duration Start' in table.columns:
                new_cols = table['Duration Start'].str.split('days',expand = True)
                table['Duration']  = new_cols[0]
                table['Start'] = new_cols[1]

            table = table.rename(columns={'CC\rRange':'CC_range',
                                          'CC Range':'CC_range'})
            table = table[['ID', 'UNIT', 'MW', 'CC_range', 'N/M', 'Duration', 'Start', 'Finish']].copy()

            table = table.rename(columns={'Duration': 'Duration_days'})
            table = table.replace('MEGALOPOLI5','MEGALOPOLI_V')
            table = table.replace('KORINTHOS POWER','KORINTHOS_POWER')
            table = table.iloc[:-1].copy()

            table['Start'] = pd.to_datetime(table['Start'])
            table['Finish'] = pd.to_datetime(table['Finish'])
            table['Duration_days'] = table['Duration_days'].str.extract("(\d+)")#.astype(int)
            table= table.set_index('ID')
        except Exception as ex:
            print(table['Duration Start'])

            # print(ex)
            # print('ERROR')
            # print(table)
            sys.exit()

        return table

    ################################################################################################ Merge and Post-Proc
    def sparse_to_dense(self, df):

        start_year = self.start_date.year
        start_date = pd.Timestamp(start_year,1,1)
        end_date =  pd.Timestamp(start_year,12,31)
        year_dates = pd.date_range(start_date,end_date,freq='D')

        df = df.set_index('UNIT')

        nom_capacities = pd.read_excel(r'C:\Users\TNATSIKAS\Desktop\GT-Local\2. Data\1. Electricity\1. Greece\3. Generators\_lib\BalancingServiceEntities.xlsx',usecols=[4,5,6],sheet_name='Dispatchable Units')
        nom_capacities = nom_capacities[nom_capacities['Type'].isin(['Lignite','Natural Gas'])].copy()

        no_maintenance = pd.DataFrame({'dummy':np.zeros(year_dates.size)})

        bridge = nom_capacities[['ISP_names','MW']].copy().set_index('ISP_names').T
        for c in bridge.columns:
            no_maintenance[c] = bridge[c].values[0]
        no_maintenance.pop('dummy')
        no_maintenance.index = year_dates
        with_maintenance = no_maintenance.copy()
        print(nom_capacities)
        for c in no_maintenance.columns:
            if c in df.index:
                print()
                print('UNIT = ', c)

                unit_nominal = nom_capacities[nom_capacities.ISP_names == c]['MW'].values[0]
                print('Nominal Capacity = ', unit_nominal, 'MW')
                try:
                    unit_maint = df[df.index == c].copy()

                    for i in range(unit_maint.shape[0]):
                        print('\tPass = ', i+1)
                        specific_maint = unit_maint.iloc[i]
                        duration = specific_maint.loc['Duration_days']
                        start = specific_maint.loc['Start']
                        end = specific_maint.loc['Finish']
                        power_at_maint = specific_maint.loc['MW']
                        power_at_maint = re.sub(',','.',power_at_maint)
                        power_at_maint = float(power_at_maint)

                        print('\t\tperiod = ', start.date(), 'to', end.date())
                        print('\t\tcapacity Off = ', power_at_maint, 'MW')
                        print('\t\tduration = ', duration)
                        if duration == 0 or end < year_dates[0]:
                            print('\t\t\t--> Outside scope')
                            pass
                        else:

                            with_maintenance.loc[((with_maintenance.index >= start)&
                                                  (with_maintenance.index <= end)),c] = unit_nominal -power_at_maint

                except Exception as ex:
                    print('ERROR at ', c)
                    print(ex)
                    sys.exit()


            else:
                print()
                print('Unit ', c, ' --> not found')

        return with_maintenance



    ################################################################################################ Merge and Post-Proc
    def read_and_merge(self):

        print('Merging...')




        return df
    ################################################################################################ Merge and Post-Proc

