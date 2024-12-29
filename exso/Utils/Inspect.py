from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


###############################################################################################
###############################################################################################
###############################################################################################
class Inspect:
    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def scan_data_file(filepath, tzone = 'EET'):
        from exso.Utils.DateTime import DateTime
        from exso.Utils.STR import STR

        filepath = Path(filepath)

        filetype = filepath.suffix
        if filetype == '.csv':
            df = pd.read_csv(filepath,header=0,index_col=0)
            df.index = pd.to_datetime(df.index)
        else:
            df = pd.read_excel(filepath,header=0,index_col=0)

        df.index = df.index.round('min')
        freq = df.index[1] - df.index[0]
        if freq < pd.Timedelta('1D'):
            df = df.tz_localize(tzone,ambiguous='infer')

        start_date = df.index[0]
        end_date = df.index[-1]

        ideal_dates = pd.date_range(start_date,
                                    end_date,
                                    freq=freq)

        next_datetime = end_date + pd.Timedelta(freq)

        print("\t\t Scanning Datafile {}".format(filepath))
        print('\t\t\tShape: ', df.shape)
        print('\t\t\tStart Date: ', start_date)
        print('\t\t\tEnd Date: ', end_date)
        print('\t\t\tFrequency: ', freq)
        print('\t\t\tDuplicate Indices:')
        print(STR.df_to_string(df[df.index.duplicated(keep=False)], indent=10))

        print('\t\t\tDST swithces within the data files time period:')
        print(STR.df_to_string(DateTime.get_dst_switches(from_year=start_date.year, to_year=end_date.year, return_datetime=False, timezone=tzone,keep = 'both'),indent=10))


        perfect = len(ideal_dates) - df.shape[0]
        if perfect == 0:
            print('No missing records.')
        else:
            print('\t\t\tMissing records:', perfect)
            print('\t\t\t(Positive means, less records than ideal, negative means more records than ideal.')

        if perfect != 0:
            print('\t\t\tIndex where, timedelta is not equal to frequency')
            print('\t\t\t\tThis is to be interpreted as following:')
            print('\t\t\t\tThere are missing dates, BEFORE the index that is printed. Duration of missing dates is indicated with the "diff" column.')

            dftemp = df.copy()
            dftemp['diff'] = df.index.to_series().diff()
            dftemp = dftemp.iloc[1:]

            display_df = dftemp[dftemp['diff'] != freq][['diff']].copy()
            display_df['diff'] -= freq
            print(STR.df_to_string(display_df, indent=10))


        return start_date,end_date,freq,next_datetime

    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def nan_analysis(df, col):

        df['conseq_nans'] = df[col].isnull().astype(int).groupby(df[col].notnull().astype(int).cumsum()).cumsum()

        df_plot = df.copy()
        df_plot.set_index('datetime', inplace = True, drop = True)
        df_plot = df_plot.resample('D').max()

        july.heatmap(dates=df_plot.index, data=df_plot['conseq_nans'].values,date_label=True,weekday_label=True,month_label=True,year_label=True,month_grid=True,colorbar=True)
        plt.title("Number of consecutive NaNs within day")
        plt.show()

        nan_dataframe = df[df.conseq_nans > 0].copy()
        nan_dataframe['change'] = (nan_dataframe['conseq_nans'] - nan_dataframe['conseq_nans'].shift(-1)) < 0

        res_c = pd.DataFrame(nan_dataframe['change'][nan_dataframe['change'] == False])
        res_c['unique'] = res_c.index.to_series().diff()

        conseq_nans = []
        for i in res_c.index:
            conseqs = nan_dataframe.loc[i]['conseq_nans']
            conseq_nans.append(conseqs)

        nan_freq = pd.DataFrame({'how_many_nans_in_row':conseq_nans})
        nan_freq = nan_freq.value_counts()
        nan_freq = pd.DataFrame({'how_often':nan_freq.values}, index=nan_freq.index)

        total_nans = df[col].isna().sum()

        nan_dataframe = df[df.conseq_nans > 0].copy()

        nan_dataframe.set_index('datetime', inplace = True, drop = True)

        return {'total_nans': total_nans, 'max_consecutive_nans':df['conseq_nans'].max(), 'conseq_nan_frequency': nan_freq, 'nan_data': nan_dataframe}


