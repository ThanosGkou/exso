import datetime
import inspect
import re
import warnings
from functools import partial

import numpy as np
import pandas as pd


# ********   *    ********   *    ********   *    ********   *   ********
# ********   *    ********   *    ********   *    ********   *   ********
# ********   *    ********   *    ********   *    ********   *   ********
class DateTime:
    @staticmethod
    def two_digit_formatter(x):
        if len(x) == 2:
            return x
        return '0' + x
    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def make_string_date(datetime_obj, order='year-month-day', sep='/'):
        if isinstance(datetime_obj, list) or isinstance(datetime_obj, np.ndarray):
            outcome = list(map(lambda x: DateTime.make_string_date(x, order, sep), datetime_obj))
            if isinstance(datetime_obj, np.ndarray):
                outcome = np.array(outcome)
            return outcome

        year = str(datetime_obj.year)
        month = str(datetime_obj.month)
        day = str(datetime_obj.day)
        month = DateTime.two_digit_formatter(month)
        day = DateTime.two_digit_formatter(day)
        date = {'year':year,  'month':month,  'day':day}
        order = order.split('-')
        ymd = date[order[0]]
        for i in range(1, len(order)):
            date_obj = sep + date[order[i]]
            ymd += date_obj
        else:
            return ymd

    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def date_magician(date, return_stamp = False):

        if isinstance(date,type(None)):
            date = pd.Timestamp(datetime.datetime.now())
            if not return_stamp:
                date = date.date()
            return date
        elif isinstance(date,pd.Timestamp):
            if not return_stamp:
                date = date.date()
            return date

        elif isinstance(date,datetime.datetime) or isinstance(date, datetime.date):
            date = pd.Timestamp(date)
            if not return_stamp:
                date = date.date()
            return date

        elif isinstance(date,int) or np.issubdtype(type(date),np.integer):

            date = str(date)

            y = date[:4];m = date[4:6] ; d = date[6:]
            y = int(y) ; m = int(m) ; d = int(d)

            date = pd.Timestamp(y,m,d)
            if not return_stamp:
                date = date.date()
            return date

        elif isinstance(date,str):
            sep = ""
            for test_separator in ['/','-','.']:
                if re.search(test_separator,date):
                    sep = test_separator

            splitted = "-".join(date.split(sep))
            date = pd.Timestamp(splitted)
            if not return_stamp:
                date = date.date()

            return date

        elif isinstance(date, list) or isinstance(date,np.ndarray):

            dates = list(map(partial(DateTime.date_magician,return_stamp=return_stamp), date))

            return pd.to_datetime(dates)

        elif isinstance(date, pd.DatetimeIndex):
            if not return_stamp:
                date = date.date()

            return date
        else:
            return date

    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def dates_constructor(date, resolution, start_hour, period_covered, timezone):

        date_obj = DateTime.date_magician(date, return_stamp = True)

        start_datetime = date_obj
        start_datetime = start_datetime + pd.Timedelta(start_hour, 'h')

        period_covered = DateTime.disambiguate_timedelta(start_datetime, period_covered)

        end_datetime = start_datetime + pd.Timedelta(period_covered) - pd.Timedelta(resolution)
        end_datetime = end_datetime.round(resolution) # when period covered is month, it needs this.
        start_datetime = start_datetime.tz_localize(timezone)
        end_datetime = end_datetime.tz_localize(timezone)

        day_dates = pd.date_range(start_datetime, end_datetime, freq=resolution)
        return day_dates
    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def disambiguate_timedelta(start_date, dt, return_timedelta = False):
        ''' cannot handle e.g. 3M. It assumed, if it is month, it must be 1M'''

        time_entity = re.findall('[A-Za-z]', dt)
        time_entity = time_entity[0]
        if time_entity in ['h', 'D', 'W'] or 'min' in time_entity:
            new_timedelta = dt
        elif time_entity == 'M':
            # days_in_month = start_date.days_in_month
            days_in_month = pd.to_datetime(start_date).days_in_month
            new_timedelta = '{}D'.format(days_in_month)
        elif time_entity == 'Y':
            if start_date.is_leap_year:
                days_in_year = 366
            else:
                days_in_year = 365
            new_timedelta = '{}D'.format(days_in_year)

        if return_timedelta:
            new_timedelta = pd.Timedelta(new_timedelta)
        return new_timedelta

    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def get_dst_switches(from_year, to_year, return_datetime = False, timezone = None, keep = None):
        ''' if return_datetime == True, the timezone must not be None (neither UTC).

            keep = upstream, downstream or "both" (default)
        '''


        naive_dates = pd.date_range(pd.Timestamp(from_year,1,1,0,0,0),
                                    pd.Timestamp(to_year,12,31,23,0,0),
                                    freq='h')


        if not return_datetime:
            tz_ = 'EET'

        elif timezone == 'UTC':
            callerframerecord = inspect.stack()[1]
            frame = callerframerecord[0]
            info = inspect.getframeinfo(frame)
            warnings.showwarning("\nCannot have both timezone = UTC and return_datetime = True.", UserWarning,
                                 filename=info.filename, lineno=info.lineno)
            return_datetime = False
            timezone = 'EET'

            tz_ = timezone
        else:
            tz_ = timezone

        local_dates = naive_dates.tz_localize('UTC').tz_convert(tz_)

        masK_downstream = np.logical_not(local_dates.hour.to_series().diff().isin([1,-23])).values
        masK_downstream[0] = False # first occurence is 1st of January, not dsf switch
        mask_upstream = np.roll(masK_downstream, -1)  # roll back by one, to see the prior-to-switch datetime

        if not keep:
            keep = 'both'

        if return_datetime:

            if keep == 'both':
                datetime_downstream = local_dates[masK_downstream]
                datetime_upstream = local_dates[mask_upstream]
                datetime_switches = np.concatenate([datetime_upstream, datetime_downstream])
                datetime_switches = np.sort(datetime_switches)

            elif keep == 'upstream':
                datetime_upstream = local_dates[mask_upstream]
                datetime_switches = datetime_upstream

            elif keep == 'downstream':
                datetime_downstream = local_dates[masK_downstream]
                datetime_switches = datetime_downstream

            df = pd.DataFrame({'dst_switch':datetime_switches})
            df['Year'] = df.dst_switch.dt.year
            df['hour'] = df.dst_switch.dt.hour

        else:
            datetime_downstream = local_dates[masK_downstream]
            dates_switches = datetime_downstream.date # np array not pandas
            df = pd.DataFrame({'dst_switch':pd.to_datetime(dates_switches)})
            df['Year'] = df.dst_switch.dt.year

        df.set_index('Year',drop=True,inplace=True)
        return df
    pass


