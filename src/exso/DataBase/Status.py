import datetime
import logging

import pandas as pd
from exso.IO.IO import IO
from exso.Utils.DateTime import DateTime
from exso.Utils.Misc import Misc
from exso.Utils.STR import STR

# *******  *******   *******   *******   *******   *******   *******
date_lambda = lambda x: datetime.datetime.strftime(DateTime.date_magician(x, return_stamp = False), format="%d-%b-%y")

# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
class Status:
    def __init__(self, dir, db_timezone, lake_inherent_tz, min_potential_datetime, max_potential_datetime, resolution):
        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.timezone = db_timezone
        self.dir = dir
        self.lake_inherent_tz = lake_inherent_tz
        self.min_potential_datetime = min_potential_datetime
        self.max_potential_datetime = max_potential_datetime
        self.resolution = resolution
        # self.period_covered = period_covered
        self.initialize()


    # *******  *******   *******   *******   *******   *******   *******
    def initialize(self):

        self.logger.info('\n-->Initializing Database ("{}")'.format(self.dir))
        self.logger.info('\t\tTarget/Current Timezone in Database: ' + self.timezone)

        self.up_to_date = False
        self.exists = False

        self.logger.info("Asserting database directory exists (creating if non-existing)")
        self.dir.mkdir(exist_ok=True, parents=True)
        self.init_dates()

    # *******  *******   *******   *******   *******   *******   *******
    def init_dates(self):
        dates = {'min':{'observed':{'date':None,
                                    'datetime':None},
                        'potential':{'date':self.min_potential_datetime.date(),
                                     'datetime':self.min_potential_datetime}},
                 'max':{'observed':{'date':None,
                                    'datetime':None},
                        'potential':{'date':self.max_potential_datetime.date(),
                                     'datetime':self.max_potential_datetime}},

                 # 'range':{'observed':{'date':pd.DatetimeIndex([]),
                 #                      'str':None},
                 #          'ideal':{'date':pd.date_range(self.min_potential_datetime.date(),
                 #                                        self.max_potential_datetime.date(),
                 #                                        freq=self.period_covered),
                 #                   'str':None},
                 #          },

                 'timezones': {'lake': self.lake_inherent_tz,
                               'base': self.timezone,
                               'current': None}
                 }

        self.dates = Misc.Dict2Object(dates)


    # *******  *******   *******   *******   *******   *******   *******
    def refresh(self, dir):

        self.sample_filepath = self.get_sample_filepath(dir)

        if self.sample_filepath:
            self.exists = True

            sample_df = IO.read_file(self.sample_filepath, timezone_as_read=self.timezone, convert_to_timezone=self.lake_inherent_tz)
            self.get_dates_info(sample_df)
            self.check_if_up_to_date()
        else:
            self.exists = False

    # *******  *******   *******   *******   *******   *******   *******
    def get_sample_filepath(self, dir):
        self.logger.info("\t\tAttempting to access a sample database file ('guide file'), in order to assess the data-range that is already stored in the Databse.")

        sample_file = None

        internal_files = list(dir.rglob('*.csv'))
        if internal_files:
            sample_file = internal_files[0]
            self.logger.info('\t\t\tFiles found within the check_directory: {}'.format(sample_file.parent))
            self.logger.info('\t\tControl Filepath: "{}"'.format(sample_file))
        else:
            self.logger.info('\t\tNo files or dirs detected within the database..')

        return sample_file

    # *******  *******   *******   *******   *******   *******   *******
    def get_dates_info(self, sample_df):
        # sample_df comes already converted to proper (consistent with lake-inherent timezone)

        min_observed_datetime_inherent_tz = sample_df.index[2] # would use zero, but crashes in multiindex. [1] doesn't hurt anyways
        max_observed_datetime_inherent_tz = sample_df.index[-1]

        if sample_df.columns[0].startswith('Unnamed'):
            daily = pd.DataFrame(index=sample_df.index.unique(), columns=['1','2'], data=0).resample('D').sum(numeric_only=True).tz_localize(None)
        else:
            daily = sample_df.resample('D').sum(numeric_only = True).tz_localize(None)
        date_range = daily.index
        start_date = date_range[0]
        end_date = date_range[-1]


        missing_days = daily[daily.index.to_series().diff() > pd.Timedelta(1,'D')]
        self.logger.info("\n\nMissing days: \n{}".format(missing_days))

        #everything in lake-inherent timezone
        dates_dict = {'min':{'observed':{'date':start_date,
                                         'datetime':min_observed_datetime_inherent_tz},
                             },
                      'max':{'observed':{'date':end_date,
                                         'datetime':max_observed_datetime_inherent_tz},
                             },
                      # 'range':{'observed':{'date':date_range}},
                      'timezones':{'current':sample_df.index.tz},
                      'missing':missing_days}

        self.dates.update(dates_dict)

        self.logger.info('Guide File Datetime Stats (lake-inherent timezone):\n\tStart Datetime: {}\n\tEnd Datetime: {}'.format(self.dates.min.observed.datetime, self.dates.max.observed.datetime))
        self.logger.info('ControlFile Date Stats (based on naive datetimes):\n\tStart Date: {}\n\t\tEnd Date: {}'.format(date_lambda(start_date), date_lambda(end_date)))
        self.logger.info('\nHead&Tail of control file:\n ' + STR.df_to_string(sample_df.head()) + '\n' + STR.df_to_string(sample_df.tail()))

    # *******  *******   *******   *******   *******   *******   *******
    def check_if_up_to_date(self):

        self.logger.info('Deciding whether database is up-to-date or not, using only the end_datetimes:')
        self.logger.info(
            '\tCache Database last observed datetime (inherent-tz):     {}'.format(self.dates.max.observed.datetime))
        self.logger.info(
            '\tCache Database maximum potential datetime (inherent-tz): {}'.format(self.dates.max.potential.datetime))


        max_date_check = pd.Timestamp(self.dates.max.observed.datetime) >= pd.Timestamp(self.dates.max.potential.datetime)
        min_date_check = pd.Timestamp(self.dates.min.observed.datetime) <= pd.Timestamp(self.dates.min.potential.datetime)
        if max_date_check and min_date_check:
        # if self.dates.max.observed.datetime  >= self.dates.max.potential.datetime:
            self.logger.info('--> The Cache   IS   up-to-date.')
            self.up_to_date = True
        else:
            self.logger.info('--> The Cache IS NOT up-to-date.')
            self.up_to_date = False

    # *******  *******   *******   *******   *******   *******   *******
    def check_if_complete(self, sample_df):
        pass
    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******

