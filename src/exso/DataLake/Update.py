import datetime
import logging
import sys

import numpy as np
import pandas as pd
from exso.DataLake.APIs.StreamHandler import StreamHandler
from exso.DataLake.Status import Status
from exso.Utils.DateTime import DateTime
from exso.Utils.Misc import Misc

date_lambda = lambda x: datetime.datetime.strftime(DateTime.date_magician(x, return_stamp = False), format="%d-%b-%y")
###############################################################################################
###############################################################################################
###############################################################################################
class Update:
    ''' This is the wrapper-class, containing the tools to update the datalake (download raw xls* or whatever datafiles)
        It's not a stand-alone class. It's inherited by the Datalake, which must have (and it has) a status attribute, instance of the Status class

        It can be instantiated directly, if provided with a status object

        Usage: Call the .update() method of the Datalake object
    '''

    def __init__(self, status, retroactive_update = False):
        ''' Be aware that this class is just inherited, not instantiated. This is just a placeholder for potential out-of-ordinary use that requires a direct instance of it
        '''
        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.status = status
        self.retroactive_update = retroactive_update

    # *******  *******   *******   *******   *******   *******   *******
    def update(self, start_date: None | str | datetime.datetime = None, end_date: None | str | datetime.datetime = None):

        self.udates = self.dates_pipeline(start_date, end_date) # zero-in on the actual days that require download


        if self.status.up_to_date:
            self.logger.info('Inside "update" function, inherited method of DataLake. But --> Datalake is up-to-date. No update required.')
            return

        else:
            self.__update_lake(suspend_stdout=False)
            lake_size_before = self.status.file_df.shape[0]
            self.status.refresh()
            lake_size_after = self.status.file_df.shape[0]
            self.n_files_added = lake_size_after - lake_size_before

        self.logger.info("Update completed. Files added: {}".format(self.n_files_added))

        if self.n_files_added == 0:
            self.logger.warning("\n\nThe update attempt (API call) resulted in ZERO new files in the datalake. The query is labeled as degenerate.")
            if self.status.up_to_date == False:
                self.logger.info("Probably due to mismatch of periof-covered, the datalake seems not to be up-to-date, but probably is.")
                self.logger.info("Setting it manually to be up-to-date.")
                self.status.up_to_date = True
        else:
            self.logger.info("\n\n {} new files added to the Datalake. Update was successful.".format(self.n_files_added))


    # *******  *******   *******   *******   *******   *******   *******
    def __update_lake(self, suspend_stdout=True):
        ''' to enter here, it means that the query is not degenerate.
            but maybe, e.g. the raw cache is up-to-date (manual download)
            but the merged cache doesnt exist, or you want overwrite, or it is not yet up-to-date
        '''

        if suspend_stdout:
            sys.stdout = None
        self.logger.info("Updating lake.")
        self.logger.info("Up-to-date? " + str(self.status.up_to_date))

        if not self.status.up_to_date or self.retroactive_update:
            self.logger.info('Making API call to download required dates. (from: {} to: {})'.format(self.udates.start, self.udates.end))

            api = StreamHandler(save_dir=self.status.dir)

            api.query(self.report_name, start_date=self.udates.start, end_date=self.udates.end,
                      publisher=self.publisher)
            sys.stdout = sys.__stdout__

    # *******  *******   *******   *******   *******   *******   *******
    def dates_pipeline(self, start_date, end_date):
        ''' This function derives, what the required date-range to download should be
        :param start_date:
        :param end_date:
        :return:
        '''

        start, end = self.filter_out_invalid(start_date, end_date)
        self.udates = self.__make_dates(start, end)

        if self.status.exists == False:
            self.logger.info("Datalake does not exist yet. Will download everything.")
            return self.udates

        if self.retroactive_update and self.r.is_alive and self.r.publisher == 'admie' and len(Status.history) == 0:
            self.logger.info("Check for new versions was True. The report is still alive, and it's not a henex-report")
            self.logger.info("So, the datalake updater will request for the whole date-range.")
            self.status.up_to_date = False
            return self.udates

        extra = self.intersect_with_existing(intermediate_missing='ignore')

        # print()
        # print(extra)
        # input('safasfasfasfasfasfa')
        if self.status.up_to_date:
            udates = {}
        else:
            start = DateTime.date_magician(extra[0])
            end = DateTime.date_magician(extra[-1])
            udates = self.__make_dates(start, end)

        self.udates = udates
        return udates

    # *******  *******   *******   *******   *******   *******   *******
    def filter_out_invalid(self, start_date, end_date):

        if not start_date:
            start = self.status.dates.min.potential.date
            self.logger.info("\tStart date given 'None': Replacing with min-potential ({})".format(start))
        else:
            # start = DateTime.date_magician(start_date, return_stamp=True)
            if type(start_date) != type(self.status.dates.min.potential.date):
                start_date = pd.Timestamp(start_date).to_pydatetime().date()
                min_potential = pd.Timestamp(self.status.dates.min.potential.date).date()
                self.status.dates.min.potential.date = min_potential

            if start_date < self.status.dates.min.potential.date:
                start = self.status.dates.min.potential.date
                self.logger.info("\tStart date given < min-potential: Replacing with min-potential ({})".format(start))
            else:
                start = start_date

        if not end_date:
            end = self.status.dates.max.potential.date
            self.logger.info("\tEnd date given 'None': Replacing with max-potential ({})".format(end))
        else:
            # end = DateTime.date_magician(end_date, return_stamp=True)
            if type(end_date) != type(self.status.dates.max.potential.date):
                end = pd.Timestamp(end_date).to_pydatetime().date()
                max_potential = pd.Timestamp(self.status.dates.max.potential.date).to_pydatetime().date()
                self.status.dates.max.potential.date = max_potential
            else:
                end = end_date

            if end > self.status.dates.max.potential.date:
                end = self.status.dates.max.potential.date
                self.logger.info("\tEnd date given > max-potential: Replacing with max-potential ({})".format(end))

        return start, end
    # *******  *******   *******   *******   *******   *******   *******
    def __make_dates(self, start, end, freq = '1D'):
        drange = pd.date_range(start, end, freq=freq)
        drange_str = list(map(lambda x: DateTime.make_string_date(x, sep=""), drange))

        dates = {'start': start,
                 'end': end,
                 'range': {'date': drange,
                           'str': np.array(drange_str)}}

        dates = Misc.Dict2Object(dates)
        return dates

    # *******  *******   *******   *******   *******   *******   *******
    def intersect_with_existing(self, intermediate_missing = 'ignore'):

        dates_in_lake = self.status.dates.range.observed.str
        dates_queried = self.udates.range.str

        extra = np.setdiff1d(dates_queried, dates_in_lake, assume_unique=True)
        self.logger.info("Unique files in lake: {}, Unique files queried: {}, Perceived extra: {}".format(len(dates_in_lake), len(dates_queried), len(extra)))

        if extra.size == 0:
            self.logger.info("Query is degenerate, lake seems to be up-to-date.")
            self.status.up_to_date = True

        elif intermediate_missing == 'ignore':
            if self.retroactive_update:
                self.logger.info("Not even assessing if the missing dates are known-to-be missing, since retroactive_update option is True.")
                return extra

            known_missing_dates = self.status.dates.missing.str
            extra = np.setdiff1d(extra, known_missing_dates)
            self.logger.info("\tIgnoring known-to-be-missing intermediate dates ({} in total)".format(len(known_missing_dates)))

            if len(extra) == 0:
                self.logger.info("Query is degenerate, lake seems to be up-to-date.")
                self.status.up_to_date = True

        else:
            pass
            #extra = np.setdiff1d(extra, known_missing_dates)
            #if extra.size == 0:
            #    self.status.up_to_date = True
            #    self.logger.info('Query is degenerate after all.')
            #else:
            #    self.logger.info("Query is not-degenerate. Will attempt to  download {} files".format(extra.size))

        return extra

    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******

