import datetime
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from exso.DataLake.APIs import ADMIE, HEnEx
from exso.DataLake.APIs import HEnExArchives
from exso.Utils.DateTime import DateTime
from exso.Utils.Misc import Misc

# *******  *******   *******   *******   *******   *******   *******
date_lambda = lambda x: datetime.datetime.strftime(DateTime.date_magician(x, return_stamp = False), format="%d-%b-%y")

# *******  *******   *******   *******   *******   *******   *******

def date_wrapper(func):
    def wrap(*args, **kwargs):
        kwargs.update(start_date = DateTime.date_magician(kwargs['start_date']))
        kwargs.update(end_date = DateTime.date_magician(kwargs['end_date']))
        res = func(*args, **kwargs)
        return res

    return wrap


###############################################################################################
###############################################################################################
###############################################################################################
class StreamHandler:
    def __init__(self, save_dir):
        self.save_dir = save_dir
        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******
    @date_wrapper
    def query(self, report_name, start_date, end_date, publisher, dry_run = False):

        self.dry_run = dry_run

        archive_api = self.archive_bypass(publisher, report_name, dry_run)

        if publisher == 'henex':
            api_class, query_start, query_end, zip_year_range = self.split_henex(report_name,
                                                                           start_date=start_date,
                                                                           end_date=end_date)


            if api_class:
                api = api_class(self.save_dir)
                api.query(report_name, start_date=query_start, end_date=query_end, dry_run=dry_run, n_threads = 6)
            else:
                api = archive_api

        elif publisher == 'admie':
            api = ADMIE.API(self.save_dir)
            api.query(report_name, start_date=start_date, end_date=end_date, dry_run=dry_run, n_threads = 6)

        elif publisher == 'entsoe':
            api = Entsoe.API(self.save_dir)
            api.query(report_name, start_date=start_date, end_date=end_date,dry_run=dry_run, n_threads = 1)

        self.api = api
        self.n_links = api.n_links
        link_dates = api.link_dates

        self.logger.info("Link dates as arrived to stream-handler: {}".format(link_dates))
        try:
            link_dates = [l[0] for l in link_dates]
        except:
            pass

        self.logger.info("link dates as fed back to main: {}".format(link_dates))

        self.link_dates = link_dates


    # *******  *******   *******   *******   *******   *******   *******
    def archive_bypass(self, publisher, report_name, dry_run):
        sys.stdout = sys.__stdout__
        lake_content = len(list(Path(self.save_dir).glob('*')))

        exclude_from_triggers_to_download_archive = ['IDM_XBID_Results', 'DAM_GasVTP', 'IDM_IDA1_Results',
                                                     'IDM_IDA2_Results', 'IDM_IDA3_Results', 'IDM_IDA1_AggDemandSupplyCurves',
                                                     'IDM_IDA2_AggDemandSupplyCurves','IDM_IDA3_AggDemandSupplyCurves',
                                                     'IDM_IDA1_MarketCoupling', 'IDM_IDA2_MarketCoupling', 'IDM_IDA3_MarketCoupling',
                                                     'IDM_IDA1_ResultsSummary', 'IDM_IDA2_ResultsSummary', 'IDM_IDA3_ResultsSummary',
                                                     ]
        if publisher != 'henex' or report_name in exclude_from_triggers_to_download_archive or lake_content > 0:
            return None
        else:

            if report_name in ['DAS', 'DayAheadSchedulingRequirements', 'HydroVariableCost',
                               'WeekAheadWaterUsageDeclaration']:
                archive = HEnExArchives.SystemArchive(save_dir = self.save_dir)
                archive.query(report_name=report_name, dry_run=dry_run, n_threads = 4)

            else:

                archive = HEnExArchives.MarketArchive(save_dir = self.save_dir)
                archive.query(report_name = report_name, dry_run=dry_run, n_threads = 4)

            return archive

    # *******  *******   *******   *******   *******   *******   *******
    def split_henex(self, report_name, start_date, end_date):

        self.logger.info("Splitting HenEx query between api, scraping, and archive")
        today = datetime.datetime.today().date() + pd.Timedelta(1,'D')

        api_start_proxy = today  - pd.Timedelta(366,'D')
        self.logger.info('\tProxy date to consider as api starting availability point: {}'.format(api_start_proxy))

        api_availability = pd.date_range(api_start_proxy, today, freq='D', inclusive='both')
        zip_availability = pd.date_range('2020-11-1', '2021-12-31', freq='D')

        requested_range = pd.date_range(start_date, end_date, freq='D', inclusive='both')
        self.logger.info("\tRequested range: {} to {}".format(start_date, end_date))
        api_range = np.intersect1d(api_availability, requested_range)
        self.logger.info('\tDeduced api range: {}'.format(api_range))

        if isinstance(api_range, type(None)):
            self.logger.warning("Requested range (from: {}, to: {}) has no intersection with api availability (from: {}, to: {})".format(requested_range[0], requested_range[-1], api_availability[0], api_availability[-1]))
            api = None
            query_start = None
            query_end = None

        elif isinstance(api_range, list) or isinstance(api_range, np.ndarray):
            if len(api_range) == 0:
                self.logger.warning(
                    "Requested range (from: {}, to: {}) has no intersection with api availability (from: {}, to: {})".format(
                        requested_range[0], requested_range[-1], api_availability[0], api_availability[-1]))
                api = None
                query_start = None
                query_end = None

            else:
                query_start = pd.to_datetime(api_range[0]).date()
                query_end = pd.to_datetime(api_range[-1] + pd.Timedelta(1,'D')).date()
                if report_name in HEnEx.API.link_generators().keys():
                    api = HEnEx.API
                    self.logger.info("\tSelected api = HEnEx.API")
                else:
                    api = HEnEx.Scrapers
                    self.logger.info("\tSelected api = HEnEx.Scrapers")
                self.logger.info(f"Requested range: {api_range[0]} to {api_range[-1]}")
                self.logger.info(f'Perceived range: {query_start} to {query_end}')
        requested_zip_range = requested_range[requested_range.isin(zip_availability)]
        zip_years_range = None

        if report_name in ['DAM_GasVTP']:
            api = HEnEx.API
            query_start = start_date
            query_end = end_date
            zip_years_range = np.array([])

        return api, query_start, query_end, zip_years_range


    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******


###############################################################################################
###############################################################################################
###############################################################################################
def main(report_name, publisher, start_date, end_date, save_dir):

    api = StreamHandler(save_dir)
    api.query(report_name, start_date=start_date, end_date=end_date, publisher=publisher)