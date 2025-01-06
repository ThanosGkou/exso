import datetime
import traceback
import logging
import os
import re
import shutil
import sys
import urllib.request

import numpy as np
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
from colorama import Fore
import exso
from exso.DataLake.APIs.Assistant import Assistant
from exso.Utils.DateTime import DateTime
from tqdm import tqdm

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
class API(Assistant):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

        self.home_url = 'https://www.enexgroup.gr/'  # former base_page
        self.base_url = "https://www.enexgroup.gr/el/markets-publications-el-day-ahead-market"

    # *******  *******   *******   *******   *******   *******   *******
    @date_wrapper
    def query(self, report_name, start_date, end_date, dry_run = False, n_threads = 6):


        self.dry_run = dry_run
        self.logger.info("Making query with arguments: report_name: {}, start_date: {}, end_date: {}, dry_run: {}, n_threads: {}".
                         format(report_name, start_date, end_date, dry_run, n_threads))

        self.report_name = report_name

        candidate_links = self.get_links(report_name, start_date, end_date) #

        candidate_filenames = list(map(lambda x: Path(x).name, candidate_links))
        valid_indices, trivial_indices = self.get_non_trivial_mask(candidate_filenames=candidate_filenames)
        links = self.get_indexed_slice(valid_indices, candidate_links)
        trivial_links = self.get_indexed_slice(trivial_indices, candidate_links)
        #new
        if dry_run:
            self.save_dir = self.save_dir / '.temp'
            self.save_dir.mkdir(exist_ok=True)
            # self.save_dir = os.path.join(self.save_dir, '.temp')

        filepaths, filenames, dates = self.extract_filepaths_filenames_dates(links)
        triv_filepaths, triv_filenames, triv_dates = self.extract_filepaths_filenames_dates(trivial_links)

        print("\n\tDownloading from Henex API")
        self.logger.info("Sending links to the .download() function. First 5 links: {}".format(links[:5]))

        validation = self.download(links, filepaths, n_threads)
        valid_indices = [i for i, valid in enumerate(validation) if valid]
        links, filepaths, filenames, dates = self.get_indexed_slice(valid_indices, links, filepaths, filenames, dates)

        # print(filenames, triv_filenames)
        # print(filepaths, triv_filepaths)
        # print(dates, triv_dates)

        self.filenames = triv_filenames + filenames
        self.filepaths = triv_filepaths + filepaths
        self.link_dates = triv_dates + dates
        self.n_links = len(dates + triv_dates)

        if dry_run:
            shutil.rmtree(self.save_dir)

    # *******  *******   *******   *******   *******   *******   *******
    def get_links(self, report_name, start_date, end_date):
        date_range = pd.date_range(start_date, end_date, freq='D', inclusive='both')
        str_dates = list(map(lambda x: DateTime.make_string_date(x, sep=""), date_range))
        ver = "01"

        link_gen = self.get_link_generator(report_name)
        if not link_gen:
            return []
        else:
            yolo_links = list(map(lambda x: link_gen.format(x, ver), str_dates))
            self.logger.info("Got {} candidate links".format(len(yolo_links)))
            return yolo_links

    # *******  *******   *******   *******   *******   *******   *******
    @staticmethod
    def link_generators():
        '''memo:
        {'PrelimResults':'MarketCoupling',
         'Results':'Results',
         'AggCurves':'AggDemandSupplyCurves',
         'POSNOM':'PhysicalDeliveriesOfftakes'
         }

        NDPS: Net deliveries, not needed
        PreMarketSummary: has the POSNOM

        '''
        generators = {'IDM_CRIDA1_Results': 'https://www.enexgroup.gr/documents/20126/853663/{}_EL-CRIDA1_Results_EN_v{}.xlsx',
                      'IDM_CRIDA2_Results':'https://www.enexgroup.gr/documents/20126/853680/{}_EL-CRIDA2_Results_EN_v{}.xlsx',
                      'IDM_CRIDA3_Results': 'https://www.enexgroup.gr/documents/20126/853704/{}_EL-CRIDA3_Results_EN_v{}.xlsx',
                      'IDM_IDA1_Results': 'https://www.enexgroup.gr/documents/20126/3257249/{}_EL-IDA1_Results_EN_v{}.xlsx',
                      'IDM_IDA2_Results': 'https://www.enexgroup.gr/documents/20126/3257281/{}_EL-IDA2_Results_EN_v{}.xlsx',
                      'IDM_IDA3_Results': 'https://www.enexgroup.gr/documents/20126/3257522/{}_EL-IDA3_Results_EN_v{}.xlsx',
                      'IDM_CRIDA1_MarketCoupling': 'https://www.enexgroup.gr/documents/20126/853668/{}_EL-CRIDA1_PrelimResults_EN_v{}.xlsx',
                      'IDM_CRIDA2_MarketCoupling': 'https://www.enexgroup.gr/documents/20126/853692/{}_EL-CRIDA2_PrelimResults_EN_v{}.xlsx',
                      'IDM_CRIDA3_MarketCoupling': 'https://www.enexgroup.gr/documents/20126/855431/{}_EL-CRIDA3_PrelimResults_EN_v{}.xlsx',
                      'IDM_IDA1_MarketCoupling': 'https://www.enexgroup.gr/documents/20126/3257252/{}_EL-IDA1_PrelimResults_EN_v{}.xlsx',
                      'IDM_IDA2_MarketCoupling': 'https://www.enexgroup.gr/documents/20126/3257278/{}_EL-IDA2_PrelimResults_EN_v{}.xlsx',
                      'IDM_IDA3_MarketCoupling': 'https://www.enexgroup.gr/documents/20126/3257525/{}_EL-IDA3_PrelimResults_EN_v{}.xlsx',
                      'IDM_CRIDA1_AggDemandSupplyCurves': 'https://www.enexgroup.gr/documents/20126/853660/{}_EL-CRIDA1_AggrCurves_EN_v{}.xlsx',
                      'IDM_CRIDA2_AggDemandSupplyCurves': 'https://www.enexgroup.gr/documents/20126/853695/{}_EL-CRIDA2_AggrCurves_EN_v{}.xlsx',
                      'IDM_CRIDA3_AggDemandSupplyCurves': 'https://www.enexgroup.gr/documents/20126/853701/{}_EL-CRIDA3_AggrCurves_EN_v{}.xlsx',

                      'IDM_IDA1_AggDemandSupplyCurves': 'https://www.enexgroup.gr/documents/20126/3257246/{}_EL-IDA1_AggrCurves_EN_v{}.xlsx',
                      'IDM_IDA2_AggDemandSupplyCurves': 'https://www.enexgroup.gr/documents/20126/3257284/{}_EL-IDA2_AggrCurves_EN_v{}.xlsx',
                      'IDM_IDA3_AggDemandSupplyCurves': 'https://www.enexgroup.gr/documents/20126/3257519/{}_EL-IDA3_AggrCurves_EN_v{}.xlsx',

                      'DAM_Results':'https://www.enexgroup.gr/documents/20126/200106/{}_EL-DAM_Results_EN_v{}.xlsx',
                      'DAM_MarketCoupling':'https://www.enexgroup.gr/documents/20126/200091/{}_EL-DAM_PrelimResults_EN_v{}.xlsx',
                      'DAM_PhysicalDeliveriesOfftakes':'https://www.enexgroup.gr/documents/20126/214481/{}_EL-DAM_POSNOMs_EN_v{}.xlsx',
                      'DAM_AggDemandSupplyCurves':'https://www.enexgroup.gr/documents/20126/200034/{}_EL-DAM_AggrCurves_EN_v{}.xlsx',
                      'DAM_BlockOrders': 'https://www.enexgroup.gr/documents/20126/270103/{}_EL-DAM_BLKORDRs_EN_v{}.xlsx',
                      'DAM_GasVTP':'https://www.enexgroup.gr/documents/20126/997118/{}_NGAS_DOL_EN_v{}.xlsx',
                      'IDM_XBID_Results': 'https://www.enexgroup.gr/documents/20126/1550281/{}_EL-XBID_Results_EN_v{}.xlsx',
                       }
        return generators

    # *******  *******   *******   *******   *******   *******   *******
    def get_link_generator(self, report_name):

        link_generators = self.link_generators()

        if report_name not in link_generators.keys():
            self.logger.warning("Oops. The report-type requested ({}) does not have a link-generator. ".format(report_name))
            self.logger.info("Available link generators are: {}".format(link_generators.keys()))
            self.logger.info("Will try to scrape or acess market archive zips.")
            return None
        else:
            return link_generators[report_name]


    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******


###############################################################################################
###############################################################################################
###############################################################################################
class Scrapers(Assistant):
    ''' onlt for categories that cannot be automated '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.home_url = 'https://www.enexgroup.gr/'

    # *******  *******   *******   *******   *******   *******   *******
    @date_wrapper
    def query(self, report_name, start_date, end_date, dry_run = False, n_threads=6):
        self.dry_run = dry_run
        self.logger.info("Making query with arguments: report_name: {}, start_date: {}, end_date: {}, n_threads: {}".
                         format(report_name, start_date, end_date, n_threads))


        self.report_name = report_name

        print("\n\tDownloading from Henex Webpage (scraping)")
        try:
            links, dates, filenames = self.get_links(start_date,end_date)
        except:
            print(f'\tFailed to connect to henex webpage. This could refer to an archived-only report, in which case ignore the warning.')
            self.logger.warning("Failed to connect in henex via scraping.")
            self.logger.debug(traceback.format_exc())
            self.filenames = []
            self.filepaths = []
            self.n_links = 0
            self.link_dates = []
            return


        valid_indices, trivial_indices = self.get_non_trivial_mask(filenames)  # ignore if already in lake
        links, dates, filenames = self.get_indexed_slice(valid_indices, links, dates, filenames)
        filepaths = list(map(lambda x: os.path.join(self.save_dir, x), filenames))
        self.filenames = filenames
        self.filepaths = filepaths

        try:
            link_dates = DateTime.date_magician(dates)
        except:
            link_dates = []
            for d in dates:
                try:
                    link_dates.append(DateTime.date_magician(d))
                except:
                    pass

        self.n_links = len(link_dates)
        self.link_dates = link_dates

        if dry_run:
            return

        self.download(links, filepaths, n_threads=6)

    # *******  *******   *******   *******   *******   *******   *******
    def get_links(self, start_date, end_date):
        selector = self.get_selector()
        market_url = self.get_market_url()

        self.web = self._WebMethods(selector, market_url, home_url=self.home_url)

        max_n_pages = self.web.get_max_pages()
        n_files_per_page = self.web.get_number_of_files_per_page()

        start_date, end_date = self.proactive_check(max_n_pages, n_files_per_page, start_date, end_date)

        days_requested = (end_date - start_date).days
        pages_to_be_read = int(days_requested/n_files_per_page) + 2 # this was +1

        start_date_int = int(DateTime.make_string_date(start_date, sep=""))
        end_date_int = int(DateTime.make_string_date(end_date, sep=""))

        links, dates, filenames = self.iterate(pages_to_be_read, market_url, start_date_int, end_date_int)

        return links, dates, filenames


    # *******  *******   *******   *******   *******   *******   *******
    def proactive_check(self, max_n_pages, n_files_per_page, start_date, end_date):

        original_dates = {'start':start_date, 'end':end_date}
        today = datetime.datetime.today().date()

        min_potential_date_in_web = today - pd.Timedelta(max_n_pages * n_files_per_page, 'D') # all pages full
        min_guaranteed_date_in_web = today - pd.Timedelta((max_n_pages-1) * n_files_per_page + 1, 'D') # last page, only one file

        self.logger.info("Minimum potential date to be found in the web is: {}".format(min_potential_date_in_web))
        self.logger.info("Minimum guaranteed date to be found in the web is: {}".format(min_guaranteed_date_in_web))
        self.logger.info("Requested start date is: {}".format(start_date))

        if start_date < min_potential_date_in_web:
            self.logger.warning("Impossible to find the complete query in the web.")
            self.logger.warning("{} Days cannot be retrieved".format((min_potential_date_in_web-start_date).days) )
            self.logger.warning("Setting start-date to the min. potential date.")
            start_date = min_potential_date_in_web

        elif start_date < min_guaranteed_date_in_web:
            self.logger.warning("It is possible that some of the dates requestd may not be available in the web.")
            dates_in_question = pd.date_range(start_date, start_date + pd.Timedelta(n_files_per_page-1), freq='D', inclusive='both')
            self.logger.warning("Days in question : {}".format(dates_in_question))
        ############################3
        if start_date > today + pd.Timedelta('1D'):
            sys.stdout = sys.__stdout__
            print('Original dates requested: ', original_dates)
            print('As arrived in break switch: Start date: {}, End Date: {}'.format(start_date, end_date))
            self.logger.error("Requested start-date is greater than the current date! Exiting.")
            raise AssertionError

        elif start_date > end_date:
            self.logger.error(
                "Requested start-date ({}) is greater than the requested end-date ({})".format(start_date, end_date))
            raise AssertionError

        ############################3
        if end_date > today:
            self.logger.warning("Requested end-date ({}) is greater than the current date! Will be set to: {}".format(end_date, today))
            end_date = today

        return start_date, end_date

    # *******  *******   *******   *******   *******   *******   *******
    def iterate(self, max_n_pages, market_url, start_date, end_date):

        links = []; dates = []; filenames = []
        url =  market_url

        tqdm._instances.clear()
        progress_bar = tqdm(range(max_n_pages),
                            desc='\tScraping Progress: ',
                            **exso._pbar_settings,
                            disable=self.dry_run)

        for i in progress_bar:

            page_filenames, page_dates, page_links, section = self.web.get_webpage_links(url)

            if not section:
                break
            url = self.web.pagination(section)
            date_reached = page_dates[-1]
            progress_bar.set_postfix_str(s=date_lambda(str(date_reached)))

            self.logger.info("Iteration {}: \tDate reached: {}".format(i, date_reached))

            filenames.extend(page_filenames)
            links.extend(page_links)
            dates.extend(page_dates)

            if date_reached > end_date:
                continue
            elif date_reached < start_date:
                break

        links = list(map(lambda x: self.home_url + x, links))
        links = np.array(links)
        dates = np.array(dates)
        filenames = np.array(filenames)
        return links, dates, filenames

    # *******  *******   *******   *******   *******   *******   *******
    def update(self, links, dates, filenames, file_info):

        download_link = self.home_url + file_info['href_link']
        links.append(download_link)
        dates.append(file_info['date'])
        filenames.append(file_info['filename'])

    # *******  *******   *******   *******   *******   *******   *******
    def get_selector(self):

        html_objects = {'DAM_ResultsSummary': 'portlet_com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_INSTANCE_9CZslwWTpeD2',
                       'DAM_PreMarketSummary': 'portlet_com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_INSTANCE_b1cLreZjcqxn',
                       'IDM_CRIDA1_ResultsSummary': 'portlet_com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_INSTANCE_B9t04v2LgOeG',
                       'IDM_CRIDA2_ResultsSummary': 'portlet_com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_INSTANCE_tY6GNWWgb8c0',
                       'IDM_CRIDA3_ResultsSummary': 'portlet_com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_INSTANCE_CF7YHvNr21f8',

                        'IDM_IDA1_ResultsSummary': 'portlet_com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_INSTANCE_mRFiDHvb6Dwx',
                        'IDM_IDA2_ResultsSummary': 'portlet_com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_INSTANCE_mPDt7zJ2kUHv',
                        'IDM_IDA3_ResultsSummary': 'portlet_com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_INSTANCE_qFj20qHSE8Wj',
                        }

        selector = {'class':'portlet',
                    'id':html_objects[self.report_name]}

        return selector


    # *******  *******   *******   *******   *******   *******   *******
    def get_market_url(self):
        market, report = self.derive_market_components(self.report_name)

        market_urls = {'DAM':'https://www.enexgroup.gr/el/markets-publications-el-day-ahead-market',
                       'LIDA':'https://www.enexgroup.gr/el/markets-publications-el-intraday-market-lida', # deprecated
                       'CRIDA':'https://www.enexgroup.gr/el/markets-publications-el-intraday-market#CRIDA',
                       'IDA':'https://www.enexgroup.gr/el/markets-publications-el-intraday-market#IDA',
                       }

        market_no_digit = re.sub(r'\d','', market)
        return market_urls[market_no_digit]


    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******

    class _WebMethods:
        def __init__(self, selector, market_url, home_url):
            self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
            self.selector = selector
            self.market_url = market_url
            self.home_url = home_url

        # *******  *******   *******   *******   *******   *******   *******
        @staticmethod
        def get_section(url, selector):
            html_page = urllib.request.urlopen(url,timeout=2)
            soup = BeautifulSoup(html_page, 'html.parser')
            section = soup.findAll('section', selector)[0]
            return section

        # *******  *******   *******   *******   *******   *******   *******
        @staticmethod
        def pagination(section):
            pagination_dropdown = section.find('ul', {'class': 'lfr-pagination-buttons pager'})
            for pag in pagination_dropdown.findAll('a'):
                if pag.text.strip() == 'Επόμενο':
                    new_url = pag.get('href')
            return new_url

        # *******  *******   *******   *******   *******   *******   *******
        def get_max_pages(self):

            ''' Get the drop=-down list that says "1 out of ####", in order to define the maximum pages.'''
            section = self.get_section(self.market_url, self.selector)

            dropdown = section.find('a', {'class': 'dropdown-toggle direction-down max-display-items-15 btn btn-default'})
            title = dropdown.get('title')
            ''' Title format : "Σελίδα 1 από 60"'''
            max_pages = int(title.split(' ')[-1])

            return max_pages

        # *******  *******   *******   *******   *******   *******   *******
        def get_number_of_files_per_page(self):
            section = self.get_section(self.market_url, self.selector)
            '#yui_patched_v3_18_1_1_1668869195170_2887'

            display_setting = section.find('small',
                                           {'class': 'search-results'})


            text = display_setting.string
            ''' something like: " Εμφάνιση 1 - 7 από 325 αποτελέσματα. " 
            '''
            n_files_per_page = re.findall(r'(?<=-\s)\d', text) #lookbehind if "-\s"
            n_files_per_page = int(n_files_per_page[0])

            return n_files_per_page

        # *******  *******   *******   *******   *******   *******   *******
        def get_webpage_links(self, page_url):
            filenames, dates, links = [],[],[]
            section = None
            try:
                section = self.get_section(page_url, self.selector)
            except:
                self.logger.warning("Some slight or severe error occured while scraping.")
                self.logger.warning("Was trying to access a next pagination (section = {}), with the link: {}".format(section, page_url))

                return filenames, dates, links, section

            for href in section.findAll('a'):
                if href.find('i', {'class': 'icon-download'}):

                    filename = href.text.strip()
                    date = int(filename.split('_')[0])
                    href_link = href.get('href')

                    filenames.append(filename)
                    dates.append(date)
                    links.append(href_link)

            return filenames, dates, links, section

        # *******  *******   *******   *******   *******   *******   *******
###############################################################################################
###############################################################################################
###############################################################################################
