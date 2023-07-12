import traceback

import entsoe
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import exso
import sys
import pandas as pd
import logging
import tqdm
import datetime, os, shutil
from exso.Utils.DateTime import DateTime

# *******  *******   *******   *******   *******   *******   *******
date_lambda = lambda x: datetime.datetime.strftime(DateTime.date_magician(x, return_stamp = False), format="%d-%b-%y")

# *******  *******   *******   *******   *******   *******   *******
def period_splitter(start, end, days_thresh = 1):
    days_requested = (end - start).days
    request_bins = []
    rolling_start = start
    if days_requested > days_thresh:
        rolling_end = rolling_start + pd.Timedelta(days_thresh, 'D')
        while rolling_end < end:
            request_bins.append((rolling_start, rolling_end))
            rolling_start += pd.Timedelta(days_thresh, 'D')
            rolling_end += pd.Timedelta(days_thresh, 'D')

        request_bins.append((pd.Timestamp(rolling_start), pd.Timestamp(end)))
    else:
        request_bins = [(pd.Timestamp(start), pd.Timestamp(end))]

    return request_bins
# *******  *******   *******   *******   *******   *******   *******
def date_wrapper(func):
    def wrap(*args, **kwargs):
        kwargs.update(start_date = DateTime.date_magician(kwargs['start_date']))
        kwargs.update(end_date = DateTime.date_magician(kwargs['end_date']))
        res = func(*args, **kwargs)
        return res

    return wrap

# *******  *******   *******   *******   *******   *******   *******
def day_wrapper(func):
    def wrap(*args, **kwargs):
        start_date = DateTime.date_magician(kwargs['start_date'], return_stamp=True)
        end_date = DateTime.date_magician(kwargs['end_date'], return_stamp=True)
        bins = period_splitter(start_date, end_date)

        tqdm.tqdm._instances.clear()
        pbar = tqdm.tqdm(range(len(bins)), desc="\tDownload Progress")
        responses = []
        dates = []
        statuses = []
        for i in pbar:
            pair = bins[i]
            pair_kwargs = kwargs.copy()
            pair_kwargs['start_date'] = pair[0].tz_localize('UTC')
            pair_kwargs['end_date'] = pair[1].tz_localize('UTC')

            date = DateTime.make_string_date(pair[0],sep='')
            dates.append(date)

            response, status = func(*args, **pair_kwargs)
            responses.append(response)
            statuses.append(status)
            pbar.set_postfix_str(s="Reached: {}".format(pair[1].date()))

        # response = pd.concat([*responses], axis=0)
        return dates, responses, statuses

    return wrap


# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
class EntsoAssistant:
    # *******  *******   *******   *******   *******   *******   *******
    def unit_request(self, link: str) -> (bool, str or None):

        self.logger.info("Requesting link: {}".format(link))

        max_tries = 3
        try_number = 0
        success = False

        payload = pd.DataFrame()
        ex = ""
        while try_number < max_tries:
            try:
                payload = eval(link)
                # response = requests.get(link)
                if payload.empty:
                    success = False
                else:
                    success = True
                break

            except Exception as ex:

                try_number += 1

        if success:
            link_is_valid = True
            # print()
            # print(payload)
            payload = self._conform_weekly(payload)


        if not success:
            link_is_valid = False

            self.logger.warning("Could not download link: {}".format(link))
            # self.logger.warning(ex)

        return link_is_valid, payload

    # *******  *******   *******   *******   *******   *******   *******
    def _conform_weekly(self, payload):
        if payload.empty:
            return payload

        resolution = payload.index[1] - payload.index[0]
        mould_dates = pd.date_range(payload.index[0], payload.index[0] + pd.Timedelta(7,'D'), freq=resolution, inclusive='left')

        if payload.shape[0] != mould_dates.size:
            payload = payload.reindex(mould_dates) # anything missing, leave it as NaN, not "nearest"

        return payload

    # *******  *******   *******   *******   *******   *******   *******
    def unit_save(self, content, filepath):
        days = content.index.to_period('D').unique()
        content = content.tz_convert('UTC').tz_localize(None)
        filename = filepath.name
        parent = filepath.parent
        pure_name = "_".join(filename.split('_')[1:])
        for d in days:
            df = content[content.index.to_period('D') == d]
            if df.empty:
                continue

            str_date = DateTime.make_string_date(d, sep = '')
            fp = parent / (str_date + "_" +  pure_name)
            df.to_csv(fp)

    # *******  *******   *******   *******   *******   *******   *******
    def unit_request_and_save(self, link, filepath):

        link_is_valid, content = self.unit_request(link)

        if link_is_valid:
            if isinstance(content, pd.DataFrame):
                self.unit_save(content, filepath)
            else:
                self.logger.warning("Empty content arrived, although it shouldn't get until here. Just skipping...")
                self.logger.warning("Link: {}".format(link))
                self.logger.warning('Target-filepath was: {}'.format(link, filepath))
                link_is_valid = False

        return link_is_valid

    # *******  *******   *******   *******   *******   *******   *******
    def _concurrent_download(self, links, filepaths, n_threads=6):
        # print('\tDownloading concurrently with {} threads.'.format(n_threads))
        validation = []
        with ThreadPoolExecutor(max_workers=n_threads) as executor:
            futures = [executor.submit(self.unit_request_and_save, link, filepath) for link, filepath in
                       zip(links, filepaths)]

            pbar = tqdm.tqdm(range(len(futures)),
                        desc="\tDownloading Progress (mt)",
                        **exso._pbar_settings)

            for i in pbar:
                link_is_valid = futures[i].result()
                validation.append(link_is_valid)

            # for future in as_completed(futures):
            #    link_is_valid = future.result()
            #    validation.append(link_is_valid)

        return validation

    # *******  *******   *******   *******   *******   *******   *******
    def _sequential_download(self, links, filepaths, dates = None, limit_per_minute = None):

        validation = []
        tqdm.tqdm._instances.clear()
        progress_bar = tqdm.tqdm(range(len(links)),
                            desc='\tDownload Progress',
                            **exso._pbar_settings,
                            disable=self.dry_run)

        if not limit_per_minute:
            limit_per_minute = 99999999

        residual_in_minute = limit_per_minute
        to = time.perf_counter()

        for i in progress_bar:
            link = links[i]
            path = filepaths[i]
            filename = os.path.split(path)[-1]
            progress_bar.set_postfix_str(s=filename)

            # print(time.perf_counter() - to)

            if time.perf_counter() - to > 60:
                residual_in_minute = limit_per_minute
                to = time.perf_counter()

            if residual_in_minute == 0:
                print('\nWaiting to respect maximum queries per minute (400).')
                time.sleep(10)

            link_is_valid = self.unit_request_and_save(link, path)
            # print(f'{link_is_valid = }')
            residual_in_minute -= 1
            validation.append(link_is_valid)

        return validation

    # *******  *******   *******   *******   *******   *******   *******
    def download(self, links, dates, filepaths, n_threads = 1, limit_per_minute = 400):

        t = time.time()
        n_links = len(links)
        if n_threads > 1 and len(links) > 4:
            self.logger.info(
                "Starting multi-threaded download of {} links, using {} threads".format(n_links, n_threads))
            validation = self._concurrent_download(links, filepaths, n_threads)
        else:
            self.logger.info("Starting sequential download of {} links".format(n_links))
            validation = self._sequential_download(links, filepaths, limit_per_minute=limit_per_minute)

        self.logger.info("Downloading completed in {:,} sec".format(round(time.time() - t, 3)))
        return validation






###############################################################################################
###############################################################################################
###############################################################################################
class API(EntsoAssistant):
    def __init__(self, save_dir:str, api_token = None):
        self.save_dir = save_dir
        os.makedirs(self.save_dir, exist_ok=True)

        self.home_url = 'https://transparency.entsoe.eu/'
        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        api_token = '355722a1-84de-40f1-8c5a-eff06951e695'
        self.client = entsoe.EntsoePandasClient(api_token)
    # *******  *******   *******   *******   *******   *******   *******
    @date_wrapper
    def query(self, report_name, start_date, end_date, dry_run=False, n_threads=1):

        sys.stdout = sys.__stdout__
        self.dry_run = dry_run
        self.logger.info(
            "Making query with arguments: report_name: {}, start_date: {}, end_date: {}, dry_run: {}, n_threads: {}".
            format(report_name, start_date, end_date, dry_run, n_threads))

        report_components = report_name.split('_')
        country_code = report_components[0]
        query_type = 'query_' + '_'.join(report_components[1:]).lower()
        # query_type = 'query_' + query_type.lower()

        neighbors = entsoe.mappings.NEIGHBOURS[country_code]
        # for k,v in entsoe.mappings.NEIGHBOURS.items() :
        #     print(k)
        #     print(v)
        #     print('\n\n')
        # input('asfasf')
        # start_date = pd.Timestamp(start_date).tz_localize('UTC')
        # for n in neighbors:
        #     print(country_code, '--> ', n)
        #     print(self.client.query_scheduled_exchanges(country_code,start = start_date, end = start_date + pd.Timedelta('1D'), country_code_to = n))
        #     print('\n\n')
        #
        # input('asfasfasf')
        # self.client.query_activated_balancing_energy("GR", start = start_date, end = end_date)
        # input('asfasf')
        query_strings, dates = self.get_links(query_type, country_code, start_date, end_date, batch_size=7) # query the api and ask for the links only

        filenames = [d + '_' + report_name + '.csv' for d in dates]
        filepaths = [Path(self.save_dir / fn) for fn in filenames]

        if dry_run or not query_strings:  # do not actually download the files
            self.link_dates = dates
            self.filenames = filenames
            self.filepaths =filepaths
            self.logger.info("Mode = dry-run, OR, the query is trivial. Returning without downloading anything.")
            return

        valid_indices = self.download(query_strings, dates, filepaths, n_threads=n_threads, limit_per_minute=400)

        self.link_dates = [d for flag,d in zip(valid_indices, dates) if flag]
        self.filepaths = [fp for flag, fp in zip(valid_indices, filepaths) if flag]
        self.filenames = [fn for flag, fn in zip(valid_indices, filenames) if flag]

        self.n_links = len(self.link_dates)

    # *******  *******   *******   *******   *******   *******   *******
    def get_links(self, query_type, country_code, start_date, end_date, batch_size = 1, **kwargs):
        bins = period_splitter(start_date, end_date, days_thresh=batch_size)
        queries = []
        dates = []
        for bin in bins:
            start_date, end_date = bin
            start_date = pd.Timestamp(start_date).tz_localize('UTC')
            end_date = pd.Timestamp(end_date).tz_localize('UTC')
            query_str = f'self.client.__getattribute__("{query_type}")("{country_code}", start=pd.Timestamp("{start_date}"), end=pd.Timestamp("{end_date}"))'

            queries.append(query_str)
            dates.append(DateTime.make_string_date(start_date,sep = ""))
        return queries, dates