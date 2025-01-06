import datetime
import logging
import os
import re
import shutil
from pathlib import Path

import requests
from exso.DataLake.APIs import ZipHandler
from exso.DataLake.APIs.Assistant import Assistant
from exso.Utils.DateTime import DateTime
from exso.Utils.Paths import Paths

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
    def __init__(self, save_dir:str):
        self.save_dir = save_dir
        os.makedirs(self.save_dir, exist_ok=True)

        self.home_url = 'https://www.admie.gr/'
        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******
    @date_wrapper
    def query(self, report_name, start_date, end_date, dry_run = False, n_threads = 1):
        self.dry_run = dry_run
        self.logger.info("Making query with arguments: report_name: {}, start_date: {}, end_date: {}, dry_run: {}, n_threads: {}".
                         format(report_name, start_date, end_date, dry_run, n_threads))

        candidate_links = self.get_links(report_name, start_date, end_date) # query the api and ask for the links only
        if isinstance(candidate_links, type(None)):
            candidate_links = []
        self.logger.info("Got {} candidate links".format(len(candidate_links)))

        candidate_filenames = list(map(lambda x: Path(x).name, candidate_links))
        valid_indices, trivial_indices = self.get_non_trivial_mask(candidate_filenames=candidate_filenames)
        links = self.get_indexed_slice(valid_indices, candidate_links)

        self.logger.info("Non-trivial links (links that their corresponding filepaths does not exist in directory: {}) : {}".format(self.save_dir, len(links)))
        filepaths, filenames, dates = self.extract_filepaths_filenames_dates(links)

        #dates = [s[0] for s in dates]
        self.link_dates = dates

        self.filenames = filenames
        self.filepaths = filepaths
        self.n_links = len(dates)

        if dry_run or not self.n_links: # do not actually download the files
            self.logger.info("Mode = dry-run, OR, the query is trivial. Returning without downloading anything.")
            return

        self.download(links, filepaths, n_threads)

        zip_files = [fp for fp in filepaths if os.path.split(fp)[-1].split('.')[-1].lower() == 'zip']
        ZIP_files = [fp for fp in filepaths if os.path.split(fp)[-1].split('.')[-1] == 'ZIP'] # admie for some reason, somethimes put .ZIP instead of .zip

        [shutil.move(src,Path(src).with_suffix('.zip')) for src in ZIP_files]

        if zip_files:
            zh = ZipHandler.ZipHandler(zipped_dir = self.save_dir,
                                       must_contain = '*.xls*',
                                       must_not_contain = None)
            zh.run()


    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******
    def get_links(self, report_name, start_date, end_date):

        url = self.home_url + 'getOperationMarketFilewRange?dateStart={}&dateEnd={}&FileCategory={}'.format(start_date, end_date, report_name)

        r = requests.get(url, allow_redirects=True)


        if r.status_code == 200:

            content = r.content
            content = content.decode()
            # avoid newer regex version SyntaxWarning for Invalid escape sequence '\/'
            content = re.sub(r'\\','',content)
            content = eval(content)
            links = content.copy()
            # I still need the below line, to convert the response from list of dicts, to list of links[str]
            links = [re.sub(r'\\','',link_info['file_path']) for link_info in content]

            return links
        else:
            self.logger.warning("Query returned non-successful status code.")
            self.logger.warning("Query link was: {}".format(url))
            self.logger.warning("Status code was: {}".format(r.status_code))
            self.logger.warning("Text: {}".format(r.text))

    # ********   *    ********   *    ********   *    ********   *   ********
    # ********   *    ********   *    ********   *    ********   *   ********
