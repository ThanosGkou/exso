import datetime
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import requests
import exso
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
class Assistant:
    def __init__(self, save_dir):
        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.save_dir = save_dir
        os.makedirs(save_dir, exist_ok=True)

    # *******  *******   *******   *******   *******   *******   *******
    def unit_request(self, link:str) -> (bool, str or None):

        self.logger.info("Requesting link: {}".format(link))

        max_tries  = 3
        try_number = 0
        success = False
        while try_number<max_tries:
           try:

               response = requests.get(link)
               success = True
               break
           except:
               try:
                   response = requests.get(link, proxies={'https': "http://10.100.133.251:80"})
                   success  = True
                   break
               except:
                   try_number += 1

        if success == False:
            return False,  None
            sys.exit("Failed 3 times to download link: {}")


        if response.status_code == 200:
            link_is_valid = True
            payload = response.content

        else:
            self.logger.warning({"Response status":response.status_code,
                           "Response Ok? ":response.ok,
                           'Response Encoding':response.encoding,
                           'Response Text Beginning':response.text[:100]})

            link_is_valid = False
            payload = None

        return link_is_valid, payload

    # *******  *******   *******   *******   *******   *******   *******
    def unit_save(self, content, filepath):

        with open(filepath, 'wb') as f:
            f.write(content)

    # *******  *******   *******   *******   *******   *******   *******
    def unit_request_and_save(self, link, filepath):

        link_is_valid, content = self.unit_request(link)

        if link_is_valid:
            if content:
                self.unit_save(content, filepath)
            else:
                self.logger.warning( "Empty content arrived, although it shouldn't get until here. Just skipping...")
                self.logger.warning("Link: {}".format(link))
                self.logger.warning('Target-filepath was: {}'.format( link, filepath))
                link_is_valid = False

        return link_is_valid

    # *******  *******   *******   *******   *******   *******   *******
    def _concurrent_download(self, links, filepaths, n_threads=6):
        # print('\tDownloading concurrently with {} threads.'.format(n_threads))
        validation = []
        with ThreadPoolExecutor(max_workers=n_threads) as executor:
            futures = [executor.submit(self.unit_request_and_save, link, filepath) for link, filepath in zip(links, filepaths)]

            pbar = tqdm(range(len(futures)),
                        desc="\tDownloading Progress (mt)",
                        **exso._pbar_settings)

            for i in pbar:
                link_is_valid = futures[i].result()
                validation.append(link_is_valid)

            #for future in as_completed(futures):
            #    link_is_valid = future.result()
            #    validation.append(link_is_valid)

        return validation

    # *******  *******   *******   *******   *******   *******   *******
    def _sequential_download(self, links, filepaths):
        validation = []
        tqdm._instances.clear()
        progress_bar = tqdm(range(len(links)),
                            desc='\tDownload Progress',
                            **exso._pbar_settings,
                            disable=self.dry_run)
        for i in progress_bar:
            link = links[i]
            path = filepaths[i]
            filename = os.path.split(path)[-1]
            progress_bar.set_postfix_str(s=filename)

            link_is_valid = self.unit_request_and_save(link, path)
            validation.append(link_is_valid)
        return validation

    # *******  *******   *******   *******   *******   *******   *******
    def download(self, links, filepaths, n_threads = 1):

        t = time.time()
        n_links = len(links)
        if n_threads > 1 and len(links) > 4:
            self.logger.info(
                "Starting multi-threaded download of {} links, using {} threads".format(n_links, n_threads))
            validation = self._concurrent_download(links, filepaths, n_threads)
        else:
            self.logger.info("Starting sequential download of {} links".format(n_links))
            validation = self._sequential_download(links, filepaths)

        self.logger.info("Downloading completed in {:,} sec".format(round(time.time() - t, 3)))
        return validation

    # *******  *******   *******   *******   *******   *******   *******
    def get_non_trivial_mask(self, candidate_filenames):

        cached_filenames = os.listdir(self.save_dir)
        dnld_indices = [i for i, fn in enumerate(candidate_filenames) if fn not in cached_filenames]
        trivial_indices = [i for i,fn in enumerate(candidate_filenames) if fn in cached_filenames]
        self.logger.info("Derived {} NON-trivial links out of {} links provided".format(len(dnld_indices), len(candidate_filenames)))

        return dnld_indices, trivial_indices

    # *******  *******   *******   *******   *******   *******   *******
    def get_indexed_slice(self, slicer, *lists, return_as_type_list = True):

        return_lists = []
        for l in lists:
            new = [l[indexx] for indexx in slicer]
            return_lists.append(new)
        if len(lists)==1:
            return_lists = return_lists[0]
        return return_lists

    # *******  *******   *******   *******   *******   *******   *******
    def get_non_trivial_links(self, candidate_links, candidate_filenames = None):

        if isinstance(candidate_filenames, list):
            pass
        else:
            candidate_filenames= list(map(lambda x: os.path.split(x)[-1], candidate_links)) # the last part of each link is the actual filename (whatever.xlsx)
        # if these filenames exist in the save_dir, omit them
        cached_filenames = os.listdir(self.save_dir)
        links_to_dnld = [candidate_links[i] for i in range(len(candidate_links)) if candidate_filenames[i] not in cached_filenames]
        self.logger.info("Non-trivial links (links that their corresponding filepaths does not exist in directory: {}) : {}".format(self.save_dir, len(links_to_dnld)))

        return links_to_dnld

    # *******  *******   *******   *******   *******   *******   *******
    def extract_filepaths_filenames_dates(self, links_to_dnld):

        #TODO: irregular links, come here and screw this up. I should, at this point, apply the elegibility.
        # This means, takle the links, derive the filename, apply eligibility, and counter-delete the invalid links of ineligible filenames

        filenames = list(map(lambda link: os.path.split(link)[-1], links_to_dnld))
        filepaths = list(map(lambda filename: os.path.join(self.save_dir, filename), filenames))
        try:
            # well named files are named as: YYYYMMDD_ReportName_#vv.xls*
            dates = list(map(lambda filename: DateTime.date_magician(re.findall(r'\d{8}', filename)), filenames))

        except:
            # some files may have ill-formed names, such as: 20200722_ISP1_ISPResults_ISP1_2020-07-22_20200721140009.xlsx
            # even if one of those exists, the map() will break. so, doing it with a for loop

            dates = []
            for fn in filenames:
                try:
                    str_date = re.findall(r'\d{8}_', fn)
                    if len(str_date) == 0: # the file is very ill-formed and will be ignored (very rarely)
                        continue

                    str_date = str_date[0][:-1] # sdelect the first encounterd 8-digit long batch
                    dates.append(DateTime.date_magician(str_date))
                except:
                    self.logger.error("Failure while extracting dates from links / filepaths")
                    self.logger.error("Problematic file: {}".format(fn))
                    self.logger.error("Perceived str_date : {}".format(str_date))
                    print('Fatal error while trying to assess file: {}'.format(fn))

                    sys.exit()

        return filepaths, filenames, dates

    # *******  *******   *******   *******   *******   *******   *******
    @staticmethod
    def derive_market_components(report_name):
        _components = report_name.split('_') #
        # e.g. [DAM, Results], or [IDM, LIDA1, Results]
        index_where_market_name = len(_components) - 2
        market_name = _components[index_where_market_name] # DAM, LIDA1, CRIDA3, etc
        report_category = _components[-1] # Results, AggDemandSupplyCurves, etc.

        return market_name, report_category
###############################################################################################
###############################################################################################
###############################################################################################
