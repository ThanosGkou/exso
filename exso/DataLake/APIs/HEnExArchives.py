import datetime
import logging
import re
import shutil
import warnings
from pathlib import Path
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from colorama import Fore
import exso
from exso.DataLake.APIs import ZipHandler
from exso.DataLake.APIs.Assistant import Assistant
from exso.Utils.DateTime import DateTime
from tqdm import tqdm

# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
decimal_f = '{:,}'
t1 = '\t'
t2 = 2*t1
t3 = 3*t1
t4 = 4*t1
n = '\n'
star = '*'*50
halfstar = '*'*25

date_lambda = lambda x: datetime.datetime.strftime(DateTime.date_magician(x, return_stamp = False), format="%d-%b-%y")



# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
class ArchiveScraper(Assistant):
    def __init__(self, save_dir:str|Path):
        ''' save_dir should be:
            Either an empty henex-report type (e.g. root_lake / henex / DAM_Results)
            '''

        self.links = []
        self.filepaths = []
        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        if isinstance(save_dir, str):
            save_dir = Path(save_dir)

        if len(list(save_dir.glob('*'))) != 0:
            warnings.warn("Caution: The directory provided was not empty. ({})".format(save_dir))
            input("Caution: The directory provided was not empty. ({}). Proceed? ".format(save_dir))

        self.save_dir = save_dir

        self.save_dir.mkdir(exist_ok = True)
        self.anchor_text_must_not_contain = None
        self.anchor_text_must_contain = None

    # *******  *******   *******   *******   *******   *******   *******
    def get_links(self):

        self.logger.info("\n\n\tGetting Links.")

        base_url = 'https://www.enexgroup.gr'
        archive_url = self.archive_url
        save_dir = self.save_dir
        anchor_text_must_contain = self.anchor_text_must_contain
        anchor_text_must_not_contain = self.anchor_text_must_not_contain

        all_anchors = self._get_anchors(archive_url)
        filtered_anchors = self._filter_anchors(anchors=all_anchors, anchor_text_must_contain=anchor_text_must_contain, anchor_text_must_not_contain=anchor_text_must_not_contain)
        links, filepaths = self._get_links_and_filepaths(filtered_anchors, base_url, save_dir)


        self.links += links
        self.filepaths += filepaths

    # *******  *******   *******   *******   *******   *******   *******

    def _get_links_and_filepaths(self, filtered_anchors, base_url, save_dir):
        file_names = [anchor.text for anchor in filtered_anchors]
        file_names = list(map(lambda x: x[re.search(r'\d{4}', x).start():].strip(), file_names))
        file_links = [base_url + anchor['href'] for anchor in filtered_anchors]

        zip_filepaths = [save_dir / fn for fn in file_names]

        self.logger.info("Now, valid file names are: {}".format(file_names))
        self.logger.info('And valid file links are: {}'.format(file_links))

        links, filepaths = self.get_non_trivial(file_links, zip_filepaths)

        return links, filepaths

    # *******  *******   *******   *******   *******   *******   *******
    def get_non_trivial(self, links, filepaths):
        non_trivial = [i for i, zfp in enumerate(filepaths) if zfp.exists() == False]
        filepaths = [filepaths[i] for i in non_trivial]
        links = [links[i] for i in non_trivial]

        return links, filepaths

    # *******  *******   *******   *******   *******   *******   *******
    def _get_anchors(self, archive_url, href_prefix = '"/el/c/document_library"'):

        response = requests.get(archive_url).text
        soup = BeautifulSoup(response, 'html.parser')
        anchors = soup.select('a[href^={}]'.format(href_prefix))

        return anchors

    # *******  *******   *******   *******   *******   *******   *******
    def _filter_anchors(self, anchors, anchor_text_must_contain = None, anchor_text_must_not_contain = None):

        file_names = [anchor.text for anchor in anchors] #['  \xa0 2022_EL-DAM_POSNOMs.zip ', ....]

        self.logger.info("All anchors that represent links:")
        self.logger.info(file_names)


        if anchor_text_must_not_contain:
            self.logger.info('Will drop any name that contains: {}'.format(anchor_text_must_not_contain))
            file_names = [filename for filename in file_names if not anchor_text_must_not_contain in filename]
            self.logger.info("Now, valid file names are: {}".format(file_names))

        if anchor_text_must_contain:
            self.logger.info('Will keep ONLY anything that  that contains: {}'.format(anchor_text_must_contain))
            file_names = [filename for filename in file_names if anchor_text_must_contain in filename]
            self.logger.info("Now, valid file names are: {}".format(file_names))

        self.logger.info('Updating valid anchors after keep/drop operations.')

        filtered_anchors = [a for a in anchors if a.text in file_names]

        return filtered_anchors


    # *******  *******   *******   *******   *******   *******   *******
    def rename(self):
        tqdm._instances.clear()
        pbar = tqdm(range(len(self.categories)),
                    desc='\tRenaming for compatibility: ',
                    **exso._pbar_settings)

        keys = list(self.renamer.keys())
        new_categories = []
        for i in pbar:
            categ_name = self.categories[i]
            pbar.set_postfix_str(s=categ_name)
            categ_dir = self.unzipped_dir / categ_name
            if 'LIDA' in categ_name or 'CRIDA' in categ_name or 'XBID' in categ_name:

                new_categ_dir_name = 'IDM_' + categ_name
            else:
                new_categ_dir_name = categ_name

            for k in keys:
                if k in categ_name:
                    new_categ_dir_name = re.sub(k, self.renamer[k], new_categ_dir_name)
                    break

            new_categ_dir = self.unzipped_dir / new_categ_dir_name
            self.logger.info('New categ-directory: {}'.format(new_categ_dir))
            new_categories.append(new_categ_dir_name)
            shutil.move(categ_dir, new_categ_dir)

        self.new_categories = new_categories


# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
class MarketArchive(ArchiveScraper):
    categories = ['DAM_AggrCurves', 'DAM_BLKORDRs', 'DAM_NDPS', 'DAM_POSNOMs', 'DAM_PreMarketSummary',
                  'DAM_Results', 'DAM_ResultsSummary', 'DAM_PrelimResults',
                  'LIDA1_AggrCurves', 'LIDA1_Results', 'LIDA1_ResultsSummary',
                  'LIDA2_AggrCurves', 'LIDA2_Results', 'LIDA2_ResultsSummary',
                  'LIDA3_AggrCurves', 'LIDA3_Results', 'LIDA3_ResultsSummary',
                  'CRIDA1_AggrCurves', 'CRIDA1_Results', 'CRIDA1_ResultsSummary', 'CRIDA1_PrelimResults',
                  'CRIDA2_AggrCurves', 'CRIDA2_Results', 'CRIDA2_ResultsSummary', 'CRIDA2_PrelimResults',
                  'CRIDA3_AggrCurves', 'CRIDA3_Results', 'CRIDA3_ResultsSummary', 'CRIDA3_PrelimResults',
                  ]
    renamer = {'AggrCurves': 'AggDemandSupplyCurves',
               'BLKORDRs': 'BlockOrders',
               'POSNOMs': 'PhysicalDeliveriesOfftakes',
               'PrelimResults': 'MarketCoupling',
               'LIDA': 'IDM_LIDA',
               'CRIDA': 'IDM_CRIDA'}

    def __init__(self, save_dir:str|Path):
        ''' save_dir should be:
            Either an empty henex-report type (e.g. root_lake / henex / DAM_Results)
            '''
        super().__init__(save_dir)

        self.archive_url = 'https://www.enexgroup.gr/el/dam-idm-archive'
        self.anchor_text_must_not_contain = 'MWO'
        self.anchor_text_must_contain = None


    # *******  *******   *******   *******   *******   *******   *******
    def query(self, report_name, dry_run = False, n_threads = 1):
        print('\n\tRequested report was: {}'.format(report_name))
        print('\tBut, because HEnEx API is a joke*, all HEnEx archive (which is also a joke**) will be downloaded now. Then, it will be available for direct usage.')
        print('\n\t\t*HEnEx API is a joke because, only some report-types can be downloaded, and only within approximately the last year. \n\t\t Previous data cannot be downloaded')
        print('\n\t\t**HEnEx Archive is also a joke, because it used to be one zip per report per year, again, only for some data. \n\t\t  Now, they mixed report-types together, and still, they have mistakes in years and missing data.')
        print('\n\tThus, this cold-start overhead is necessary for later smooth usage. \n\tAfter that, the henex api or scraping process are used to get the recent files (i.e. belonging in the current calendar year)')
        print('\n\tTotal Size ~1GB, so may take up to 5-20 minutes, depending on you internet speed. \n\t(The progress bar in multi-threaded zipfiles is not always representative. Even if it seems stuck, leave it running)')
        print('\n\t---> IMPORTANT: Do not interrupt this process.')
        print('\t     ^^^^^^^^^')
        print('\t                (If by accident you do, it\'s best that you DELETE the whole datalake > henex directory, and re-launch the process.)\n\n')



        self.dry_run = dry_run

        self.get_links()

        ghost_links, ghost_filepaths = GhostArchive(self.save_dir).get_links()
        self.links += ghost_links
        self.filepaths += ghost_filepaths

        unique_ids = [i for i,l in enumerate(np.unique(self.links)) if l in self.links]
        self.links = [self.links[i] for i in unique_ids]
        self.filepaths = [self.filepaths[i] for i in unique_ids]


        self.n_links = len(self.links)
        self.link_dates = list(map(lambda x: re.findall(r'\d+', str(x)), self.filepaths))

        self.download(self.links, self.filepaths, n_threads=n_threads)

        zh = ZipHandler.ZipHandler(zipped_dir = self.save_dir, extract_to_dir = self.save_dir, must_not_contain = 'DryRun')
        zh.run(move_to_dst = True, delete_leftovers = True)

        pool_cleaner = ZipHandler.PoolCleaner(pool_dir = self.save_dir, new_root = self.save_dir.parent, split_in_categs = self.categories, renamer = self.renamer)
        pool_cleaner.clean()


# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
class GhostArchive(MarketArchive):
    def __init__(self, save_dir:Path|str):
        super().__init__(save_dir)


    # *******  *******   *******   *******   *******   *******   *******
    def get_links(self):
        links = self._links()
        filepaths = [self.save_dir / str(i) + '.zip' for i in range(len(links))]
        links, filepaths = self.get_non_trivial(links, filepaths)
        return links, filepaths

    # *******  *******   *******   *******   *******   *******   *******
    @staticmethod
    def _links():
        ''' should be obsolete, since henex changed its archive format (again). But some links show up.
            This shouldn't be needed, but in the new henex archive format they forgot some files...............
            The below links are not retrievable by any official site anymore. But they still work...'''

        root = 'https://www.enexgroup.gr/el/c/document_library/get_file?uuid='

        all_links = ''' 9b8f8495-c79e-d39c-c84f-08858572a0a9&groupId=20126
                        2cd5c3fa-70cf-68fc-1c4f-2d850b4328a5&groupId=20126
                        7c635ed9-abaf-c07b-4731-b4d3fee52b0c&groupId=20126
                        a3f0ed1a-819e-de60-5c59-55b6eb99579d&groupId=20126
                        88aaf96c-def2-8a23-d2b7-944c3127176a&groupId=20126
                        23d04ca0-5850-415b-4c7b-d0a65ed293cf&groupId=20126
                        3cac3027-f686-4517-e1d8-317e51d83558&groupId=20126
                        a567a718-ff96-2200-c46e-717264b690b9&groupId=20126
                        11fa9c10-ebf8-7770-5e0f-92ff8d5749fc&groupId=20126
                        737bafea-0545-3f30-98d6-48506cb1aed5&groupId=20126
                        076c799b-10a9-14ad-522a-99de33b7895d&groupId=20126
                        1211aea5-b6b1-344b-a4b9-8d605446f7fb&groupId=20126
                        9a9582d7-0e7b-3c54-ce2d-400e28d657d0&groupId=20126
                        7b28acd0-b52a-7fee-e984-499a5322bbc9&groupId=20126
                        61a185d5-81ac-65b0-1d3f-0005209a1462&groupId=20126
                        64f194a8-69dd-941f-c351-bbcf6e6223fb&groupId=20126
                        1ae3f4d1-acb7-263a-fc25-dc46fc9139a0&groupId=20126
                        c7579bf1-9916-dc48-0d40-f5900cfa78e9&groupId=20126
                        8b71e484-a6ca-b2b7-20b2-ea9e4b21f7e0&groupId=20126
                        1c55d0c4-1f8d-ea0b-80b2-f6c2631315e3&groupId=20126
                        f176f94e-b760-b485-e5a9-b14fbf8f688a&groupId=20126
                        1983d47d-c1cd-1911-1656-6dba6b766450&groupId=20126
                        a0cd4b15-1254-bd58-efa7-f7b0ac6ce0a4&groupId=20126
                        aa59409d-87a7-dd2f-4602-286fe01730a0&groupId=20126
                        1983d47d-c1cd-1911-1656-6dba6b766450&groupId=20126
                        a0cd4b15-1254-bd58-efa7-f7b0ac6ce0a4&groupId=20126
                        aa59409d-87a7-dd2f-4602-286fe01730a0&groupId=20126
                        57bfe9f8-2bcd-7881-48d1-d13a1199b3f5&groupId=20126
                        0fdaf901-cf73-9d68-7c46-2d6e1ba699d2&groupId=20126
                        d423a7cc-042f-cb98-44a1-1e2f66f8d17b&groupId=20126
                        1eeccb96-8154-d730-b3e9-00946d3208b4&groupId=20126
                        b7ebf9df-dadc-ac3d-9bbc-0a3c7f91eb4d&groupId=20126
                        abcd6695-65e6-c248-a64d-22a0d8be865f&groupId=20126
                        1eeccb96-8154-d730-b3e9-00946d3208b4&groupId=20126
                        b7ebf9df-dadc-ac3d-9bbc-0a3c7f91eb4d&groupId=20126
                        abcd6695-65e6-c248-a64d-22a0d8be865f&groupId=20126
                        25443a0c-0730-cfb5-95d6-3c8be5fbe2b5&groupId=20126
                        df17b277-40ab-b813-9af4-72421258d7f9&groupId=20126
                        2383d957-fabe-4c91-35e4-b24cfbf9bfbd&groupId=20126
                        25443a0c-0730-cfb5-95d6-3c8be5fbe2b5&groupId=20126
                        df17b277-40ab-b813-9af4-72421258d7f9&groupId=20126
                        2383d957-fabe-4c91-35e4-b24cfbf9bfbd&groupId=20126
                        '''
        all_links = all_links.strip().split('\n')
        all_links = list(map(lambda x: root + x.strip(), all_links))
        return all_links

    # *******  *******   *******   *******   *******   *******   *******

# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
class SystemArchive(ArchiveScraper):
    def __init__(self, save_dir:str|Path):
        ''' save_dir should be:
            Either an empty henex-report type (e.g. root_lake / henex / DAM_Results)
            '''

        super().__init__(save_dir)

        self.anchor_text_must_not_contain = 'MWO'
        self.anchor_text_must_contain = None

        self.archive_url = 'https://www.enexgroup.gr/el/day-ahead-scheduling-archive'

    # *******  *******   *******   *******   *******   *******   *******
    def query(self, report_name, dry_run = False, n_threads = 1):
        print('\n\tRequested report was: {}'.format(report_name))
        print('\tThis is normally an ADMIE-published report, but the archived files are kept in HEnEx.')
        print('\n\t---> IMPORTANT: Do not interrupt this process.')
        print('\t     ^^^^^^^^^')
        print('\t                (If by accident you do, it\'s best that you DELETE the whole datalake > henex > {} directory, and re-launch the process.)\n\n'.format(report_name))


        self.dry_run = dry_run
        self.anchor_text_must_not_contain = 'Report'
        self.anchor_text_must_contain = report_name


        for page_number in [1, 2, 3, 4]:
            self.archive_url = 'https://www.enexgroup.gr/el/web/guest/day-ahead-scheduling-archive?p_p_id=com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_INSTANCE_6eBaUXF5VIb7&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view&_com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_INSTANCE_6eBaUXF5VIb7_delta=5&p_r_p_resetCur=false&_com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_INSTANCE_6eBaUXF5VIb7_cur={}'.format(page_number)
            try:
                self.get_links()
            except:
                pass

        self.n_links = len(self.links)
        self.link_dates = list(map(lambda x: re.findall(r'\d+', str(x)), self.filepaths))
        self.download(self.links, self.filepaths, n_threads = n_threads)

        zh = ZipHandler.ZipHandler(zipped_dir=self.save_dir, extract_to_dir=self.save_dir, must_not_contain='DryRun')
        zh.run(move_to_dst=True, delete_leftovers=True)

    # *******  *******   *******   *******   *******   *******   *******
