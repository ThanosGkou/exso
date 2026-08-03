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
                  'IDA1_AggrCurves', 'IDA1_Results', 'IDA1_ResultsSummary', 'IDA1_PrelimResults',
                  'IDA2_AggrCurves', 'IDA2_Results', 'IDA2_ResultsSummary', 'IDA2_PrelimResults',
                  'IDA3_AggrCurves', 'IDA3_Results', 'IDA3_ResultsSummary', 'IDA3_PrelimResults',
                  ]
    renamer = {'AggrCurves': 'AggDemandSupplyCurves',
               'BLKORDRs': 'BlockOrders',
               'POSNOMs': 'PhysicalDeliveriesOfftakes',
               'PrelimResults': 'MarketCoupling',
               'LIDA': 'IDM_LIDA',
               'CRIDA': 'IDM_CRIDA',
               'IDA': 'IDM_IDA'}

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
        print('\n\t---> IMPORTANT: Do not interrupt this process, despite if it seems stuck.')
        print('\t     ^^^^^^^^^')
        print('\t                (If by accident you do, it\'s best that you DELETE the whole datalake > henex directory, and re-launch the process.)\n\n')



        self.dry_run = dry_run

        # self.get_links()

        ghost_links, ghost_filepaths = GhostArchive(self.save_dir).get_links(match_str=None)

        warnings.warn('undo')
        self.links = ghost_links
        self.filepaths = ghost_filepaths

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
    def get_links(self, match_str = None):

        _links = self._links()
        if match_str:
            links = [v for k,v in _links.items() if bool(re.search(match_str,k))]
        else:
            links = list(_links.values())
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

        new_links = '''
        2025_EL-DAM_POSNOMs    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=d1de1152-4d24-b764-4dbf-919b22c4b447&groupId=20126
        2024_EL-DAM_POSNOMs    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=cdedfb01-be9d-608f-00d8-0d3fd8281a82&groupId=20126
        2023_EL-DAM_POSNOMs    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=2a3cf949-3a2b-f2dd-b17b-77fac6509085&groupId=20126
        2022_EL-DAM_POSNOMs    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=c7511729-19c5-639c-26ee-762c8957c416&groupId=20126
        2021_EL-DAM_POSNOMs    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=64f194a8-69dd-941f-c351-bbcf6e6223fb&groupId=20126
        2020_EL-DAM_POSNOMs    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=9b8f8495-c79e-d39c-c84f-08858572a0a9&groupId=20126
        2025_EL-DAM_NDPS    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=14182901-e52f-590c-ed40-632af84373bf&groupId=20126
        2024_EL-DAM_NDPS    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=5fbf67cd-71c0-1255-cc40-58c0e0c138cb&groupId=20126
        2023_EL-DAM_NDPS    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=65a45938-eb98-94ce-3943-309d31792ba0&groupId=20126
        2022_EL-DAM_NDPS    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=af4d000e-2f71-3340-db50-0559b8fe509f&groupId=20126
        2021_EL-DAM_NDPS    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=c5fffb78-b5e8-45f4-9709-3cdf2239e00c&groupId=20126
        2020_EL-DAM_NDPS    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=9197c767-0f7b-1379-5ab1-3532d358185e&groupId=20126
        2025_EL-DAM_PreMarketSummary    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=8fa9c811-9981-cd5e-6b19-ccbcaf8a5acd&groupId=20126
        2024_EL-DAM_PreMarketSummary    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=cfd8d260-2036-7aef-5fbf-ea7d2723de92&groupId=20126
        2023_EL-DAM_PreMarketSummary    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=3af8d71d-1b64-11b3-1424-fa2eb829e93a&groupId=20126
        2022_EL-DAM_PreMarketSummary    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=797a22b7-df0c-cedd-6370-7f637f35f665&groupId=20126
        2021_EL-DAM_PreMarketSummary    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=ee0ba923-ee56-df2f-f9dc-4c181d8f13ed&groupId=20126
        2020_EL-DAM_PreMarketSummary    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=18e42f73-b201-5ece-1463-31e1f0dc6485&groupId=20126
        2025_EL-DAM-IDAs_Results    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=73d205d3-0885-f876-b995-d73ad1b934b8&groupId=20126
        2024_EL-DAM-IDAs-CRIDAs_Results    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=6f2dc5b3-ef9b-b3a9-c935-ca44e7648b93&groupId=20126
        2023_EL-DAM-CRIDAs_Results    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=f543600f-3948-d3e9-c337-8ff7ee997794&groupId=20126
        2022_EL-DAM-CRIDAs_Results    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=6feb2af7-daa6-9d77-557c-db32ba4573a2&groupId=20126
        2021_EL-DAM-CRIDAs-LIDAs_Results    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=eb8ea827-88d7-10c3-16c1-356e386439ae&groupId=20126
        2020_EL-DAM-LIDAs_Results    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=3c970a88-5230-26ef-bac7-a9b0891a2b73&groupId=20126
        2025_EL-DAM-IDAs_PrelimResults    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=7dbc65e2-edd9-29c5-97f2-9202a15d3abd&groupId=20126
        2024_EL-DAM-IDAs-CRIDAs_PrelimResults    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=3565e0e1-b107-d954-dc13-249a1b38f116&groupId=20126
        2023_EL-DAM-CRIDAs_PrelimResults    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=2e26c9c0-ca2c-1f87-089a-42e0a62993b2&groupId=20126
        2022_EL-DAM-CRIDAs_PrelimResults    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=296b60a0-3bc8-c9f4-6b81-8f0e2d75104f&groupId=20126
        2021_EL-DAM-CRIDAs_PrelimResults    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=c4349734-b350-3fdb-40a4-b2f39b592667&groupId=20126
        2020_EL-DAM_PrelimResults    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=7c635ed9-abaf-c07b-4731-b4d3fee52b0c&groupId=20126
        2025_EL-DAM-IDAs_ResultsSummary    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=a514f49d-3658-f0e6-cbd1-d92963b61d27&groupId=20126
        2024_EL-DAM-IDAs-CRIDAs_ResultsSummary    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=be47d9c9-6c8b-5e84-6f1a-6ec6c4216a5c&groupId=20126
        2023_EL-DAM-CRIDAs_ResultsSummary    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=eec9f8c7-fd9e-7098-ea59-ab724e5fb040&groupId=20126
        2022_EL-DAM-CRIDAs_ResultsSummary    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=c2bb33c3-5fc2-d3d4-d737-8b5b031ce383&groupId=20126
        2021_EL-DAM-CRIDAs-LIDAs_ResultsSummary    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=9f4bc0c9-5dfd-a131-d0d0-c359bcf6dc53&groupId=20126
        2020_EL-DAM-CRIDAs-LIDAs_ResultsSummary    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=93bafd5a-8baa-488e-6f06-5cf95a6ec37d&groupId=20126
        2025_EL-DAM-IDAs_AggrCurves    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=e930fb3c-bd82-82cf-2443-098abf6ed46e&groupId=20126
        2024_EL-DAM-IDAs-CRIDAs_AggrCurves    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=2e29b6be-aacb-0fd7-9259-ba5e517de52f&groupId=20126
        2023_EL-DAM-CRIDAs_AggrCurves    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=607fa270-a082-b2eb-23fd-b146ecfdecf5&groupId=20126
        2022_EL-DAM-CRIDAs-LIDAs_AggrCurves    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=594436bd-396e-ec71-62bd-3e6de32c5d7f&groupId=20126
        2021_EL-DAM-CRIDAs-LIDAs_AggrCurves    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=46af3b38-0125-0454-bc6c-1abb663259f1&groupId=20126
        2020_EL-DAM-CRIDAs-LIDAs_AggrCurves    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=0df1023b-d425-e0c0-1194-b87103e66402&groupId=20126
        2025_EL-DAM_BLKORDRs    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=ddc5e316-ac22-1686-bb75-0eaf4806cdf7&groupId=20126
        2024_EL-DAM_BLKORDRs    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=8144dc57-435d-4b0a-e353-0dae3c5ccb00&groupId=20126
        2023_EL-DAM_BLKORDRs    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=5e080a80-bec8-7bc1-ab48-d8dae5025d63&groupId=20126
        2022_EL-DAM_BLKORDRs    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=72393753-dd51-37bd-2b20-16377916c1b0&groupId=20126
        2021_EL-DAM_BLKORDRs    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=1c55d0c4-1f8d-ea0b-80b2-f6c2631315e3&groupId=20126
        2020_EL-DAM_BLKORDRs    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=88aaf96c-def2-8a23-d2b7-944c3127176a&groupId=20126
        2025_EL-DAM-IDAs_AggrCurves    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=e930fb3c-bd82-82cf-2443-098abf6ed46e&groupId=20126
        2024_EL-DAM-IDAs-CRIDAs_AggrCurves    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=2e29b6be-aacb-0fd7-9259-ba5e517de52f&groupId=20126
        2023_EL-DAM-CRIDAs_AggrCurves    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=607fa270-a082-b2eb-23fd-b146ecfdecf5&groupId=20126
        2022_EL-DAM-CRIDAs-LIDAs_AggrCurves    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=594436bd-396e-ec71-62bd-3e6de32c5d7f&groupId=20126
        2021_EL-DAM-CRIDAs-LIDAs_AggrCurves    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=46af3b38-0125-0454-bc6c-1abb663259f1&groupId=20126
        2020_EL-DAM-CRIDAs-LIDAs_AggrCurves    https://www.enexgroup.gr/el/c/document_library/get_file?uuid=0df1023b-d425-e0c0-1194-b87103e66402&groupId=20126
        '''
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
                        73d205d3-0885-f876-b995-d73ad1b934b8&groupId=20126
                        '''

        _links = new_links.strip().split('\n')
        links = [l.strip().split('    ')[1].strip() for l in _links]
        identifiers = [l.strip().split('    ')[0].strip() for l in _links]

        return dict(zip(identifiers, links))

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
