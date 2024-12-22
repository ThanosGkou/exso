import re, os
import shutil
from pathlib import Path
from zipfile import ZipFile
import logging
import haggis.string_util
import numpy as np
import pandas as pd


###############################################################################################
###############################################################################################
###############################################################################################
class ZipHandler:
    def __init__(self, zipped_dir:str|Path|None, zip_filepaths:list|None = None, extract_to_dir:str|Path|None=None, must_contain:str|None = '*.xls*', must_not_contain = None):
        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

        self.conform_input(zipped_dir, zip_filepaths, extract_to_dir, must_contain, must_not_contain)


    # *******  *******   *******   *******   *******   *******   *******
    def conform_input(self, zipped_dir, zip_filepaths, extract_to_dir, must_contain, must_not_contain):

        self.logger.info("Input given to zip-handler")
        self.logger.info(f'{zipped_dir = }')
        self.logger.info(f'{zip_filepaths = }')
        self.logger.info(f'{extract_to_dir = }')
        self.logger.info(f'{must_contain = }')
        self.logger.info(f'{must_not_contain = }')

        if zip_filepaths:
            zipped_dir = zip_filepaths[0].parent
            all_zips = self.scan_for_zipfiles(zipped_dir)
            ignore_zips = [zf for zf in all_zips if zf not in zip_filepaths]

        else:
            if isinstance(zipped_dir, str):
                zipped_dir = Path(zipped_dir)

            ignore_zips = []


        if not extract_to_dir:
            extract_to_dir = zipped_dir
        elif isinstance(extract_to_dir, str):
            extract_to_dir = Path(extract_to_dir)

        extract_to_dir.mkdir(exist_ok=True, parents=True)

        self.must_contain = must_contain
        self.must_not_contain = must_not_contain
        self.logger.info("Initiating unzipping process")
        self.logger.info("Directory in which to search for zipfiles: {}".format(zipped_dir))
        self.logger.info("Export payload files to dir: {}".format(extract_to_dir))
        self.zipped_dir = zipped_dir
        self.ignore_zips = ignore_zips
        self.extract_to_dir = extract_to_dir

        self.logger.info("Input, as interpreted by conform_input()")
        self.logger.info(f'{self.zipped_dir = }')
        self.logger.info(f'{zip_filepaths = }')
        self.logger.info(f'{self.extract_to_dir = }')
        self.logger.info(f'{self.must_contain = }')
        self.logger.info(f'{self.must_not_contain = }')
        self.logger.info(f'{self.ignore_zips = }')

    # *******  *******   *******   *******   *******   *******   *******
    def run(self, move_to_dst =True, delete_leftovers = True):
        self.deep_unzip()
        if move_to_dst:
            self.move_to_destination()

        if delete_leftovers:
            self.delete_leftovers()

    # *******  *******   *******   *******   *******   *******   *******
    def deep_unzip(self):
        visited = []
        zip_filepaths = self.scan_for_zipfiles(ignore = visited + self.ignore_zips)
        unzipped_dirs = []
        self.logger.info("Initiated deep-unzip process.")

        while zip_filepaths:
            self.logger.info('Zip files that seem eligible to be assessed:')
            self.logger.info(haggis.string_util.format_list(zip_filepaths))
            for zf in zip_filepaths:

                if self.must_not_contain and bool(re.search(self.must_not_contain, zf.name)):
                    self.logger.info('\tIgnoring file because it includes {}'.format(self.must_not_contain))
                else:
                    unzip_to = zf.with_suffix('')
                    self.logger.info("\tUnzipping file {} to {}".format(zf, unzip_to))
                    unzipped_dirs.append(unzip_to)
                    self.unzip_file(zf, unzip_to)

                visited.append(zf)

            self.logger.info('End of recursion step. Going one step deeper.')
            zip_filepaths = self.scan_for_zipfiles(ignore = visited + self.ignore_zips)

        self.unzipped_dirs = unzipped_dirs
        self.visited_zipfiles = visited

    # *******  *******   *******   *******   *******   *******   *******
    def move_to_destination(self):
        self.payload = []

        for d in self.unzipped_dirs:
            self.logger.info("Moving files of dir {} to {}".format(d, self.extract_to_dir))
            if self.must_contain:
                eligible_files_per_dir = list(d.rglob(self.must_contain))
            else:
                eligible_files_per_dir = list(d.rglob('*'))

            if self.must_not_contain:
                eligible_files_per_dir = [f for f in eligible_files_per_dir if self.must_not_contain not in f.name]

            self.logger.info(haggis.string_util.format_list(eligible_files_per_dir, width=10,indent=2))

            src_files = [file for file in eligible_files_per_dir if (self.extract_to_dir / file.name).exists() == False and file.exists()]
            [shutil.move(file, self.extract_to_dir / file.name) for file in src_files]
            # self.payload.extend([self.extract_to_dir / file.name for file in src_files])

        if len(list(self.zipped_dir.glob('*'))) == 0:
            shutil.rmtree(self.zipped_dir)

    # *******  *******   *******   *******   *******   *******   *******
    def delete_leftovers(self):
        [os.remove(zf) for zf in self.visited_zipfiles]
        [shutil.rmtree(d,ignore_errors=True) for d in self.unzipped_dirs]

    # *******  *******   *******   *******   *******   *******   *******
    def scan_for_zipfiles(self, in_dir = None, ignore = None):

        if in_dir:
            scan_in_dir = in_dir
        else:
            scan_in_dir = self.zipped_dir
        zip_filepaths = list(scan_in_dir.rglob('*.zip'))
        if ignore:
            zip_filepaths = [zfp for zfp in zip_filepaths if zfp not in ignore]
        return zip_filepaths

    # *******  *******   *******   *******   *******   *******   *******
    def unzip_file(self, zipfile, to_dir):
        with ZipFile(zipfile, 'r') as zipper:
            zipper.extractall(to_dir)


    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******


# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
class PoolCleaner:
    def __init__(self, pool_dir:str|Path, new_root:str|Path, split_in_categs:list|None=None, renamer:dict|None=None):

        if isinstance(pool_dir, str):
            pool_dir = Path(pool_dir)
        if isinstance(new_root, str):
            new_root = Path(new_root)
        self.pool_dir = pool_dir
        self.new_root = new_root

        self.pool_files = list(self.pool_dir.glob('*'))

        if not split_in_categs:
            self.split_in_categs = self.derive_categs()
        else:
            self.split_in_categs = split_in_categs

        if not renamer:
            renamer = {}
        self.renamer = renamer


        self.make_conformed_renamer()

    # *******  *******   *******   *******   *******   *******   *******
    def derive_categs(self):
        unique_categs = np.unique(list(map(lambda x: re.sub(r'\d\d+', '', x.with_suffix("").name), self.pool_files)))
        unique_categs = np.unique(list(map(lambda x: re.sub('_EL-|_EN_v|- Copy', '', x).strip(), unique_categs)))
        return unique_categs

    # *******  *******   *******   *******   *******   *******   *******
    def make_conformed_renamer(self):
        categs_df = pd.DataFrame({'old_categs':self.split_in_categs, 'new_categs':self.split_in_categs})
        categs_df['new_categs'] = categs_df['new_categs'].replace(self.renamer, regex=True)
        new_categs = categs_df['new_categs'].values
        self.new_categs = new_categs

    # *******  *******   *******   *******   *******   *******   *******
    def clean(self):

        for i in range(len(self.split_in_categs)):
            rule = '*{}_*.xls*'.format(self.split_in_categs[i])
            categ_files = list(self.pool_dir.glob(rule))
            categ_dir = self.new_root / self.new_categs[i]
            categ_dir.mkdir(exist_ok=True, parents=True)
            [shutil.move(categ_file, categ_dir / categ_file.name) for categ_file in categ_files]

        if len(list(self.pool_dir.glob('*'))) == 0:
            shutil.rmtree(self.pool_dir)

    # *******  *******   *******   *******   *******   *******   *******

