import ast
import json
import pathlib
import logging
import sys
from exso import Files
from exso.ReportsInfo import Report
from exso.HighLevel.Updater import Updater
from exso.HighLevel.Validation import Validation
from exso.IO.Tree import Tree
import pandas as pd
import re
import os
from colorama import Fore
import warnings
warnings.filterwarnings(action='ignore', category=DeprecationWarning)

pd.set_option('frame_repr',False)
pd.options.display.max_rows = None
pd.options.display.max_columns= None

# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
__version__ = "1.0.4"
logfile = Files.root_log #Path(tempfile.mktemp())
logging.basicConfig(filename=logfile,
                    level=logging.DEBUG,
                    filemode='w',
                    format='[%(asctime)s-%(levelname)s]  %(name)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    encoding='utf-8')
rootlogger = logging.getLogger(name='__main__')
logging.info("\nFirst Line\n")

files_dir = Files.files_dir

_dt_format = "%Y-%m-%d %H:%M"

_pbar_settings = {'bar_format': "%s{desc:<30} {percentage:5.2f}symbol| %s{bar:15}%s{r_bar}" % (Fore.LIGHTGREEN_EX,
                                                              Fore.YELLOW,
                                                              Fore.LIGHTYELLOW_EX),
                 'ncols':125,
                 'smoothing':1}

_pbar_settings['bar_format'] = re.sub('symbol', '%', _pbar_settings['bar_format'])


user_root_windows = pathlib.Path(os.environ['USERPROFILE'])
fp_default_datalake = user_root_windows / 'Desktop' / 'exso_data' / 'datalake'
fp_default_database = user_root_windows / 'Desktop' / 'exso_data' / 'database'


# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
class Settings:
    def __init__(self):
        self.fp_requirements = files_dir / 'refresh_requirements.txt'
        self.fp_system_formats = files_dir / 'system_formats.txt'
        rp = Report.Pool()
        self.avail_reports = list(rp.get_available(only_names=True))
        
    # *******  *******   *******   *******   *******   *******   *******

    def set_system_formats(self, decimal_sep='.', list_sep=','):
        with open(self.system_formats_file, 'w') as f:
            formats = {'decimal_sep': decimal_sep,
                       'list_sep': list_sep}
            json.dump(formats, f)

        exso._decimal_sep = decimal_sep
        exso._list_sep = list_sep
        exso._thousand_sep = ',' if decimal_sep == '.' else '.'

    # *******  *******   *******   *******   *******   *******   *******
    def get_system_formats(self):
        with open(self.system_formats_file, 'r') as f:
            formats = json.loads(f.read())
        _decimal_sep = formats['decimal_sep']
        _thousand_sep = ','
        _list_sep = formats['list_sep']
        return {'decimal': _decimal_sep, 'thousand': _thousand_sep, 'list': _list_sep}

    # *******  *******   *******   *******   *******   *******   *******
    def _interpret_refresh_requirements(self, force_refresh, force_no_refresh, previous = [], mode = 'a'):

        fr = force_refresh
        fnr = force_no_refresh
        if fr:
            if isinstance(fr, str):
                if fr == 'all':
                    to_refresh = self.avail_reports.copy()
                else:
                    to_refresh = [fr]
            else:
                to_refresh = fr
        else:
            to_refresh = []
        if mode == 'a':
            to_refresh = list(set(previous + to_refresh))
        if fnr:
            if isinstance(fnr, str):
                if fnr == 'all':
                    to_refresh = []
                else:
                    if fnr.lower() in [tr.lower() for tr in to_refresh]:
                        rmv_idx = [i for i,r in enumerate(to_refresh) if r.lower() == fnr.lower()][0]
                        to_refresh.pop(rmv_idx)
                    else:
                        warnings.warn(f"Report given ('{fnr}')as force-no-refresh was not part of the to-refresh reports anyway")
            else:
                for item_fnr in fnr:
                    rmv_idx = [i for i,r in enumerate(to_refresh) if r.lower() == item_fnr.lower()]
                    if rmv_idx:
                        to_refresh.pop(rmv_idx[0])
        else:
            pass
        to_refresh = sorted(to_refresh)
        return to_refresh

    # *******  *******   *******   *******   *******   *******   *******
    def get_refresh_requirements(self):
        with open(self.fp_requirements, 'r') as f:
            content = f.read()
            content = ast.literal_eval(content)

        to_refresh = self._interpret_refresh_requirements(content['force_refresh'],
                                                          content['force_no_refresh'])
        to_refresh = sorted(to_refresh)

        return to_refresh

    # *******  *******   *******   *******   *******   *******   *******
    def set_refresh_requirements(self, force_refresh=None, force_no_refresh=None, mode = 'a'):
        new_command =  self._interpret_refresh_requirements(force_refresh, force_no_refresh, previous=self.get_refresh_requirements(), mode = mode)
        if mode  == 'w':
            to_refresh = new_command
        else:
            previous = self.get_refresh_requirements()
            to_refresh = sorted(list((set(previous).intersection(set(new_command)))))
            # to_refresh = list(set(previous + new_command))

        fr = to_refresh
        fnr = []

        content = {'force_refresh': fr,
                   'force_no_refresh': fnr}
        print(content)
        with open(self.fp_requirements, 'w') as f:
            json.dump(content,f, indent=2)
    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******

settings = Settings()
