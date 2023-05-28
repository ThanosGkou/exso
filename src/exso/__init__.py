import json
import logging
from exso import Files
from exso.ReportsInfo import Report
from exso.HighLevel.Updater import Updater
from exso.HighLevel.Validation import Validation
from exso.IO.Tree import Tree
import pandas as pd
import re
from colorama import Fore


pd.set_option('frame_repr',False)
pd.options.display.max_rows = None
pd.options.display.max_columns= None



__version__ = '0.0.0'
logfile = Files.root_log #Path(tempfile.mktemp())
logging.basicConfig(filename=logfile,
                    level=logging.DEBUG,
                    filemode='w',
                    format='[%(asctime)s-%(levelname)s]  %(name)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    encoding='utf-8')
rootlogger = logging.getLogger(name='__main__')
logging.info("malakas")
files_dir = Files.files_dir

system_formats_file = files_dir / 'system_formats.txt'
with open(system_formats_file, 'r') as f:
    formats = json.loads(f.read())

_decimal_sep = formats['decimal_sep']
_thousand_sep = ','
_list_sep = formats['list_sep']

_dt_format = "%Y-%m-%D %H:%M"

_pbar_settings = {'bar_format': "%s{desc:<30} {percentage:4.1f}symbol| %s{bar:30}%s{r_bar}" % (Fore.LIGHTGREEN_EX,
                                                              Fore.YELLOW,
                                                              Fore.LIGHTYELLOW_EX),
                 'ncols':115,
                 'smoothing':1}
_pbar_settings['bar_format'] = re.sub('symbol', '%', _pbar_settings['bar_format'])

def _modify_system_formats(decimal_sep='.', list_sep=','):
    system_formats_file = files_dir / 'system_formats.txt'
    with open(system_formats_file, 'w') as f:
        formats = {'decimal_sep':decimal_sep,
                   'list_sep':list_sep}

        json.dump(formats,f)