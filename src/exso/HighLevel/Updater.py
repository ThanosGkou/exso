import datetime
import json
import logging
import re
import shutil
import sys
import time
import traceback
from pathlib import Path

import colorama
from colorama import Fore

import pandas as pd
import rich
from exso import Files
from exso.DataBase import DataBase
from exso.DataLake import DataLake
from exso.IO.Tree import Tree
from exso.ReportsInfo import Report
from exso.Utils.DateTime import DateTime
from haggis import string_util as hag


###############################################################################################
###############################################################################################
###############################################################################################
class Updater:
    """ The main API-class of the exso project to update datasets.
        Check out the __init__.__doc__ for more information """
    def __init__(self, root_lake:str|Path='datalake', root_base:str|Path='database', reports_pool:Report.Pool|None = None, which:str|list = 'all', groups:None|list = None, only_ongoing:bool = False):
        """
        Constructor parameters for the Updater class:

        :param root_lake: [str|Path] The desired path to use as the datalake directory
        :param root_base: [str|Path] the desired path to use as the database directory
        :param reports_pool (optional): [Report.Pool] If you have instantiated a Report.Pool object, you can pass it to avoid re-instantiation
        :param which (optional): [list|None] If None, will update everything (unless otherwise specified through "groups" or "only_ongoing"). If not None, give a list or a str, with the specific report-names that you want to update.
        :param groups (optional): [list|None] Give a list of groups that you want to update
        :param only_ongoing (optional): [bool|False] if True, will only update reports that are still actively updated.

        *Default settings dictate update of all reports..

        Usage: - Instantiate an Updater object.
               - call the .run() method

        The first time of datalake/database creation for all available reports, the runtime may up to 7-8 hours depending on system-specs.
        Then, every e.g. weekly update is a matter of minutes.
        For more information on available reports, you can instantiate a Report.Pool object, and access the .get_available() method.
        """

        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.logger.info("Initializing Updater.")

        self.set_dirs(root_lake, root_base)
        self.start_date = None
        self.end_date = None

        self.rp = self.get_pool(reports_pool)
        self.keep_steps = False
        self.mode = None
        self.report_names = self.derive_reports(self.rp, which, groups, only_ongoing)


        self.refresh_requirements_file = Files.files_dir / 'refresh_requirements.txt'
        self.fulfilled_refreshes_file = Files.files_dir / 'fulfilled_refreshes.txt'

        self.refresh_requirements = self.read_refresh_requirements()

    # *******  *******   *******   *******   *******   *******   ******* >>> Logging setup
    def set_dirs(self, root_lake, root_base):
        if isinstance(root_lake, str):
            root_lake = Path(root_lake)
        if isinstance(root_base, str):
            root_base = Path(root_base)

        self.ensure_intention(root_lake, root_base)

        self.root_lake = root_lake
        self.root_base = root_base


        self.logger.info("Root Lake dir: {}".format(self.root_lake))
        self.logger.info("Root Base dir: {}".format(self.root_base))

    # *******  *******   *******   *******   *******   *******   ******* >>> Logging setup
    def ensure_intention(self, root_lake, root_base):
        if root_lake.exists() == False and root_base.exists() == False:
            print('\n\n----->>>> Confirm that you provided the intended paths for database/datalake? (if correct, just hit enter)')
            print('\t\tRoot lake:', root_lake.absolute())
            print('\t\tRoot base:', root_base.absolute())

            answer = input('\n'+'\t'*10 + '--> Answer here [y]/n: ')
            if answer == 'y' or not answer:
                print('\n\nProceeding.')
                pass
            else:
                print('\n\nExiting.')
                sys.exit()
            return

    # *******  *******   *******   *******   *******   *******   ******* >>> Logging setup
    def get_pool(self, reports_pool):
        if isinstance(reports_pool, type(None)):
            self.logger.info("No reports pool object provided. Will initiate it.")
            rp = Report.Pool()
        else:
            self.logger.info("Report Pool Object provided (hot-start, no need to re-initiate it)")
            rp = reports_pool
        return rp

    # *******  *******   *******   *******   *******   *******   *******
    @staticmethod
    def read_textfile(filepath):
        with open(filepath, 'r') as f:
            return json.loads(f.read())
    # *******  *******   *******   *******   *******   *******   *******
    def read_refresh_requirements(self):

        previously_fulfilled = self.read_textfile(self.fulfilled_refreshes_file)
        self.initially_fulfilled = previously_fulfilled.copy()

        reqs = self.read_textfile(self.refresh_requirements_file)

        # reqs['all'] = eval(reqs['all'])
        reqs['already_done'] = eval(reqs['already_done'])

        all_reports = self.rp.get_available()['report_name'].values
        requirements = {report_name: False for report_name in all_reports}

        # if reqs['all'] == True:
        #     requirements.update({rn:True for rn in all_reports})
        if reqs['which']:
            requirements.update({rn:True for rn in reqs['which']})
        elif reqs['all_except']:
            requirements.update({rn:True for rn in all_reports})
            requirements.update({rn:False for rn in reqs['all_except']})

        requirements.update({rn:False for rn in previously_fulfilled})

        self.logger.info("Refresh Requirements")
        self.logger.info("Input given: {}".format(reqs))
        self.logger.info("Previously fulfilled: {}".format(previously_fulfilled))
        self.logger.info("Reports deemed to need requirement: {}".format([r for r,v in requirements.items() if v]))
        print()

        return requirements

    # *******  *******   *******   *******   *******   *******   ******* >>> Logging setup
    def _set_debugging_options(self, options):
        '''

        :param options: {'base':{'new_dir':None, 'suffix':None},
                         'start_date': None | datetime-like,
                         'end_date':None|datetime-like}
        :return:
        '''
        if 'base' in options.keys():
            base_settings = options['base']
            if 'new_dir' in base_settings.keys():
                self.root_base = base_settings['new_dir']
            elif 'suffix' in base_settings.keys():
                self.root_base = self.root_base.parent / (self.root_base.name + base_settings['suffix'])

        if 'start_date' in options.keys():
            if options['start_date']:
                self.start_date = DateTime.date_magician(options['start_date'])


        if 'end_date' in options.keys():
            if options['end_date']:
                self.end_date = DateTime.date_magician(options['end_date'])

        self.mode = 'debugging'

    # *******  *******   *******   *******   *******   *******   *******
    def run(self, use_lake_version = 'latest', retroactive_update = False):

        self.refresh_requirements_fulfilled = []
        self.log_split = LogSplitter(root_logfile=Files.root_log, save_at_dir=Files.latest_logs_dir)

        self.logger.info('\n\n\n\n\n')
        self.logger.info("Running Update Kernel")
        self.failed = {}
        self.update_summary = {}
        t0 = time.perf_counter()

        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M')


        colorama.init(autoreset=True)
        print(Fore.LIGHTCYAN_EX + '\n\n--> Update started at: {} \n'.format(now))


        for report_name in self.report_names:

            print(Fore.LIGHTWHITE_EX + hag.make_box(report_name, style='bold-line', alignment='center', horizontal_padding=10, vertical_padding=1,))
            t = time.perf_counter()
            self.logger.info('\n' + '<{}>'.format(report_name))
            try:
                self.single(report_name, use_lake_version, retroactive_update)
                self.logger.info('\n\n++UpdateStatus:Success')
                if self.refresh_requirements[report_name] == True:
                    self.refresh_requirements_fulfilled.append(report_name)
                    self.update_fulfilled_refreshes()
                print(Fore.CYAN + hag.align('\n\t' + report_name + ' --> Succeeded\n\n\n', alignment = 'right', width = 10))
                self.update_summary[report_name] = {'Status':'Success'}

            except Exception as ex:
                self.logger.error("\n\nReport {} failed".format(report_name))
                self.logger.error('*'*50)
                self.logger.error("Immediate exception:")
                self.logger.error(ex)
                self.logger.error('*'*50)
                self.logger.error('Traceback:')

                trace = traceback.format_exc()
                self.logger.error(trace)
                self.logger.error('*'*50)
                self.logger.info('\n\n++UpdateStatus:Fail')

                self.update_summary[report_name] = {'Status':'Fail'}

                print(Fore.CYAN + hag.align('\t' + report_name + ' --> Failed !!\n', alignment = 'right', width = 10))
                print(hag.align(traceback.format_exc(), alignment = 'right', width = 10))
                print('\n\nMoving on ....\n\n')

            now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H_%M')
            self.update_summary[report_name]['Elapsed (sec)'] = round(time.perf_counter() - t,3)

            self.logger.info('\n\n++PerformedAt:{}'.format(now))
            self.logger.info('\n\n++Elapsed: {:.3f} sec'.format(time.perf_counter()-t))
            self.logger.info('\n' + '</{}>'.format(report_name))
            self.log_split.extract(report_name)
            self.log_split.export(report_name)

        self.logger.info("\n\n\n\n\nTotal Time: {}".format(time.perf_counter() - t0))
        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M')
        print(Fore.GREEN + '\n\n\n==> Ended at: ', now)
        elapsed = time.perf_counter() - t0

        successful = [report for report, content in self.update_summary.items() if content['Status'] == 'Success']

        self.update_summary['Totals'] = {'Total Time Elapsed (sec)': round(elapsed,3),
                                         '#Successful': len(successful),
                                         '#Failed': len(self.report_names) - len(successful),
                                         '#Total': len(self.report_names)}

        colorama.init(autoreset=True)
        with open(Files._logs_dir / 'update_summary.log', 'w') as f:
            [f.write(json.dumps({report:content}) + '\n') for report, content in self.update_summary.items()]

        print('\n==> Total Time Elapsed: {:.3f} sec ({:.2f} min)\n\n'.format(elapsed, elapsed/60 ))

        print(hag.make_box("Update Summary", style='bold-line', alignment='center', horizontal_padding=10, vertical_padding=1))
        for report, summary in self.update_summary.items():
            print(report)
            for k,v in summary.items():
                print('\t{}: {}'.format(k, v))




    # *******  *******   *******   *******   *******   *******   *******
    def update_fulfilled_refreshes(self):
        # if self.mode == 'debugging':
        #     self.logger.info("Not updateing fulfilled refreshes file, as this run was in debugging mode.")
        # else:

        fulfilled = self.initially_fulfilled.copy()
        print('previously fulfilled')
        print(fulfilled)

        fulfilled.extend(self.refresh_requirements_fulfilled)
        print('Now fulfilled')
        print(fulfilled)
        with open(self.fulfilled_refreshes_file, 'w') as f:
            f.write(json.dumps(fulfilled))
        self.logger.info("Successfully updated fulfilled refreshes file.")
        self.logger.info("Old values were:{}".format(self.initially_fulfilled))
        self.logger.info("New values are: {}".format(fulfilled))


    # *******  *******   *******   *******   *******   *******   *******
    def _modify_requirements(self, requirements, lake):
        if not self.start_date:
            self.start_date = lake.status.dates.min.potential.date
        if not self.end_date:
            self.end_date = lake.status.dates.max.potential.date

        requirements['start_date'] = self.start_date

        requirements['end_date'] = self.end_date

        requirements['range']= {'date': pd.date_range(requirements['start_date'], requirements['end_date'], freq='1D')}
        return requirements


    # *******  *******   *******   *******   *******   *******   ******* >>> Logging setup
    def single(self, report_name, use_lake_version, retroactive_update, keep_raw = False, refresh_database = False):
        self.logger.info('\n\n\n\t\tAssessing report type: {}'.format(report_name))

        r = Report.Report(self.rp, report_name, self.root_lake, self.root_base, api_allowed=True)
        if self.refresh_requirements[report_name]:
            self.logger.info("Refresh requirement = True. Deleting the existing database: {}".format(r.database_path))
            print('\t\tIt seems like you upgraded to the latest exso version, which brought some changes to the specific report ({}). This report\'s data, just for this time, will be fully refreshed instead of just updated.'.format(report_name))
            shutil.rmtree(r.database_path, ignore_errors=True)

        lake = DataLake.DataLake(r, use_lake_version=use_lake_version, retroactive_update = retroactive_update)

        if self.mode == 'debugging':
            start_date = self.start_date
            end_date = self.end_date
        else:
            start_date = None
            end_date = None

        lake.update(start_date, end_date)

        base = DataBase.DataBase(r, db_timezone='UTC')
        requirements = base.get_update_requirements()

        if self.mode == 'debugging':
            requirements = self._modify_requirements(requirements, lake)

        if requirements:


            data = lake.query(dates_iterable=requirements['range']['date'], keep_raw=keep_raw)
            base.tree = Tree(root_path=base.tree.root.path, zero_depth_kind=base.tree.root.kind)
            base.tree.make_tree(from_dict=data,ignore_fruits=True)
            base.tree.make_dirs()
            base.update(data)

        self.lake = lake
        self.base = base
        self.r = r

    # *******  *******   *******   *******   *******   *******   ******* >>> Logging setup
    def derive_reports(self, rp, which:str|list|None, groups:str|list|None, only_ongoing:bool):

        report_names = None
        self.logger.info("Assessing what kind of update was requested.")
        self.logger.info(f"Provided arguments: {which = }, {groups = }, {only_ongoing = }")

        selected = None
        implemented = rp.get_available()

        if not which:
            which = []

        elif isinstance(which, str):
            if which == 'all':
                which = implemented.report_name.values
            else:
                which = [which]

        if not groups:
            groups = implemented['group'].to_list()
        elif isinstance(groups, str):
            groups = [groups]

        intersect = implemented[((implemented.report_name.isin(which))&
                                 (implemented.group.isin(groups)))]

        if only_ongoing:
            intersect = intersect[intersect.available_until.isna() == True].copy()

        report_names = intersect['report_name'].to_list()

        self.logger.info("Will update datasets based on argument '{}'".format(selected))
        self.logger.info("To-update datasets: {}".format(report_names))

        return report_names
    # *******  *******   *******   *******   *******   *******   ******* >>> Logging setup
    # *******  *******   *******   *******   *******   *******   *******


# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
class LogSplitter:
    report_logs = {}
    def __init__(self, root_logfile:str|Path, save_at_dir:None|str|Path = None):

        if isinstance(root_logfile, str):
            root_logfile  = Path(root_logfile)
        if isinstance(save_at_dir, str):
            save_at_dir = Path(save_at_dir)

        self.root_logfile = root_logfile
        self.save_at_dir = save_at_dir
        self.read()

    # *******  *******   *******   *******   *******   *******   *******
    def read(self):
        with open(self.root_logfile, 'r', encoding='utf-8') as f:
            raw_log = f.read()

        self.raw_log = raw_log

    # *******  *******   *******   *******   *******   *******   *******
    def report_existence(self, report_name):
        start_regex = fr'<{re.escape(report_name)}>'
        report_starter = re.findall(start_regex, self.raw_log)
        if report_starter: return True
        else: return False


    # *******  *******   *******   *******   *******   *******   *******
    def extract(self, report_name):

        exists = self.report_existence(report_name)
        if not exists:
            self.read()
            exists = self.report_existence(report_name)
            if not exists:
                return

        start_regex = fr'<{re.escape(report_name)}>'
        end_regex = fr'</{re.escape(report_name)}>'
        report_starters = re.findall(start_regex, self.raw_log)
        report_enders = re.findall(end_regex, self.raw_log)

        starter = re.search(report_starters[0], self.raw_log)
        ender = re.search(report_enders[0], self.raw_log)

        report_log = self.raw_log[starter.start():ender.end()]
        facts_raw = re.findall('\+\+.*', report_log)

        facts = {}
        [facts.update(self.events_cleaner(fact)) for fact in facts_raw]

        _warnings = re.findall('WARNING.*', report_log)
        if _warnings:
            print('\t\t*There were some warnings activated during the update process. Chances are that they are completely harmless, but if you were expecting something specific, have a look at the log files, at the very bottom. (C:/Users/yourusername/AppData/Local/Temp/exso/logs/latest_logs/{}.log)\n\n'.format(report_name))

        self.report_logs[report_name] = {'facts': facts,
                                        'log': report_log,
                                         'warnings':_warnings}


    # *******  *******   *******   *******   *******   *******   *******
    def export(self, report_name, save_at_dir = None):
        if report_name not in list(self.report_logs.keys()):
            self.extract(report_name)

        if save_at_dir:
            dir = save_at_dir
        else:
            dir = self.save_at_dir

        # performed_at = self.report_logs[report_name]['facts']['performedat']
        output_path = dir/ (report_name + '.log')

        if not dir.exists():
            dir.mkdir(exist_ok=True)

        if report_name not in self.report_logs.keys():
            return

        with open(output_path, 'w', encoding='utf-8') as f:
            for k, v in self.report_logs[report_name].items():
                if k != 'log':
                    if isinstance(v, list):
                        f.write('\n\n'+k + '\n')
                        count = 1
                        for single_warning in v:
                            f.write(f'\t{count} '+ single_warning + '\n')
                            count += 1
                    else:
                        f.write(k + ' --> \n\t' + json.dumps(v) + '\n\n\n')
                else:
                    f.write(k)
                    f.writelines(v)

    # *******  *******   *******   *******   *******   *******   *******
    def extractAll(self):
        self.read()
        report_starters = re.findall('<\w+>', self.raw_log)
        report_names = list(map(lambda x: re.sub('<|>', '', x), report_starters))
        for report_name in report_names:
            self.extract(report_name)

    # *******  *******   *******   *******   *******   *******   *******
    def exportAll(self, save_at_dir=None):
        for report_name in self.report_logs.keys():
            self.export(report_name, save_at_dir)

    # *******  *******   *******   *******   *******   *******   *******
    @staticmethod
    def events_cleaner(event):
        ''' an event starts with two "+" signs, and kas a key, a ":" and a value '''

        event = re.sub('\++', '', event).split(':')
        event = list(map(lambda x: x.strip().lower(), event))
        event = {event[0]:event[1]}
        return event
    # *******  *******   *******   *******   *******   *******   *******
