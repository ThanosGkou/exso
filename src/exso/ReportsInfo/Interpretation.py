import ast
import datetime
import re
import sys
import traceback

import pandas as pd, numpy as np # numpy used in eval. dont remove the import
from exso.DataLake.APIs.StreamHandler import StreamHandler
from exso.Utils.DateTime import DateTime

###############################################################################################

date_lambda = lambda x: datetime.datetime.strftime(DateTime.date_magician(x, return_stamp = False), format="%d-%b-%y")

# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
class Metadata:
    def get_metadata(self, json):

        self.report_name = json['report_name']
        self.alias = json['alias']
        self.group = json['group']
        self.publisher = json['publisher']
        self.available_from = json['available_from']
        self.available_until = json['available_until'] # this requires further handling

        self.is_useful = json['is_useful']
        self.is_implemented = json['is_implemented']
        self.is_alive = json['is_alive']
        self.comment = json['comment']

        Metadata.interpret(self)
    # *******  *******   *******   *******   *******   *******   *******

    def interpret(self):
        if isinstance(self.available_from, datetime.datetime):
            self.available_from = self.available_from.date()
        else:
            pass
        if not self.alias:
            self.alias = self.report_name

    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******


# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
class ReadingSettings:

    def get_reading_settings(self, json):
        self.file_format = json['file_format']
        self.__eligibility_file = json['eligibility_file']  #
        self.lake_version = json['lake_version']  #
        self.sheet_locators = json['sheet_locators']  #
        self.sheet_tags = json['sheet_tags']  #
        self.header = json['header_row']
        self.use_cols = json['use_cols']  #

        ReadingSettings.interpret(self)

    # *******  *******   *******   *******   *******   *******   *******
    def interpret(self):
        self.eligibility = self.__interpret_eligibility(self.__eligibility_file, self.config_dir)
        self.lake_version = self.__interpret_lake_version(self.lake_version)

        if isinstance(self.sheet_locators, type(None)):
            self.sheet_locators = [0]
        else:
            self.sheet_locators = self.__list_parser(self.sheet_locators)

        if isinstance(self.sheet_tags, type(None)):
            self.sheet_tags = [self.report_name]
        else:
            self.sheet_tags = self.__list_parser(self.sheet_tags)

        self.header = 0 if not self.header else int(self.header)
        self.use_cols = self.__interpret_use_cols(self.use_cols)

    # *******  *******   *******   *******   *******   *******   *******
    def __interpret_eligibility(self, eligib_filename, config_dir):

        if not eligib_filename:
            eligib_filename = 'generic.txt'

        eligib_filepath = config_dir / 'datalake_eligibility' / eligib_filename

        with open(eligib_filepath, 'r') as f:
            content = f.read()

        eligibility = ast.literal_eval(content)

        if not 'extension_filter' in eligibility.keys():
            eligibility['extension_filter'] = self.file_format
        return eligibility

    # *******  *******   *******   *******   *******   *******   *******
    def __interpret_lake_version(self, lake_version):
        if not lake_version:
            lake_version = 'latest'
        else:
            lake_version = int(lake_version)
        return lake_version

    def __interpret_use_cols(self, use_cols):
        if use_cols:
            use_cols = eval(use_cols)
        return use_cols
    # *******  *******   *******   *******   *******   *******   *******
    @staticmethod
    def __list_parser(string):
        listed  = ast.literal_eval(string)
        return listed

    # *******  *******   *******   *******   *******   *******   *******


# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
class ParsingSettings:
    def get_parsing_settings(self, json, sheet_tags):
        self.cuepoints_file = json['cuepoints_file'] #
        self.renamer_mapping = json['renamer_mapping'] #
        self.replacer_mapping = json['replacer_mapping'] #
        self.regex_mapping = json['regex_mapping'] #
        self.time_axis = json['time_axis']
        self.long_format = json['long_format']
        self.persistent_long = json['persistent_long']

        ParsingSettings.interpret(self, sheet_tags)


    # *******  *******   *******   *******   *******   *******   *******
    def interpret(self, sheet_tags):
        self.cue_dict = self.__interpret_cuepoints(sheet_tags)
        self.renamer_mapping = self.__interpret_mappings(self.renamer_mapping, search_in_dir=self.config_dir / 'renamers')
        self.replacer_mapping = self.__interpret_mappings(self.replacer_mapping, search_in_dir=self.config_dir / 'replacers')
        self.regex_mapping = self.__interpret_mappings(self.regex_mapping, search_in_dir=self.config_dir / 'regex')
        self.long_format = False if not self.long_format else True
        self.persistent_long = False if not self.persistent_long else True

        self.cue_summary = {sheet_tag: [cue_name for cue_name in self.cue_dict[sheet_tag].keys()] for sheet_tag in self.cue_dict.keys()}

    # *******  *******   *******   *******   *******   *******   *******
    def __interpret_cuepoints(self, sheet_tags):
        if self.cuepoints_file:
            cuepoints_filepath = self.config_dir / 'cuepoints' / self.cuepoints_file

            with open(cuepoints_filepath, 'r', encoding='utf-8') as content:
                raw_cues = content.read()

            cue_rules = CuePoints.parse_cue_points_content(raw_cues)

        else:
            cue_rules = CuePoints.construct_placeholder_cue_points(sheet_tags)

        return cue_rules

    # *******  *******   *******   *******   *******   *******   *******
    def __interpret_mappings(self, mapping, search_in_dir):
        if not mapping:
            mapping = {}

        else:
            attempt_filepath = search_in_dir / mapping
            if attempt_filepath.is_file():
                # dont use exso._list_sep: these files are provided by exso, and use list_sep = ',' regardless of the user system's formats
                df = pd.read_csv(attempt_filepath, header=0, index_col=None)
                mapping = dict(zip(df.iloc[:, 0].values, df.iloc[:, 1].values))
            else:
                mapping = eval(mapping)  # dictionary was given as string

        return mapping


# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
class TimeSettings:
    def get_time_settings(self, json, system_tz):
        self.system_tz = system_tz
        self.period_covered = json['period_covered']
        self.publication_frequency = json['publication_frequency']
        self.publication_time = json['publication_time']
        self.inherent_tz = json['inherent_tz']
        self.start_hour = json['start_hour']
        self.resolution = json['resolution']
        self.time_lag_days = json['time_lag_days']

        TimeSettings.interpret(self)

    # *******  *******   *******   *******   *******   *******   *******
    def interpret(self):
        self.period_covered = self.duration_converter(self.period_covered, to_freq='D')
        self.publication_frequency = self.duration_converter(self.publication_frequency, to_freq='D')
        self.inherent_tz = self.__interpret_timezone()
        self.start_hour = self.__interpret_start_hour()
        if bool(re.search('[DWMY]', self.resolution)):
            self.requires_tz_handling = False
        else:
            self.requires_tz_handling = True

    # *******  *******   *******   *******   *******   *******   *******
    @staticmethod
    def duration_converter(duration, to_freq, from_freq = None):
        if not duration:
            return None
        if 'M' or 'Y' in duration:
            return duration

        if isinstance(duration, str):
            duration, from_freq = TimeSettings.split_numbers_letters(duration)

        matrix = pd.DataFrame({'sec':[1,              60,           3600,      3600*24, 3600*24*7, 3600*24*30, 3600*8760],
                               'min':[1/60,           1,            60,        60*24,   60*24*7,   60*24*30,   60*8760],
                               'H':  [1/3600,         1/60,         1,         24,      24*7,      24*30,      8760],
                               'D':  [1/(3600*24),    1/(24*60),    1/24,      1,       7,         30,         365],
                               'W':  [1/(3600*24*7),  1/(60*24*7),  1/(24*7),  1/24,    1,         4,          52],
                               'M':  [1/(3600*24*30), 1/(60*24*30), 1/(24*30), 1/30,    1/7,       1,          12],
                               'Y':  [1/(3600*8760),  1/(8760*60),  1/(8760),  1/365,   1/52,      1/12,       1]
                               })
        matrix.index = ['sec', 'min', 'H', 'D', 'W', 'M', 'Y']
        converted = int(matrix.loc[from_freq, to_freq] * duration)
        if converted == 0:
            converted = str(duration) + from_freq
        else:
            converted = str(converted) + to_freq
        return converted

    # *******  *******   *******   *******   *******   *******   *******
    @staticmethod
    def __interpret_frequencies(string):
        if not string:
            sys.exit("Period Covered property not given. Fatal.")

        steps, units = TimeSettings.split_numbers_letters(string)

        daily_frequency = string

        if units == 'D': pass
        if units == 'W':  daily_frequency = str(units * 7) + 'D'
        elif units == 'M': daily_frequency = str(units*30) + 'D'
        elif units == 'Y': daily_frequency = str(units * 365) + 'D'
        else: sys.exit("Dont know what to do with given frequency ({})".format(string))

        return daily_frequency

    # *******  *******   *******   *******   *******   *******   *******
    def __interpret_timezone(self):
        if not self.inherent_tz: return 'UTC'
        else: return self.inherent_tz

    # *******  *******   *******   *******   *******   *******   *******
    def __interpret_start_hour(self):
        if not self.start_hour: return 0
        else: return self.start_hour

    # *******  *******   *******   *******   *******   *******   *******
    @staticmethod
    def split_numbers_letters(string):
        text = re.findall('[A-Za-z]+', string)[0]  # M, D, H, Y, W
        number = re.findall(r'\d+', string)
        if not number:
            number = 1
        else:
            number = int(number[0])

        return number, text

    # *******  *******   *******   *******   *******   *******   *******
    def __interpret_available_until(self, report_name, publisher, lake_dir,  available_until, api_allowed=True, is_alive = True):

        if available_until == 'unavailable':
            raise IOError("Selected report-type ({}) is not available in ADMIE".format(report_name))

        elif isinstance(available_until, datetime.datetime):
            lake_max_date = available_until.date()

        else:  # aN --> file is still ongoing and needs regular updates
            if api_allowed and is_alive:
                lake_max_date = self.sniff_api(report_name, publisher, lake_dir)
            else:
                lake_max_date = datetime.datetime.today().date()
        return lake_max_date


    # *******  *******   *******   *******   *******   *******   *******
    def sniff_api(self, report_name, publisher, lake_dir):

        now = pd.Timestamp(datetime.datetime.now()).tz_localize(self.system_tz).tz_convert(self.inherent_tz)

        today = now.date()
        api = StreamHandler(save_dir=lake_dir)


        try:
            start_date = today - DateTime.disambiguate_timedelta(today, self.period_covered, return_timedelta=True)
        except:
            print('Weird error')
            print(traceback.format_exc())
            input('-X')
        sys.stdout = None
        try:
            start_date = today - DateTime.disambiguate_timedelta(today, self.period_covered, return_timedelta = True)
            end_date = today + pd.Timedelta(1,'D')
            api.query(report_name,
                      start_date=start_date,
                      end_date= end_date,
                      publisher=publisher,
                      dry_run=True)

            successful_dates = api.link_dates  # list of datetime.date objects, or empty list

        except:
            self.logger.warning("Connection failed. WIll assuume that lake is up-to-date.")
            self.logger.warning(traceback.format_exc())
            successful_dates = []

        sys.stdout = sys.__stdout__
        if len(successful_dates):
            self.logger.info('\n\n\t\tMade test api call. Latest dates: \n{}'.format(
                str(list(map(date_lambda, successful_dates)))))

            # in cases of scrapers, links arrive in descending order -> so, latest date is first list item
            # but in other cases, the latest date is the last list item. So, get the max
            lake_max_date = max(successful_dates)
            # lake_max_date = successful_dates[-1]
            # lake_max_date = successful_dates[0] # why I was using the first ?
            # if self.publisher == 'entsoe':
            #     lake_max_date = successful_dates[-1]
            self.logger.info('Latest date acquired from sniff_api: {}'.format(lake_max_date))
        else:
            self.logger.info("No request was successful for the query from: {}, to: {}".format(start_date, end_date))
            self.logger.info("Assuming that the maximum potential lake date is 1 period before the first failure")
            lake_max_date = start_date
            # lake_max_date = today  # probably not true, but don't care that much
        return lake_max_date

    # *******  *******   *******   *******   *******   *******   *******
    def get_database_min_max_datetimes(self, lake_from, lake_until):
        lake_min_date = lake_from
        lake_max_date = lake_until

        database_min_datetime = pd.to_datetime(lake_min_date) + pd.Timedelta(self.start_hour)
        database_min_datetime = database_min_datetime.tz_localize(self.inherent_tz)

        if self.resolution == 'M':
            dt = '30D'
        else:
            dt = self.resolution

        database_max_datetime = pd.to_datetime(lake_max_date) + pd.Timedelta(
            DateTime.disambiguate_timedelta(pd.to_datetime(lake_max_date), self.period_covered)) - pd.Timedelta(dt)
        database_max_datetime = database_max_datetime.tz_localize(self.inherent_tz)

        return database_min_datetime, database_max_datetime

# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
class CuePoints:

    # *******  *******   *******   *******   *******   *******   *******
    def assert_observed_tags_as_declared(self, observed, declared):

        if len(observed) != len(declared):
            self.logger.error("Nicknames declared in 'Space' tab do not match the tags observed in cue-points file ({})")
            raise AssertionError

        obs = observed.copy()
        dec = declared.copy()
        obs = sorted(obs)
        dec = sorted(dec)

        if obs != dec:
            self.logger.error("Nicknames declared in 'Space' tab do not match the tags observed in cue-points file ({})")
            raise AssertionError

    # *******  *******   *******   *******   *******   *******   *******
    @staticmethod
    def construct_placeholder_cue_points(sheet_tags):

        placeholder = {'start':{'cue':None,'inclusive':True},
                       'end':{'cue':None, 'inclusive':True}
                       }
        cue_rules = {field:{subfield:placeholder for subfield in sheet_tags} for field in sheet_tags}


        return cue_rules


    # *******  *******   *******   *******   *******   *******   *******
    @staticmethod
    def parse_cue_points_content(content):
        cue_rules = {}
        sheet_tag = None
        for row in content.split('\n'):
            if not row:
                continue

            if row[0] == '$':
                sheet_tag = row[1:]
                cue_rules[sheet_tag] = {}

            elif row[0] == ';' or row == '\n':
                pass

            else:
                cue_name, cue_bounds = CuePoints._row_splitter(row)
                cue_rules[sheet_tag][cue_name] = CuePoints._cue_splitter(cue_bounds)


        return cue_rules
    # *******  *******   *******   *******   *******   *******   *******
    @staticmethod
    def _row_splitter(row):
        # print(row)
        cue_name, cue_limits = row.split(':')
        cue_name = cue_name.strip()
        cue_limits = cue_limits.strip()
        return cue_name, cue_limits

    # *******  *******   *******   *******   *******   *******   *******
    @staticmethod
    def _cue_splitter(cue_bounds):

        def symbol_to_bool(symbol):
            if symbol in ['[',']']:
                return True
            elif symbol in ['(', ')']:
                return False

        start_cue_inclusivity = cue_bounds[0]
        end_cue_inclusivity = cue_bounds[-1]
        start_cue_inclusivity = symbol_to_bool(start_cue_inclusivity)
        end_cue_inclusivity = symbol_to_bool(end_cue_inclusivity)

        ''' be carefule where to strip(), becasue any spaces put within the quotes is deliberate'''
        cue_bounds = cue_bounds[1:-1]
        start_cue, end_cue = list(map(lambda x: re.sub(r'["\']', '', x.strip()), cue_bounds.split(',')))

        if start_cue == 'None' or not start_cue:
            start_cue = None
        if end_cue == 'None' or not end_cue:
            end_cue = None


        parsed = {'start':{'cue':start_cue, 'inclusive':start_cue_inclusivity},
                  'end':{'cue':end_cue, 'inclusive':end_cue_inclusivity}}

        return parsed

    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******