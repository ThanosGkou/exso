import logging
import os.path
import tempfile
import time
from pathlib import Path

import numpy as np
import pandas as pd
from exso.HighLevel.Updater import Updater
from exso.Utils.DateTime import DateTime
from haggis import string_util as hag


###############################################################################################
###############################################################################################
###############################################################################################
class SingleValidation(Updater):
    def __init__(self, date, root_lake, reports_pool = None):

        self.allow_handshake = True
        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

        self.date = DateTime.date_magician(date, return_stamp=False)
        self.str_date = DateTime.make_string_date(self.date, sep = "")
        self.database_tz = 'UTC'
        self.convert_to_tz = 'datalake'
        self.keep_steps = True
        self.rp = self.get_pool(reports_pool = reports_pool)
        self.root_base = Path(tempfile.mkdtemp())
        self.root_lake = root_lake


    # *********************************************
    def run(self, report_name, field = None, subfields:str|list|str = None):

        self.report_name = report_name
        self.refresh_requirements = {report_name:False}

        self._set_debugging_options({'start_date':self.date, 'end_date':self.date})
        self.single(report_name=report_name, keep_raw = True, use_lake_version = 'latest', retroactive_update = False)
        print('\n\nValidation log.')
        self.derive_field_subfield(field)

        self.derive_time_zone()
        self.get_slice()

        temp_dir = Path(tempfile.mkdtemp())
        self.export(temp_dir= temp_dir)
        self.launch_excel(self.excel_temp_path)
        self.write_readme(temp_dir)


    # *********************************************
    def derive_field_subfield(self, field):
        if not field:
            print(self.base.tree.root.children)
            self.field = self.base.tree.root.children[0].name
        else:
            self.field = field

        field_node = self.base.tree.get_nodes_whose('name', equals=self.field)
        # print(field_node.children)
        self.subfields = [c.name for c in field_node.children][0]
        print(f'{self.field = }', f'{self.subfields = }')
        # print('Validating for ("{}","{}")'.format(self.field, self.subfield))

    # *********************************************
    def derive_time_zone(self):
        if self.convert_to_tz == 'database':
            tz = self.database_tz
        elif self.convert_to_tz == 'datalake':
            tz = self.r.inherent_tz
        else:
            tz = self.convert_to_tz
        self.reporting_timezone = tz
        print('Reporting in datalake=inherent timezone ({})'.format(self.reporting_timezone))

    # *********************************************
    def get_slice(self):
        ''' the child "Updater", finally has a self.lake attribute
            Use this, the str-date, and the field/subfield provided to get the specific data.
            But, if the field is None, I had no way to check it prior to this point.
            So, error-catching, if non-existing, I get the first field and first subfield
        '''

        try:
            self.lake_raw = self.lake.pipeline.as_read[self.str_date][self.field]
        except:

            extr_data = self.lake.pipeline.as_read[self.str_date]
            field = list(extr_data.keys())[0]
            self.lake_raw = extr_data[field]

        self.file_used = self.lake.status.file_df

        parsed_data = self.lake.data[self.field]
        lake_df = parsed_data[self.subfields[0]]
        for i in range(1, len(self.subfields)):
            lake_df = pd.concat([lake_df, pd.DataFrame({' ':np.full(len(lake_df), np.nan)}, index=lake_df.index), parsed_data[self.subfields[i]]], axis = 1)

        field_node = self.base.tree.get_nodes_whose('name',equals=self.field, collapse_if_single=False)
        field_node = [fn for fn in field_node if fn.kind == 'field'][0]

        parsed_data = field_node()
        base_df = parsed_data[self.subfields[0]]
        for i in range(1, len(self.subfields)):
            base_df = pd.concat([base_df, pd.DataFrame({' ': np.full(len(base_df), np.nan)}, index=base_df.index),
                                 parsed_data[self.subfields[i]]], axis=1)

        self.lake_data = lake_df.tz_localize(None)
        self.base_data = base_df

    # *********************************************
    def export(self, temp_dir):

        temp_file = temp_dir/ f'validation_{self.report_name}_{self.field}_{self.str_date}.xlsx'

        with pd.ExcelWriter(temp_file) as writer:
            self.lake_raw.to_excel(writer, sheet_name='Raw Lake')
            self.lake_data.to_excel(writer, sheet_name="Lake Data")
            self.base_data.to_excel(writer, sheet_name="Base Data")

        self.excel_temp_path = temp_file

    # *********************************************
    def launch_excel(self, open_filepath):
        print('Launching excel. If you want to keep it: Save as. Otherwise it will be deleted.')
        f = '"' + str(open_filepath) + '"'
        os.system("start EXCEL.EXE {}".format(f))

    # *********************************************
    def write_readme(self, temp_dir):
        algn = 'left'
        readme_filename = 'readme_{}_{}_{}.txt'.format(self.report_name, self.date, self.field)
        readme_filepath = temp_dir / readme_filename
        with open(readme_filepath, 'w', encoding='utf-8') as f:
            f.write(hag.make_box("Inspection of datalake --> database integrity",style='bold-line', horizontal_padding=4, vertical_padding=1))
            f.write("\nAn excel file has been opened, which compares a raw-datalake file to the corresponding data, as parsed, and as stored in the database*")
            f.write('\n\t*For inspection purposes, the parsed data are converted into the raw datalake\'s inherent timezone')
            f.write('\n\nThis inspection was performed with the following settings:')
            f.write('\n\t' + hag.align('Report Name: "{}"'.format(self.report_name), alignment = algn))
            f.write('\n\t' + hag.align('Date: "{}"'.format(self.date), alignment = algn))
            f.write('\n\t' + hag.align('File used: {}'.format(self.file_used['filenames'].squeeze()), alignment = algn))
            f.write('\n\t' + hag.align('Field (aka sheet of datalake xls* file): "{}"'.format(self.field), alignment = algn))
            f.write('\n\t' + hag.align('Subfield (aka section of the sheet): "{}"'.format(self.subfields), alignment = algn))
            f.write('\n\t' + hag.align('Inherent timezone of raw datalake file: {}'.format(self.reporting_timezone), alignment = algn))
            f.write('\n\nHow to read the excel file:')
            f.write('\n\t1. The "Raw Lake" tab contains the data as-is in the {} sheet'.format(self.field))
            f.write('\n\t2. The "Lake Data" tab, contains the parsed data for subfield: {}, in the same timezone as the inherent raw lake\'s ({})'.format(self.subfields, self.reporting_timezone))
            f.write('\n\t3. The "Base Data" tab, contains the data as would be written in the database, in UTC timezone.')

            f.write('\n\n--> Caution: This excel file is a temporary file and will be deleted if you simply close it.   '
                    '\n\t--> If you want to keep it you must "save as" it accordingly. (the same holds for this text file)')

        str_path = '"' + str(readme_filepath) + '"'
        time.sleep(3)
        os.system("START notepad.exe {}".format(str_path))



###############################################################################################
###############################################################################################
###############################################################################################
class Validation(Updater):
    ''' The Validation class assists in comparing raw datalake files to parsed/database timeseries, to perform custom checks on intended behaviour.

    :param report_name [str]: The name of the report you want to validate
    :param dates [str|list]: A list (or a single object) of datetime-like nature (pandas compatible) -- can be 'YYYY-MM-DD', or pd.Timestamp, etc.
    :param fields [None|str|list]: A list ( or a single object) of fields you want to validate. Fields are basically sheets of the raw datalake file
                                   If None, will use the first field / sheet
    :param reports_pool [None|Report.Pool]: If None, will instantiate a new Report.Pool object

    Example Usage:

        mv = Validation(report_name:ISP1ISPResults, dates=['2022-12-30', '2022-12-31'], root_lake=Path('C:/u/l/a/t/e/r')
        mv.run()

        This will launch one Excel file for every pair of (field, date)
        The excel file will contain the raw datalake content, the parsed content in the same timezone as the raw lake, and the parsed content in UTC (as it would be in the database)
            - The excel file(s) are temp files and will be automatically deleted if not saved.

    '''
    def __init__(self, report_name:str, dates:str|list, root_lake:str|Path, fields:str|list|None = None, reports_pool = None):
        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

        if not reports_pool:
            reports_pool = self.get_pool(None)

        report_name = report_name

        if not isinstance(dates, list):
            dates = [dates]

        if not fields:
            fields = [None] #*len(report_names)
        if isinstance(fields, str):
            fields = [fields]

        self.rp = reports_pool
        self.fields = fields
        self.dates = dates
        self.root_lake = root_lake
        self.report_name = report_name

    # *********************************************
    def run(self):
        # for i, r in enumerate(self.report_names):
        report_name = self.report_name
        for j,f in enumerate(self.fields):
            for k,d in enumerate(self.dates):
                sv = SingleValidation(d, self.root_lake,  self.rp)
                sv.run(report_name=report_name, field = f)#, subfields = self.subfields[i])
                print('\n\nFinished for: {}, {}, {}'.format(report_name, d, f))

            next_field = None
            try:
                next_field = self.fields[j+1]
                answer = input('--> Proceed in next field ({})? ([Enter] when you\'re ready): '.format(next_field))
            except:
                print('\nValidation Completed.\n')


###############################################################################################
###############################################################################################
###############################################################################################
