import datetime
import logging
import os
import warnings

import pandas as pd
from exso.DataLake.Query import Query
from exso.DataLake.Status import Status
from exso.DataLake.Update import Update
from exso.Utils.DateTime import DateTime

warnings.simplefilter('ignore', UserWarning)

###############################################################################################

date_lambda = lambda x: datetime.datetime.strftime(DateTime.date_magician(x, return_stamp = False), format="%d-%b-%y")



###############################################################################################
###############################################################################################
###############################################################################################
class DataLake(Update, Query):
    instances = []
    def __init__(self, report, use_lake_version = 'latest', retroactive_update = False):

        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.logger.info("Initializing Datalake for '{}'".format(report.report_name))

        self.instances.append(self)
        self.r = report
        self.report_name = report.report_name
        self.publisher = report.publisher
        self.retroactive_update = retroactive_update

        args_needed = {'dir':self.r.datalake_path,
                       'min_potential_date':self.r.available_from,
                       'max_potential_date':self.r.available_until,
                       'time_lag_days':self.r.time_lag_days,
                       'period_covered':self.r.period_covered,
                       'eligibility':self.r.eligibility,
                       'sheet_tags':self.r.sheet_tags}


        self.status = Status(**args_needed)


        self.status.refresh(timeslice={'start_date':self.status.dates.min.potential.date},
                            use_lake_version = use_lake_version)




###############################################################################################
###############################################################################################
###############################################################################################
class Diagnostic:
    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******
    def make_ambiguity_history(self, sheet_locator = 0, from_col = 0):
        '''
        :return: it checks the datalake dataframes and sees the "version history" of naming conventions.

        '''

        self.logger.info("Making ambiguation history file.")

        name_history = {}
        sheet_tag = self.r.get_sheet_tag[sheet_locator]
        ''' here, assign the data_as_extrated with the dict object returned by extractAll() {str_date: {shhet:df,sheet2:df2}, ....}'''
        data_as_extracted = {}
        for str_date, sheet_dfs in data_as_extracted.items():

            df = sheet_dfs[sheet_tag]
            name_history[str_date] = df.iloc[:, from_col]

        name_history = pd.DataFrame({k: pd.Series(v) for k, v in name_history.items()}).T

        start_ = self.file_df['dates'][0]
        end_ = self.file_df['dates'][-1]
        start_ = datetime.datetime.strftime(pd.to_datetime(start_), format = "%d%b%Y")
        end_ = datetime.datetime.strftime(pd.to_datetime(end_), format = "%d%b%Y")

        string_repr = self.r.report_name + '_'+ sheet_tag + '_' + 'col'+ str(from_col) + '_' + start_ + '_to_' + end_

        save_path = os.path.join(self.r.database_path, '.name_ambiguity_{}.xlsx'.format(string_repr))
        self.logger.info("Made history : {}".format(string_repr))
        self.logger.info('--> Stored in file: {}'.format(save_path))

        writer = pd.ExcelWriter(save_path, engine='xlsxwriter')
        name_history.to_excel(writer, sheet_name='Sheet1')

        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        format1 = workbook.add_format({'bg_color': 'red'})
        (max_row, max_col) = name_history.shape
        worksheet.conditional_format(1, 1, max_row, max_col,
                                     {'type': 'formula',
                                      'criteria': '=C2<>C3',
                                      'format': format1})
        writer.save()


###############################################################################################
###############################################################################################
###############################################################################################
