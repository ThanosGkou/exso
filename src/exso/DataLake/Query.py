import datetime
import copy
import logging

from exso.DataLake.ETL.ETL import Loader, Parser, Joiner
from exso.Utils.DateTime import DateTime

date_lambda = lambda x: datetime.datetime.strftime(DateTime.date_magician(x, return_stamp=False), format="%d-%b-%y")
###############################################################################################
###############################################################################################
###############################################################################################
class Query:
    ''' This is the wrapper-class, containing the tools to query the datalake.
        Query, by means that it processes the datalake content, it does not add new content to it (does not update the datalake)
        It is completely agnostic to the database.

        This is not a stand-alone class, but is inherited by the DataLake class.
        It assumes that a self.status attribute exists (Status instance)\

        It can be instantiated directly, but requires a status object.

        Usage: Call the .query() method of the datalake object
    '''
    def __init__(self, status):
        ''' Be aware that this class is just inherited, not instantiated. This is just a placeholder for potential out-of-ordinary use that requires a direct instance of it
        '''
        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.status = status

    # *******  *******   *******   *******   *******   *******   *******
    def query(self, start_date: None | str | datetime.datetime=None, end_date: None | str | datetime.datetime=None, dates_iterable = None, str_dates = None, keep_raw = False):

        self.status.refresh(timeslice = {'start_date':start_date, 'end_date':end_date, 'dates_iterable':dates_iterable, 'str_dates':str_dates})
        if self.status.file_df.empty:
            self.logger.info("Datalake ETL query is degenerate.")
            self.data = {}
        else:
            self.logger.info("Making Datalake ETL query for {} files".format(self.status.file_df.shape[0]))
            self.pipeline = Pipeline(self.status.file_df, self.r)
            self.pipeline.run(keep_raw = keep_raw)
            self.data = self.pipeline.data

        return self.data
    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******



###############################################################################################
###############################################################################################
###############################################################################################
class Pipeline(Loader, Parser, Joiner):
    ''' Depending on desired structure of the pipeline, one may want to follow either a process-lead or a file-lead pipeline
        Process-driven: 1. Load all files to memory "as-is"
                        2. Transform/clean/rectify all files
                        3. Join all timeseries

        File-driven:    For each file, read, transform, concat to the total cache-timeseries (not the database)

        Because the files of the system are very unstructured, badly formatted, and varyingly formatted (ad-hoc renames, restructure, etc), one may want:
        - To test that e.g. all files can be read --> in this case you want a process-driven approach, which will pass or crash at specific file, without the burden of ETL
        - To test if a specific file can be properly parsed --> In this case maybe (or maybe not) a file-driven approach is beetter

        For actual deployment, the process-driven offers asyncio optimization oppportunities, while the file-driven offers parallelization/multi-threading opportunities

        Currently, the process-driven approach is followed, and no optimization at all is there.
        This is because the file formatting is so poor, that the benefit of maintanability is way larger than the (very) sub-optimal implementation performance-wise.
        Also, a first setup od datalake and database, may take 5-8 hours, but every update is only a matter of minutes. Thus, it is questionable if meaningfull to optimize at all.

        #todo: add the option of process vs file-driven pipeline
        #todo later: multi-processing file-driven (multi-threading performing poorly)

    '''
    def __init__(self, file_df, report):
        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.r = report
        self.file_df = file_df

    # *******  *******   *******   *******   *******   *******   *******
    def run(self, keep_raw=False):

        self.filepaths = self.file_df['filepaths'].values
        self.str_dates = self.file_df.index.values

        self.decide_reading_engine()
        self.get_reader()
        self.readAll()

        # a_date = list(self.data.keys())[0]
        # a_field = list(self.data[a_date].keys())[0]

        if keep_raw:
            self.as_read = copy.deepcopy(self.data)


        self.get_parser()
        self.dates_per_period, self.dates_flat_series = self.datetime_constructor()

        self.transformAll()
        # a_subfield = list(self.data[a_date][a_field].keys())[0]

        self.joinAll()

        # df = self.data[a_field][a_subfield]
        # self.sort()
        # self.insert_missing_records()

    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******