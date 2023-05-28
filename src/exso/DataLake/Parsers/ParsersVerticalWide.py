import datetime
import logging
import traceback

from exso.DataLake.Parsers.ParsersArch import Archetype
from exso.Utils.DateTime import DateTime

# *********************************************

date_lambda = lambda x: datetime.datetime.strftime(DateTime.date_magician(x, return_stamp = False), format="%d-%b-%y")
logger = logging.getLogger(__name__)

# *********************************************
def error_handler(func):
    def wrap(*args, **kwargs):

        text = ' '.join(func.__name__.split('_')[1:]) + ' ' + str(args) + ' | ' + str(kwargs)

        try:
            res = func(*args, **kwargs)
            return res

        except Exception:
            print('ERROR')
            print(text)
            logger.error(msg="\n\n\nError !!. info:\n\n" + text)
            logger.error(msg=traceback.format_exc())
            print('\t\t\tException was:\n\n' +   traceback.format_exc())
            raise InterruptedError

    return wrap

###############################################################################################
###############################################################################################
###############################################################################################
class ArchetypeWide(Archetype):
    # *******  *******   *******   *******   *******   *******   *******
    def param_setter(self):
        pass

    # *******  *******   *******   *******   *******   *******   *******
    def pre_proc(self, df):
        if self.replacer_mapping:
            df = self.apply_replacements(df, self.replacer_mapping, regex =False, field = self.field)

        if self.regex_mapping:
            df = self.apply_replacements(df, self.regex_mapping, regex =True, field = self.field)

        if self.dropna_settings:
            df = self.drop_nans(df, settings=self.dropna_settings, field=self.field)

        return df

    # *******  *******   *******   *******   *******   *******   *******
    def split_cue_points(self, df):
        return {self.field: df}

    # *******  *******   *******   *******   *******   *******   *******
    def transposeAll(self, subfields_dfs):
        return subfields_dfs

    # *******  *******   *******   *******   *******   *******   *******
    def rolling_post_proc(self, subfields_dfs):
        for subfield, df in subfields_dfs.items():

            df = self.apply_datetime_index(df, self.period_dates)

            if self.renamer_mapping:
                self.col_renamer(df, renamer_mapping = self.renamer_mapping, field = self.field, subfield = subfield)

            if self.drop_col_settings:
                df = self.drop_columns_if(df,
                                          col_names=self.drop_col_settings['col_names'],
                                          startswith=self.drop_col_settings['startswith'],
                                          error_action=self.drop_col_settings['error_action'],
                                          field=self.field,
                                          subfield=subfield)

            df = df.fillna(0)

            subfields_dfs[subfield] = df
        return subfields_dfs

    # *******  *******   *******   *******   *******   *******   *******
    def expost(self, subfields_dfs):
        return subfields_dfs

    # *******  *******   *******   *******   *******   *******   *******

###############################################################################################
###############################################################################################
###############################################################################################
class IMBABE(ArchetypeWide):
    def param_setter(self):
        self.drop_col_settings.update(col_names= ['Period', 'STARTDATE', 'ENDDATE'],
                                      startswith= 'Unnamed')

###############################################################################################
class BalancingEnergyProduct(IMBABE):
    pass

###############################################################################################
class BalancingCapacityProduct(IMBABE):
    pass

###############################################################################################
class RESMV(ArchetypeWide):
    def param_setter(self):
        self.renamer_mapping = {'ΗΜΕΡΑ':'Day', 'ΩΡΑ':'Hour',
                                'B/A ΕΝΕΡΓΕΙΑ KWh':'Biogas_MWh',  'B/A ΕΓΚ ΙΣΧΥΣ MW':'Biogas_MWinstalled',
                                'ΜΥΗΣ ΕΝΕΡΓΕΙΑ KWh':'SmallHydro_MWh', 'ΜΥΗΣ ΕΓΚ ΙΣΧΥΣ MW':'SmallHydro_MWinstalled',
                                'ΣΗΘΥΑ ΕΝΕΡΓΕΙΑ KWh': 'CHP_MWh', 'ΣΗΘΥΑ ΕΓΚ ΙΣΧΥΣ MW':'CHP_MWinstalled',
                                'Φ/Β ΕΝΕΡΓΕΙΑ KWh':'PV_MWh',  'Φ/Β ΕΓΚ ΙΣΧΥΣ MW':'PV_MWinstalled'}

        self.dropna_settings.update(how= None, thresh = 2, axis='rows')

    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******
    def pre_proc(self, df):

        df = df.rename(columns=self.renamer_mapping)

        df = df.dropna(axis = 'rows', thresh=2) # admie has 6-7 rows with text in first col, after all the date

        df[df.columns[df.columns.str.contains('MWh')]]/=1000

        return df

    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******
###############################################################################################
class HVCUSTCONS(ArchetypeWide):
    def param_setter(self):
        self.renamer_mapping = {'ΗΜΕΡΑ':'Day', 'ΩΡΑ':'Hour',
                                'ΚΑΤΑΝΑΛΩΣΗ ΠΕΛΑΤΩΝ ΥΤ (MWh)':'HV_Load',
                                'ΒΟΗΘΗΤΙΚΑ ΜΟΝΑΔΩΝ ΥΤ (MWh)': 'HV_AssistiveLoad'}


        self.dropna_settings.update(how= None, thresh = 2, axis='rows')

    # *******  *******   *******   *******   *******   *******   *******
    def pre_proc(self, df):

        df = df.rename(columns=self.renamer_mapping)

        df = df.dropna(axis = 'rows', thresh=2) # admie has 6-7 rows with text in first col, after all the date

        return df


    # *******  *******   *******   *******   *******   *******   *******
###############################################################################################
class RESMVLVPROD(ArchetypeWide):
    def param_setter(self):
        self.renamer_mapping = {'ΗΜΕΡΑ':'Day', 'ΩΡΑ':'Hour',
                                'ΕΝΕΡΓΕΙΑ (MWh)':'RESMVLV',}


        self.dropna_settings.update(how= None, thresh = 2, axis='rows')

    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******
    def pre_proc(self, df):

        df = df.rename(columns=self.renamer_mapping)

        df = df.dropna(axis = 'rows', thresh=2) # admie has 6-7 rows with text in first col, after all the date

        return df

    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******
###############################################################################################
###############################################################################################
###############################################################################################
