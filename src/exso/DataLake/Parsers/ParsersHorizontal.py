import copy
import datetime
import inspect
import logging
import os
import re
import time

import numpy as np
import pandas as pd
from exso.DataLake.Parsers.ParsersArch import Archetype
from exso.Utils.DateTime import DateTime
from exso.Utils.STR import STR
from unidecode import unidecode

# *********************************************

date_lambda = lambda x: datetime.datetime.strftime(DateTime.date_magician(x, return_stamp = False), format="%d-%b-%y")
logger = logging.getLogger(__name__)

# *********************************************
# *********************************************
def logging_sampler(func):
    def wrap(*args, **kwargs):
        func_name = func.__name__
        stack = inspect.stack()
        parser_object = args[0]

        runtime = time.time()
        res = func(*args, **kwargs)
        runtime = str(round(time.time() - runtime,2)) + ' sec'

        if parser_object.log_now == False:
            return res

        parser_full_name = parser_object.__class__.__module__ + '.' + parser_object.__class__.__name__

        ascendants = [asc.__name__ for asc in type(parser_object).__mro__[:-1]]
        parser_inheritance_history = " <-- ".join(ascendants)


        traces = []
        for fn in [1,2,3]:
            s = stack[fn]
            s_dict = {'called_from':os.path.split(s.filename)[-1],
                      'within_method':s.function,
                      'lineno':s.lineno,
                      'code':s.code_context[0].strip()}
            if s_dict['within_method'] == 'wrap':
                pass
            else:
                traces.append(s_dict)

        traces_text = []
        for tr in traces[::-1]:
            tr_text = (f"File: {tr['called_from']}", f"Method: {tr['within_method']}", f"LineNo: {tr['lineno']}", f"Code: {tr['code']}")
            traces_text.append(tr_text)

        def aligner(*args):
            t = ""
            argssss = args[0]
            t += '{:<30}'.format(argssss[0])
            t += '{:<35}'.format(argssss[1])
            # t += '{:<20}'.format(argssss[2])
            # t += '{:<50}'.format(argssss[3])
            return t

        def code_calls_printer(code_calls):

            t = "\n"
            for cc in code_calls:
                t += aligner(cc) + '\n'
            return t

        signature = {'Parser':parser_full_name, 'Method':func_name, 'Inheritance':parser_inheritance_history}

        nstar100 = '\n' + '*'*100
        date_block = nstar100 * 3 + '  Date of Parsing: {}'.format(parser_object.date) + 2 * nstar100
        end_block = nstar100 * 3

        if parser_object.date in parser_object.dates_logged:
            pass
        else:
            logger.debug(date_block)
            parser_object.dates_logged.append(parser_object.date)

        logger.debug('\t  ' + traces[1]['code'])
        logger.debug('\t    ' + traces[0]['code'])

        logger.debug('Signature:\n\n'+STR.iterprint(signature,return_text=True),stack_info=False,stacklevel=5)
        logger.debug(code_calls_printer(traces_text),stack_info=False,stacklevel=5)
        logger.debug(nstar100)

        logger.debug('Input Data')
        logger.debug('Keyword Arguments:')
        logger.debug(STR.iterprint(kwargs, return_text=True))
        logger.debug("Payload (df, or dict of dfs)")
        logger.debug(STR.iterprint({'data':args[1]},return_text=True))
        logger.debug(nstar100)


        logger.debug("Output data")
        logger.debug("\tRuntime: {}".format(runtime))
        logger.debug(STR.iterprint({'data':res}, return_text=True))
        logger.debug(end_block)

        return res

    return wrap


###############################################################################################
###############################################################################################
###############################################################################################
class ArchetypeHorizontal(Archetype):
    logger = logging.getLogger()

    # *********************************************
    def param_setter(self):
        pass

    # *********************************************
    def param_updater(self):
        pass
    # *********************************************
    @logging_sampler
    def pre_proc(self, df):

        df = self.baptize_indicator_column(df, indicator_col_index = self.indicator_col_index, field = self.field)

        if self.replacer_mapping:
            df = self.apply_replacements(df, self.replacer_mapping, regex =False, field = self.field)

        if self.regex_mapping:
            df = self.apply_replacements(df, self.regex_mapping, regex =True, field = self.field)

        if self.last_column_trigger:
            df = self.truncate_upto_last_column_trigger(df, self.last_column_trigger, self.drop_last, guide_row = 0, field = self.field)

        if self.dropna_settings:
            df = self.drop_nans(df, settings = self.dropna_settings, field = self.field)

        if df.empty:
            if self.action_if_empty_df:
                df = self.empty_df_handler(df,
                                           action_if_empty = self.action_if_empty_df,
                                           arbitrary_columns = self.empty_df_filler['arbitrary_columns'],
                                           fill_value= self.empty_df_filler['fill_value'],
                                           field = self.field)

        return df

    # *********************************************
    @logging_sampler
    def split_cue_points(self, df):
        subfields_dfs = {}
        field_cue_dict = self.cue_dict[self.field]
        if df.shape[0] == 1:
            subfields_dfs = {self.field: df}
            return subfields_dfs

        for subfield, subfield_cue_dict in field_cue_dict.items():

            start_cue = subfield_cue_dict['start']['cue']
            start_inclusive = subfield_cue_dict['start']['inclusive']
            end_cue = subfield_cue_dict['end']['cue']
            end_inclusive = subfield_cue_dict['end']['inclusive']
            start_operator, end_operator, resid_operator = self._make_operators(start_inclusive, end_inclusive)

            id_start, id_end = self._get_start_end_indices(df, start_cue, end_cue, field = self.field, subfield = subfield)

            # print('Base df:')
            # print(df)
            # print()
            # print()
            # print(f'{start_cue = }')
            # print(f'{start_inclusive = }')
            # print(f'{end_cue = }')
            # print(f'{end_inclusive = }')
            # print(f'{start_operator = }')
            # print(f'{end_operator = }')
            # print(f'{id_start = }')
            # print(f'{id_end = }')

            if isinstance(id_start, type(None)) or isinstance(id_end, type(None)):
                if isinstance(id_start, type(None)):
                    self.logger.warning("Could not locate start-cue '{}', while extracting subfield: '{}' from field '{}', for report: '{}', for date: '{}'".format(start_cue, subfield, self.field, self.report_name, self.period_dates[0]))
                if isinstance(id_end, type(None)):
                    self.logger.warning("Could not locate end-cue '{}', while extracting subfield: '{}' from field '{}', for report: '{}', for date: '{}'".format(end_cue, subfield, self.field, self.report_name, self.period_dates[0]))

                subfields_dfs[subfield] = pd.DataFrame()
                continue

            extract = 'df[(df.index {} id_start) & (df.index {} id_end)].copy().reset_index(drop=True)'.format(start_operator, end_operator)
            subfields_dfs[subfield] = eval(extract)

            residual_expression = 'df[df.index {} id_end].copy().reset_index(drop = True)'.format(resid_operator)
            df_resid = eval(residual_expression)

            df = df_resid.copy()
            # print('Resulted subdf:')
            # print(subfields_dfs[subfield])
            # print()
            # print()
            # input('\nok?')

        return subfields_dfs

    # *********************************************
    @logging_sampler
    def transposeAll(self, subfields_dfs):
        for subfield, df in subfields_dfs.items():
            if df.empty:
                pass
            else:
                df = self.transposer(df, skip_rows_at_final = self.skip_rows_at_final, field = self.field, subfield = subfield)

            subfields_dfs[subfield] = df

        return subfields_dfs

    # *********************************************
    @logging_sampler
    def rolling_post_proc(self, subfields_dfs):
        for subfield, df in subfields_dfs.items():

            if df.empty:
                if self.action_if_empty_df:
                    df = self.empty_df_handler(df,
                                               action_if_empty=self.action_if_empty_df,
                                               arbitrary_columns=self.empty_df_filler['arbitrary_columns'],
                                               fill_value=self.empty_df_filler['fill_value'],
                                               field = self.field,
                                               subfield = subfield)

            df = self.apply_datetime_index(df, self.period_dates)

            if self.renamer_mapping:
                df = self.col_renamer(df, renamer_mapping = self.renamer_mapping, field = self.field, subfield = subfield)

            if df.shape[1] == 1:
                df.columns = [subfield]

            try:
                df = df.astype(float).fillna(0)

            except:
                df = df.fillna(0)

            if self.drop_col_settings:
                df = self.drop_columns_if(df,
                                          col_names = self.drop_col_settings['col_names'],
                                          startswith = self.drop_col_settings['startswith'],
                                          error_action = self.drop_col_settings['error_action'],
                                          field = self.field,
                                          subfield = subfield)

            subfields_dfs[subfield] = df


        return subfields_dfs

    # *********************************************
    def expost(self, fields_dfs):
        # for f in fields_dfs.keys():
        #     for sf, df in fields_dfs[f].items():
        #         print()
        #         print(sf)
        #         print(df)
        #         print('\n\n')
        return fields_dfs
    # *********************************************


###############################################################################################
###############################################################################################
###############################################################################################
class ISP1ISPResults(ArchetypeHorizontal):

    # *******  *******   *******   *******   *******   *******   *******
    def param_updater(self):
        self.last_column_trigger = '23:30:00'
        self.drop_col_settings.update(startswith='MOCK')

    # *******  *******   *******   *******   *******   *******   *******
    def rolling_post_proc(self, subfields_dfs):
        ''' post-processing within sheet'''
        # todo: watch out for inherintance, maybe postproc is called not just once, but N times.......

        if self.field != 'CCGT_Schedule':
            subfields_dfs = super().rolling_post_proc(subfields_dfs)

        else:
            ''' it comes like this: columns = [Lavrio4, nan, nan, Komotini, nan, nan, Megalopoli_V, nan, nan, Alouminio, nan, nan]
                I want it to be: [Lavtio4_GT, Lavrio4_ST, Lavrio4_MW, etc]'''

            df = subfields_dfs['CCGT_Schedule']

            cols = df.columns.to_list()
            plant_names = df.columns[~df.columns.isna()].to_list()
            suffix = ['_GT','_ST','_MW']
            new_cols = [cols[3*divmod(i,3)[0]] + suffix[divmod(i,3)[1]] for i in range(len(cols)) ]
            df.columns = new_cols

            df = self.apply_datetime_index(df,self.period_dates)

            for plant_name in plant_names:
                df[plant_name + '_mode'] = df[plant_name + '_GT'] + "+" + df[plant_name + '_ST']
                df = df.drop(columns=[plant_name + '_GT', plant_name + '_ST'])

            df = df.sort_index(axis = 1)
            subfields_dfs['CCGT_Schedule'] = df


        return subfields_dfs

    # *******  *******   *******   *******   *******   *******   *******
    def expost(self, fields_dfs):
        fields_dfs = super().expost(fields_dfs) # fill na
        ''' post-processing within date, has access to all sheets'''
        fields_dfs['Market_Schedule'] = {'Market_Schedule':self.derive_market_schedule(fields_dfs)}
        return fields_dfs

    # *******  *******   *******   *******   *******   *******   *******
    def derive_market_schedule(self, fields_dfs):

        market_schedule = pd.DataFrame()
        for key in ['Thermal', 'Hydro', 'DispatchableLoad']:
            market_schedule = pd.concat([market_schedule, fields_dfs['ISP_Schedule'][key] - fields_dfs['ISP_Activations'][key]], axis=1)

        return market_schedule

###############################################################################################
class ISP2ISPResults(ISP1ISPResults):
    pass
###############################################################################################
class ISP3ISPResults(ISP1ISPResults):
    pass

###############################################################################################
class AdhocISPResults(ISP1ISPResults):
    def pre_proc(self, df):

        # first row is: "Non Dispatchable Load, 27/11/2022  15:30:00, 27/11/2022  16:00:00, 27/11/2022  16:30:00, ansd so on
        start_datetime = df.iloc[0].values[1]
        flat_dates = self.period_dates.tz_localize(None)
        matching_points = [i for i, flat_date in enumerate(flat_dates) if flat_date == start_datetime]
        matching_point = matching_points[0]

        self.period_dates = self.period_dates[matching_point:]

        df = super().pre_proc(df)
        return df
###############################################################################################
class DispatchSchedulingResults(ISP1ISPResults):
    def param_updater(self):
        self.last_column_trigger = 'TOTAL'
        self.drop_last = True

    # *******  *******   *******   *******   *******   *******   *******
    def expost(self, fields_dfs):
        return fields_dfs
###############################################################################################
###############################################################################################
###############################################################################################
class DayAheadSchedulingUnitAvailabilities(ArchetypeHorizontal):

    # *******  *******   *******   *******   *******   *******   *******
    @logging_sampler
    def rolling_post_proc(self, subfields_dfs):
        if hasattr(self, 'thermal_unavailability_reason'):
            pass
        else:
            self.thermal_unavailability_reason = pd.DataFrame()
            self.hydro_unavailability_reason = pd.DataFrame()

        row_content = {'Actual': 0, 'Forecast': 1, 'Unavailability_Reason': 2}

        new_subfields_dfs = {}


        # From somepoint in late 2024, admie puts KOMOTINI_POWER and DR unit BELOW the Hydro units.
        # So, they are mistajkenly parsed as hydro instead of thermal or whatever. The 10 lines below deal with this issue.
        listed_in_hydro_but_should_be_thermal = ['KOMOTINI_POWER', 'DUF_BZ1_01_IR_PV']
        listed_in_thermal_but_should_be_hydro = []

        _h = subfields_dfs['Hydro']
        _t = subfields_dfs['Thermal']

        move_to_thermal = _h[_h.columns[_h.columns.isin(listed_in_hydro_but_should_be_thermal)]]
        move_to_hydro = _t[_t.columns[_t.columns.isin(listed_in_thermal_but_should_be_hydro)]]
        _h.drop(columns=listed_in_hydro_but_should_be_thermal, errors='ignore', inplace = True)
        _t.drop(columns=listed_in_thermal_but_should_be_hydro, errors='ignore', inplace = True)
        subfields_dfs['Thermal'] = pd.concat([subfields_dfs['Thermal'], move_to_thermal], axis = 1)
        subfields_dfs['Hydro'] = pd.concat([subfields_dfs['Hydro'], move_to_hydro], axis = 1)

        for subfield in ['Thermal', 'Hydro']:

            df = subfields_dfs[subfield]


            for suffix, row_locator in row_content.items():

                new_subfield = subfield + '_' + suffix
                new_df = pd.DataFrame(dict(zip(df.columns, df.loc[row_locator, :].values)), index=[0])

                if subfield == 'Thermal':
                    new_df = self.fix_irregularities(new_df)

                applicable_dates = self.period_dates.copy()
                if suffix == 'Actual' and 'ISP3' not in self.report_name: # ISP3 predicts at day D 08:00 for day D later.
                    applicable_dates = list(map(lambda x: x - pd.Timedelta('1D'), applicable_dates))

                new_df = self.apply_datetime_index(new_df, applicable_dates)

                new_subfields_dfs[new_subfield] = new_df

        return new_subfields_dfs

    # *******  *******   *******   *******   *******   *******   *******
    def fix_irregularities(self, df):

        # admie, sometimes, e.g. 13-Jan-2021, instead of putting the aggregated unit information,
        # puts both the aggregated, and the per-unit (GT, 2GT, 3GT, etc) info.
        # but Heron1, heron2, heron3 behave differently as always.
        # ELPEDISON has also issues. I use a highh-level replacer, directly from the config file.
        # So, originally, ELPEDIOSN_THES_g becomes: ELPEDISON_THESS, and ELPEDISON_THIS_G becomes ELPEDISON_THISVI
        # Also, HERON1_G bcomes HERON1, Heron2-g becomes HERON2, etc
        # irregularity_check: see if "GT" or "ST" or "O" are included in the columns: delete them, the info has been extracted.

        irregular_flags = df.columns.str.contains('_*GT|_ST|_O')

        if any(irregular_flags):
            # we dont need to split HERON to HERON1, HERON2, HERON3, since, the irregularity has them splitted.
            keep_indices = [i for i,flag in enumerate(irregular_flags) if flag == False]
            df = df[df.columns[keep_indices]].copy()

        if 'HERON' in df.columns:
            df = self.split_HERON_to_GTs(df)

        df = df.drop(columns=[0], errors='ignore') # on April 2nd 2013, instead of Ag Dimitrios 1, it has a zero value

        return df

    # *******  *******   *******   *******   *******   *******   *******
    def split_HERON_to_GTs(self, df):

        unit_name = 'HERON'
        max_aggregated_capacity = 147
        available_aggregated_capacity = df[[unit_name]].squeeze()
        max_unit_capacity = 49
        n_gts = 3

        try:
            n_full_gts = df[unit_name].div(max_unit_capacity).astype(int).squeeze()
            resid = df[unit_name].mod(max_unit_capacity).astype(int).squeeze()
        except: # the unavailability_reason must have been passed as df
            heron_vals = df['HERON'].values
            df = df.assign(HERON1=heron_vals,
                           HERON2=heron_vals,
                           HERON3=heron_vals)
            df.pop('HERON')
            return df

        avails = []
        ''' arbitrary availability merit order, I assume that if one unit available
            , this will be heron1, if two units, heron1, heron2, erc.'''
        for unit in range(1, n_full_gts + 1, 1):
            avails.append(max_unit_capacity)
        if n_full_gts < n_gts:
            avails.append(resid)
        for unit in range(n_full_gts + 1, n_gts + 1, 1):
            avails.append(0)

        avails = dict(zip([1, 2, 3], avails))
        index_of_heron_col = df.columns.get_loc(unit_name)
        for id in range(1, n_gts + 1, 1):
            df.insert(index_of_heron_col + id, unit_name + str(id), avails[id])
        df.pop(unit_name)

        return df

    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******


###############################################################################################
class ISP1UnitAvailabilities(DayAheadSchedulingUnitAvailabilities):
    pass
###############################################################################################
class ISP2UnitAvailabilities(DayAheadSchedulingUnitAvailabilities):
    pass
###############################################################################################
class ISP3UnitAvailabilities(DayAheadSchedulingUnitAvailabilities):
    pass
###############################################################################################
###############################################################################################
###############################################################################################
class DAS(ISP1ISPResults): # DAS from henex
    def param_updater(self):
        self.last_column_trigger = '25'
        self.drop_last = False
    # *******  *******   *******   *******   *******   *******   *******
    def rolling_post_proc(self, subfields_dfs):
        # subfields_dfs = super().rolling_post_proc(subfields_dfs)
        for subfield, df in subfields_dfs.items():

            if self.field == 'SecondaryReserve':
                if subfield == 'SecondaryReserve':
                    new_cols = ['Requirements_Up', 'Requirements_Down', 'Price_Up', 'Price_Down']
                else:
                    ''' i dont remember what this does'''
                    left_parts = df.columns.to_list()
                    left_parts = [left_parts[i] if isinstance(left_parts[i], str) else left_parts[i-1] for i in range(len(left_parts))]

                    right_parts = ['_Up' if divmod(i,2)[1]==0 else '_Down' for i in range(len(left_parts)) ]
                    new_cols = [a+b for a,b in zip(left_parts, right_parts)]

                df.columns = new_cols
                df = df.iloc[1:]

            df = df.iloc[:self.period_dates.size]
            df = self.apply_datetime_index(df, self.period_dates)
            subfields_dfs[subfield] = df

        return subfields_dfs

    # *******  *******   *******   *******   *******   *******   *******
    def expost(self, fields_dfs):
        return fields_dfs
    # *******  *******   *******   *******   *******   *******   *******



###############################################################################################
###############################################################################################
###############################################################################################
class ReservoirFillingRate(ArchetypeHorizontal):
    def param_updater(self):
        self.drop_col_settings.update(startswith='Unnamed')
        self.dropna_settings.update(how= None, axis= 'columns', thresh=5)  # at least 5 non-nan to keep the col. Why 5? cause it works
        self.action_if_empty_df = 'return_compat'
        self.empty_df_filler.update(fill_value = np.nan)
        self.skip_rows_at_final = 1

    # *******  *******   *******   *******   *******   *******   *******
    def split_cue_points(self, df):
        the_only_key = list(self.cue_dict[self.field].keys())[0]
        return {the_only_key: df}
    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******

###############################################################################################
###############################################################################################
###############################################################################################
class ISP1DayAheadRESForecast(ArchetypeHorizontal):
    def param_updater(self):
        self.dropna_settings.update(how='all', axis='columns')  # at least 5 non-nan to keep the col. Why 5? cause it works
        self.skip_rows_at_final = 1

    # *******  *******   *******   *******   *******   *******   *******

###############################################################################################
class ISP2DayAheadRESForecast(ISP1DayAheadRESForecast):
    pass
###############################################################################################
class ISP3IntraDayRESForecast(ISP1DayAheadRESForecast):
    pass
###############################################################################################
class ISP1DayAheadLoadForecast(ISP1DayAheadRESForecast):
    pass
###############################################################################################
class ISP2DayAheadLoadForecast(ISP1DayAheadRESForecast):
    pass
###############################################################################################
class ISP3IntraDayLoadForecast(ISP1DayAheadRESForecast):
    pass
###############################################################################################
class DayAheadLoadForecast(ArchetypeHorizontal):
    def param_updater(self):
        self.drop_col_settings.update({'startswith': ['Unnamed']})

    pass
###############################################################################################
class DayAheadRESForecast(ArchetypeHorizontal):
    pass
###############################################################################################
###############################################################################################
###############################################################################################
class SystemRealizationSCADA(ArchetypeHorizontal):
    ''' This is very ad-hoc, because admie keeps changing the names and languages of everything. also, a lot of greek.
        Also, no daylight-saving awareness.
    '''
    def param_updater(self):
        self.drop_col_settings = {'col_names':['HERON_invalid'], 'startswith':['Unnamed'], 'error_action':'ignore'}

    # *******  *******   *******   *******   *******   *******   *******
    def set_ccgt_modes_switch(self, retain_modes = False):
        #todo: give the retina_modes, keep_ccgt_modes a bit more intuitive name
        self.keep_ccgt_modes = retain_modes

    # *******  *******   *******   *******   *******   *******   *******
    def rolling_post_proc(self, subfields_dfs):
        subfields_dfs = super().rolling_post_proc(subfields_dfs)
        new_subfields = copy.deepcopy(subfields_dfs)
        for subfield, df in subfields_dfs.items():

            if subfield == 'NaturalGas':
                # ELPEDISON HAS ST AND GT. But, the name is ELPEDISON_THISVI and ELPEDISON_THISVI_ST, intead of _GT and _ST
                df = df.rename(columns = {'ELPEDISON_THISVI':'ELPEDISON_THISVI_GT'})
                dense, ccgt_wide, ccgt_modes = self.deal_with_ccgts(df)
                # The plan is: Merge Natural gas dense, Lignite and oil
                # keep the natural_gas wide,. but only for the ccgt columns (since, the non-ccgt plants will be contained fully in the Thermal)
                subfield = "CCGTs"
                df = ccgt_wide.copy()
                new_subfields['CCGT_modes'] = ccgt_modes
                new_subfields['NaturalGas_dense'] = dense

            elif subfield == 'RES':
                df.columns = list(map(lambda x: unidecode(x.strip()), df.columns.to_list()))

            df = self.drop_columns_if(df,
                                      col_names=self.drop_col_settings['col_names'],
                                      startswith=self.drop_col_settings['startswith'],
                                      error_action=self.drop_col_settings['error_action'],
                                      field=self.field,
                                      subfield=subfield)

            df = df.loc[:, ~df.columns.isna()] # sometimes occur "nan" as column-names
            new_subfields[subfield] = df.copy()

        new_subfields['Thermal'] = pd.concat([new_subfields['Lignite'], new_subfields['NaturalGas_dense'], new_subfields['Oil']], axis =1)
        new_subfields.pop('NaturalGas_dense')
        new_subfields.pop('NaturalGas')
        new_subfields.pop('Lignite')
        new_subfields.pop('Oil')

        return new_subfields

    # *******  *******   *******   *******   *******   *******   *******
    def discover_ccgt_columns(self, col_list):
        ccgt_cols = [c for c in col_list if re.search('_GT|_ST', c)]
        ccgt_names = np.unique([re.search('.*(?=_GT|_ST)', c).group() for c in ccgt_cols])
        ccgt_pack = {name: [c for c in ccgt_cols if name in c] for name in ccgt_names}
        return ccgt_pack

    # *******  *******   *******   *******   *******   *******   *******
    def derive_ccgt_modes(self, ccgt_df, plant_name):
        '''
        :param ccgt_df: dataframe containing only the ccgt-plants columns, of the form:
                        <plant>_ST, <plant>_GT1, ..., <plant>_GTn
        :param plant_name:
        :return:
        '''
        gas_turbines = ccgt_df.columns[~ccgt_df.columns.str.contains("_ST")]

        modes = ccgt_df[gas_turbines].astype(bool).sum(axis=1).astype(str) + "GT"
        modes += "+" + ccgt_df[plant_name + "_ST"].astype(bool).astype(int).astype(str) + "ST"

        # the output is "", "ST". "1GT+ST", "2GT+ST", etc.
        modes = modes.replace({r'\+0ST': '', '1ST': 'ST', '0GT': ''}, regex=True)
        return modes

    # *******  *******   *******   *******   *******   *******   *******
    def deal_with_ccgts(self, subdf):

        all_cols = subdf.columns.to_list()
        ccgt_mapping = self.discover_ccgt_columns(all_cols)

        dense_df = subdf.copy()
        modes = pd.DataFrame(index=subdf.index)
        all_ccgt_units = []
        for plant_name, plant_units in ccgt_mapping.items():
            # to avoid changing the order of columns, I add in-place, on the first unit of each plant.
            # Then I rename from unit_name --> plant_name
            dense_df[plant_units[0]] = dense_df[plant_units].sum(axis = 1)
            dense_df = dense_df.rename(columns = {plant_units[0]:plant_name})
            dense_df = dense_df.drop(columns=plant_units, errors='ignore')

            modes[plant_name] = self.derive_ccgt_modes(subdf[plant_units], plant_name )
            all_ccgt_units.extend(plant_units)

        ccgt_wide = subdf[all_ccgt_units].copy()


        return dense_df,ccgt_wide, modes


###############################################################################################
###############################################################################################
###############################################################################################
class DAM_ResultsSummary(ArchetypeHorizontal):
    #todo: (general) define which opeartions are pre and which are post
    #   (e.g. drop columns, so far is only ex-post, on the transposed dataframe, but it is usefrul to perform also in the raw)
    def param_updater(self):
        self.dropna_settings = {'how':None, 'thresh':1, 'axis':'columns'}
        self.renamer_mapping.update({' IMPORTS':'IMPORTS',
                                     ' IMPORTS (IMPLICIT)':'IMPORTS (IMPLICIT)'})


    # *******  *******   *******   *******   *******   *******   *******
    def pre_proc(self, df):
        str_columns = [df.columns[i] for i in range(df.shape[1]) if type(df.columns[i])==str]
        df = df.drop(columns =str_columns, errors = 'ignore')
        df = super().pre_proc(df)
        return df


###############################################################################################
class IDM_LIDA1_ResultsSummary(DAM_ResultsSummary):
    pass
###############################################################################################
class IDM_LIDA2_ResultsSummary(DAM_ResultsSummary):
    pass
###############################################################################################
class IDM_LIDA3_ResultsSummary(DAM_ResultsSummary):
    pass
###############################################################################################
class IDM_CRIDA1_ResultsSummary(DAM_ResultsSummary):
    pass
###############################################################################################
class IDM_CRIDA2_ResultsSummary(DAM_ResultsSummary):
    pass
###############################################################################################
class IDM_CRIDA3_ResultsSummary(DAM_ResultsSummary):
    pass
###############################################################################################
class IDM_IDA1_ResultsSummary(DAM_ResultsSummary):
    pass
###############################################################################################
class IDM_IDA2_ResultsSummary(DAM_ResultsSummary):
    pass
###############################################################################################
class IDM_IDA3_ResultsSummary(DAM_ResultsSummary):
    pass
###############################################################################################
class DAM_PreMarketSummary(DAM_ResultsSummary):
    pass
###############################################################################################
###############################################################################################
###############################################################################################
class ISP1Requirements(ArchetypeHorizontal):
    def param_updater(self):
        self.last_column_trigger = '23:30'
        self.drop_last = False

    # *********************************************
    def transposeAll(self, subfields_dfs):
        ''' due to ISPrequirements peculiarity, the actual mandatory hydro names are in column 1
            instead of the indicator column.
            Basically, I need to keep the the df.iloc[:,1:], and then drop_at_final = 1, as usually
        '''
        for subfield, df in subfields_dfs.items():
            df = df.iloc[:,1:].T.copy().reset_index(drop=True)
            df.columns = df.iloc[max(self.skip_rows_at_final - 1, 0)].values
            df = df.iloc[self.skip_rows_at_final:].copy().reset_index(drop=True)
            subfields_dfs[subfield] = df

        return subfields_dfs
    # *********************************************

###############################################################################################
class ISP2Requirements(ISP1Requirements):
    pass
###############################################################################################
class ISP3Requirements(ISP1Requirements):
    pass
###############################################################################################
###############################################################################################
###############################################################################################
class LTPTRsNominationsSummary(ArchetypeHorizontal):
    def param_updater(self):
        self.drop_col_settings.update({'col_names':['Sum']})
    pass

###############################################################################################
###############################################################################################
###############################################################################################
class RealTimeSCADARES(ArchetypeHorizontal):
    ''' This handles dst as following:
        Always has 25 columns (for hours).
        When a day is normal 24-hour long, the last column is zero (not NaN)
        When a day is 25-hour long, all values filled
        When a day is 23-hours (spring dst), the last two columns are zero.

        So, I just drop the last n columns (n = df.shape - len(period_dates))
        '''
    # *******  *******   *******   *******   *******   *******   *******
    def param_updater(self):
        self.dropna_settings.update(how= None, axis= 'rows', thresh=5)  # at least 5 non-nan to keep the col. Why 5? cause it works
        self.drop_col_settings.update(col_names=['Date'])


    # *******  *******   *******   *******   *******   *******   *******
    def rolling_post_proc(self, subfields_dfs):

        for subfield, df in subfields_dfs.items():
            df = df.iloc[:len(self.period_dates)].copy()
            if df.shape[1] > 1:
                df.columns = [self.field, 'Date', 'CreteHVAC']
            subfields_dfs[subfield] = df

        subfields_dfs = super().rolling_post_proc(subfields_dfs)

        return subfields_dfs


###############################################################################################
class RealTimeSCADASystemLoad(RealTimeSCADARES):
    pass

###############################################################################################
###############################################################################################
###############################################################################################

