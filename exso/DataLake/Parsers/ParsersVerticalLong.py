import datetime
import logging
import re
import sys
import traceback

import numpy as np
import pandas as pd
from exso.DataLake.Parsers.ParsersArch import Archetype
from exso.Utils.DateTime import DateTime

# *********************************************

date_lambda = lambda x: datetime.datetime.strftime(DateTime.date_magician(x, return_stamp = False), format="%d-%b-%y")
logger = logging.getLogger(__name__)

# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******

class ArchetypeLong(Archetype):

    # *******  *******   *******   *******   *******   *******   *******
    def param_setter(self):
        self.index_cols = ['SORT', 'DELIVERY_MTU', 'MCP']
        self.index_cols_to_keep = ['MCP']
        self.eigen_cols = ['ASSET_DESCR', 'CLASSIFICATION']
        self.payload_cols = ['TOTAL_TRADES']
        self.swap_eigenvalues = {}
        self.replacer_mapping = {}
        self.mode = "collapsed"
        self.drop_col_settings = {'error_action':'ignore', 'col_names':[""],
                                  'startswith':None}
        self.side_indicator_col = None

    # *******  *******   *******   *******   *******   *******   *******
    def param_updater(self):
        pass

    # *******  *******   *******   *******   *******   *******   *******
    def pre_proc(self, df):
        df = df.fillna("")

        if self.replacer_mapping:

            df = self.apply_replacements(df, replacer_mapping = self.replacer_mapping)

        if self.regex_mapping:

            df = self.apply_replacements(df, replacer_mapping = self.replacer_mapping, regex = True)

        if self.swap_eigenvalues:
            df = self._swap_values_of_eigen_cols_where(df, where=self.swap_eigenvalues['where'], isin=self.swap_eigenvalues['isin'])

        df = self._make_eigen_col(df)

        return df

    # *******  *******   *******   *******   *******   *******   *******
    def _swap_values_of_eigen_cols_where(self, df, where:str, isin:list):
        # henex xas "imports" not as classification, but as asset-description....
        # and classification is e.g. AL-GR. Which is kind of stupid. So I swap them
        # the below is read as: get the slice of df where the "where" column values are anything within the "isin" list
        # for this slice, swap the values of eigen-col 1 with the values of eigen-col2
        # this assumes only 2 eigen-cols

        swappable_slice = df[df[where].isin(isin)].copy()
        df.loc[df.index.isin(swappable_slice.index), self.eigen_cols] = swappable_slice[self.eigen_cols[::-1]].values
        return df

    # *******  *******   *******   *******   *******   *******   *******
    def _make_eigen_col(self, df):
        if len(self.eigen_cols):
            df['eigen'] = df[self.eigen_cols[0]].astype(str)

        if len(self.eigen_cols) > 1:
            for col in self.eigen_cols[1:]:
                df['eigen'] += "_" + df[col].astype(str)

        return df

    # *******  *******   *******   *******   *******   *******   *******
    def _clean_pivot(self, df):

        df = (df.dropna(axis = 'rows', thresh=1).    # why 1?: just because. problematic files are expected to have only one non-Nan value row-wise.
              drop(0, level = 'SORT', axis = 'rows', errors='ignore').   # e.g. in 2022-11-26 of BlockOrders, there are a lot of 0 sort-index which are all empty
              reset_index(level=self.index_cols, drop=False).
              sort_index(axis = 1))

        df.columns = df.columns.remove_unused_levels()
        df = self._handle_extreme_stupidity(df)

        return df

    # *******  *******   *******   *******   *******   *******   *******
    def _handle_extreme_stupidity(self, df):

        ''' sometimes, this happens
            20   2023-03-23 19:00:00  187.34          NaN  500.0   NaN      NaN      NaN    NaN  
                                      187.35          51.0  NaN    NaN      NaN   400.000  720.0 
            So, same sort, same time, but the fucking price is a tiny bit different, and thus creates a new row.
        '''
        # process: first: get a bool mask array of whether delivery mtu is duplicate
        #                 not keep = False! I just want the unique indices

        #FIRST, check if duplication is caused by Autumn-DST....

        period_start = pd.to_datetime(df.xs('DELIVERY_MTU', axis = 1)[0]).tz_localize(self.period_dates[0].tzinfo)
        condition_for_DST = period_start.month == 10 and period_start.isoweekday() == 7 and (period_start + pd.Timedelta('7D')).month == 11

        if condition_for_DST:
            # only possible bug: If the extreme stupidity happens the same day that an Autumn DST happens, it will fail.
            # Because, the condition_forDST will be True, but the duplicate values will be actually there (df.shape > 25)
            return df

        is_duplicate_bool_mask = df.xs('DELIVERY_MTU', axis = 1).duplicated()
        # now, get the series: id, datetime of duplications
        dup_index = df.xs('DELIVERY_MTU', axis = 1)[is_duplicate_bool_mask]

        if dup_index.empty:
            return df

        # keep indices where this array is False
        keep_indices = is_duplicate_bool_mask[is_duplicate_bool_mask==False]
        # isolate the datetime values where duplication occurs
        datetime_where_dupl = dup_index.values

        # copy the old dataframe
        df_old = df.copy()
        # ... and keep a non-duplicate version of the dataframe
        df = df.loc[keep_indices.index].reset_index(drop=True)

        for dtm in datetime_where_dupl: # for each suplicateion dateteime
            subdf = df_old[df_old['DELIVERY_MTU'] == dtm] # the problematic subdf is this
            # transfer values/fillna between entries referring to the same datetime, that are not identical due to fucking henex
            # and keep only one of them (the first, or whatever)
            subdf = subdf.bfill().iloc[0]

            try:
                # this information-full subdf (which is actually a multi-idex series now)
                # shall replace the placeholder entry in the final dataframe.
                replace_in_index = df.xs('DELIVERY_MTU',axis = 1)[df.xs('DELIVERY_MTU',axis = 1) == dtm].index
                df.loc[replace_in_index] = subdf.values
            except:
                print('Failed')
                print(traceback.format_exc())
                sys.exit()

        return df

    # *******  *******   *******   *******   *******   *******   *******
    def split_cue_points(self, df):
        df = pd.pivot_table(df, index=self.index_cols, columns=self.eigen_cols, values=self.payload_cols)
        df = self._clean_pivot(df)
        return {self.field: df}


    # *******  *******   *******   *******   *******   *******   *******
    def rolling_post_proc(self, subfields_dfs):

        for subfield, df in subfields_dfs.items():
            if df.columns.nlevels == 1:
                # ok, the df is a single-index df, already in the dict
                pass
            else:
                if self.mode == 'collapsed':

                    subsubdfs = self.expand_multiindex(df)
                    #collapse the expanded (in dicts) original df into a single, wide df with single-index
                    df = self._collapse(subsubdfs)

                    if self.drop_col_settings:
                        df = self.drop_columns_if(df, **self.drop_col_settings)
                    # df = df.drop(columns = [""], errors='ignore')

                    subfields_dfs[subfield] = df

                else:
                    # nothing for now. In the expost, the expanded df will be added as new fields and subfields
                    pass

        return subfields_dfs

    # *******  *******   *******   *******   *******   *******   *******
    def _collapse(self, dict_of_dicts):

        df_agg = pd.DataFrame()
        for new_field, new_subfields_dfs in dict_of_dicts.items():
            for new_subfield, new_df in new_subfields_dfs.items():
                df_agg = pd.concat([df_agg, new_df], axis=1)
        return df_agg


    # *******  *******   *******   *******   *******   *******   *******
    def expand_multiindex(self, df):

        # df = fields_dfs[self.field][self.field]
        levels = df.columns.levels
        new_dfs = {}
        for field in levels[0]:  # this will be the folder name

            if (field in self.index_cols) and (field not in self.index_cols_to_keep) or not field:
                continue

            new_dfs[field] = {}
            # print('\n\n')
            # print('Whole df')
            # print(df)

            subdf = df.xs(field, axis=1)
            # print('\n\n')
            # print(f'Field df for: {field = }')
            # print(subdf)

            if isinstance(subdf, pd.Series):
                subdf = subdf.to_frame()

            if subdf.empty:
                subdf = self.dimensionalize_empty_df(subdf)

            if subdf.columns.nlevels > 1:

                sublevs = subdf.columns.levels
                for subfield in sublevs[0]:  # this will be the filename
                    if subfield:  # str name not ""
                        # print('\n\n')
                        # print(f'Subfield dataframe for: {field = }, {subfield = }\n')

                        subsubdf = subdf.xs(subfield, axis=1)
                        # print(subsubdf)

                        if isinstance(subsubdf, pd.Series):
                            subsubdf = subsubdf.to_frame()

                        if subsubdf.empty:
                            subsubdf = self.dimensionalize_empty_df(subsubdf)


                        elif subsubdf.shape[0] < len(self.period_dates):

                            # print(subsubdf)
                            # input('that shouldn\'t be')
                            sorts = np.arange(1, len(self.period_dates) + 1, 1)
                            subsubdf = subsubdf.reindex(sorts)

                        subsubdf = subsubdf.fillna(0)
                        subsubdf = subsubdf.drop(subsubdf.columns[subsubdf.columns.str.startswith('Unnamed')],
                                                 axis=1, errors='ignore')

                        new_dfs[field][subfield] = subsubdf

            else:
                new_dfs[field][field] = subdf

        return new_dfs

    # *******  *******   *******   *******   *******   *******   *******
    def transposeAll(self, subfields_dfs):
        return subfields_dfs

    # *******  *******   *******   *******   *******   *******   *******
    def expost(self, fields_dfs):

        for field, subfields_dfs in fields_dfs.items():
            for subfield, df in subfields_dfs.items():
                fields_dfs[field][subfield] = self.apply_datetime_index(df, period_dates=self.period_dates)
                # df = fields_dfs[field][subfield]

        if self.mode == 'collapsed':
            return fields_dfs

        new_dfs = {}
        for field, subfields_dfs in fields_dfs.items():
            for subfield, df in subfields_dfs.items():
                try:
                    df.index.remove_unused_levels()
                except:
                    pass

                if df.columns.nlevels == 1:
                    new_dfs[field] = {}
                    if self.drop_col_settings:
                        df = self.drop_columns_if(df, **self.drop_col_settings)
                    new_dfs[field][subfield] = df

                else:
                    new_dfs_multiindex = self.expand_multiindex(df)
                    if self.drop_col_settings:
                        for field, subfields_dfs in new_dfs_multiindex.items():
                            new_dfs[field] = {}
                            for subfield, dff in subfields_dfs.items():
                                new_dfs[field][subfield] = self.drop_columns_if(dff, **self.drop_col_settings)
                    else:
                        new_dfs.update(**new_dfs_multiindex)

        return new_dfs

    # *******  *******   *******   *******   *******   *******   *******

    # *******  *******   *******   *******   *******   *******   *******

###############################################################################################
###############################################################################################
###############################################################################################
class _UsualParams(ArchetypeLong):
    def param_updater(self):
        self.side_indicator_col = 'SIDE_DESCR'
        self.swap_eigenvalues = {'where': 'CLASSIFICATION', 'isin': ['Imports', 'Exports']}
        self.replacer_mapping = {'Import': 'Imports', 'Export': 'Exports'}
        # self.drop_cols_settings.update({'startswith': "Unnamed",'col_names':[""]})


###############################################################################################
###############################################################################################
###############################################################################################
class DAM_Results(_UsualParams):

    def split_cue_points(self, df):

        side_col = self.side_indicator_col
        unique_sides = df[side_col].sort_values().unique()

        assert unique_sides[0].lower() == 'buy'
        assert unique_sides[1].lower() == 'sell'

        dfs = {}
        for side in unique_sides:
            dfside = df[df[self.side_indicator_col] == side]
            dfside = pd.pivot_table(dfside, index=self.index_cols, columns=self.eigen_cols, values=self.payload_cols)
            dfside = self._clean_pivot(dfside)
            dfs[side] = dfside
        return dfs

    # *******  *******   *******   *******   *******   *******   *******
    def rolling_post_proc(self, subfields_dfs):
        subfields_dfs = super().rolling_post_proc(subfields_dfs)
        gen_columns = ['RES', 'Lignite', 'Natural Gas', 'Big Hydro',
                       'CRETE CONVENTIONAL', 'CRETE RENEWABLES']
        load_columns = ['HV', 'MV', 'LV', 'LOSSES', 'PUMP', 'CRETE LOAD']
        demand_columns = [_ for _ in load_columns if _ != 'PUMP']
        netdf = pd.DataFrame()
        _sell = subfields_dfs['Sell'].copy()
        _buy = subfields_dfs['Buy'].copy()
        for asset in gen_columns:

            if asset in _sell.columns and asset in _buy.columns:
                netdf[asset] = _sell[asset] - _buy[asset]

            elif asset in _sell.columns:
                netdf[asset] = _sell[asset]

            elif asset in _buy.columns:
                netdf[asset] = -_buy[asset]

        for asset in load_columns:

            if asset in _sell.columns and asset in _buy.columns:
                netdf[asset] = _buy[asset] - _sell[asset]

            elif asset in _buy.columns:
                netdf[asset] = _buy[asset]

            elif asset in _sell.columns:
                netdf[asset] = -_sell[asset]



        importing_cols = _sell.columns[_sell.columns.str.endswith('-GR')]
        imports = _sell[importing_cols]
        imports.columns = list(map(lambda col: "-".join(col.split('-')[::-1]), imports.columns))
        imports *= -1

        exporting_cols = _buy.columns[_buy.columns.str.startswith('GR')]

        net_exchanges = _buy[exporting_cols] + imports

        netdf = pd.concat([netdf, net_exchanges], axis = 1)

        netdf['Demand'] = netdf[netdf.columns[netdf.columns.isin(demand_columns)]].sum(axis = 1)
        netdf['Generation'] = netdf[netdf.columns[netdf.columns.isin(gen_columns)]].sum(axis = 1)
        netdf['NetMarketSchedule'] = net_exchanges.sum(axis = 1)




        subfields_dfs['Net'] = netdf.copy()


        return subfields_dfs

###############################################################################################
class IDM_LIDA1_Results(DAM_Results):
    pass
###############################################################################################
class IDM_LIDA2_Results(DAM_Results):
    pass
###############################################################################################
class IDM_LIDA3_Results(DAM_Results):
    pass
###############################################################################################
class IDM_CRIDA1_Results(DAM_Results):
    pass
###############################################################################################
class IDM_CRIDA2_Results(DAM_Results):
    pass
###############################################################################################
class IDM_CRIDA3_Results(DAM_Results):
    pass
###############################################################################################
class IDM_IDA1_Results(DAM_Results):
    pass
###############################################################################################
class IDM_IDA2_Results(DAM_Results):
    pass
###############################################################################################
class IDM_IDA3_Results(DAM_Results):
    pass
###############################################################################################
###############################################################################################
###############################################################################################
class IDM_XBID_Results(_UsualParams):

    def param_updater(self):
        super().param_updater() # required because inherinting from DAM Results, not Archetype directly
        self.index_cols = ['DELIVERY_DATETIME']
        self.index_cols_to_keep = []
        self.eigen_cols = ['SIDE_DESCR', 'CLASSIFICATION']
        self.payload_cols = ['VWAP(€_MWh)', 'MIN_PRICE', 'MAX_PRICE', 'TOTAL_TRADES']
        self.mode = 'expanded'

    # *******  *******   *******   *******   *******   *******   *******
    def pre_proc(self, df):

        df.columns = df.columns.str.replace(r'\s', '', regex = True)
        df.columns = df.columns.str.replace('/', '_', regex = True)
        if self.swap_eigenvalues:
            df = self._swap_values_of_eigen_cols_where(df, where=self.swap_eigenvalues['where'], isin=self.swap_eigenvalues['isin'])

        df = self._make_eigen_col(df)
        return df

    # *******  *******   *******   *******   *******   *******   *******
    def split_cue_points(self, df):

        df = pd.pivot_table(df, index=self.index_cols, columns=self.eigen_cols, values=self.payload_cols)

        df =  df.reset_index(level=self.index_cols, drop=False).sort_index(axis = 1)

        return {self.field: df}

    # *******  *******   *******   *******   *******   *******   *******

###############################################################################################
###############################################################################################
###############################################################################################
class DAM_PhysicalDeliveriesOfftakes(_UsualParams):
    def param_updater(self):
        super().param_updater() # required because inherinting from DAM Results, not Archetype directly
        self.index_cols = ['SORT', 'DELIVERY_MTU']
        self.payload_cols = ['TOTAL_ORDERS']

    # *******  *******   *******   *******   *******   *******   *******

###############################################################################################
###############################################################################################
###############################################################################################
class DAM_BlockOrders(_UsualParams):
    def param_updater(self):
        super().param_updater() # required because inheriting from DAM Results, not Archetype directly
        self.index_cols = ['SORT', 'DELIVERY_MTU']
        self.index_cols_to_keep = []
        self.eigen_cols = ['SIDE_DESCR', 'CLASSIFICATION']
        self.payload_cols = ['TOTAL_ORDERS', 'TOTAL_QUANTITY','MATCHED_ORDERS', 'MATCHED_QUANTITY']
        self.mode = 'expanded'
        self.drop_col_settings = {'startswith': "Unnamed",'col_names':["", np.nan]}
    # *******  *******   *******   *******   *******   *******   *******


###############################################################################################
###############################################################################################
###############################################################################################
class DAM_GasVTP(ArchetypeLong):
    # *******  *******   *******   *******   *******   *******   *******
    def param_updater(self):
        self.index_cols = ['Trading Date']
        self.index_cols_to_keep = []
        self.eigen_cols = ['Contract']
        self.payload_cols = ['Start Price', 'Max Price', 'Min Price', 'Last Price', 'Closing Price', 'Previous Closing Price',
                             'Number of Orders', 'Number of Trades', 'Traded Quanity (No of Contracts)', 'Traded Volume (MWh)', 'VWAP',
                             'Traded Volume Pre Agreed (MWh)', 'VWAP MWh Pre Agreed', 'HGSIDA', 'HGSIWD', 'HGMBI', 'HGMSI']
        self.mode = "expanded"

    # *******  *******   *******   *******   *******   *******   *******
    def pre_proc(self, df: pd.DataFrame) -> pd.DataFrame:

        # self.payload_cols = ['Number Of Auctions', 'Auction Total Traded Quantity', 'Auction Total Traded Volume (MWh)']
        df.columns = list(map(lambda x: re.sub('\n','',x).strip(), df.columns.to_list()))
        df = pd.pivot_table(df, index=self.index_cols, columns=self.eigen_cols, values=self.payload_cols)
        df = df.dropna(axis='rows',
                       thresh=6)
        df = df.reset_index(level=self.index_cols, drop=False)
        df.columns = df.columns.remove_unused_levels()

        df = df.swaplevel(axis = 1)
        df = df.sort_index(axis=1)

        return df
    # *******  *******   *******   *******   *******   *******   *******

    def split_cue_points(self, df):
         return {self.field: df}

    # *******  *******   *******   *******   *******   *******   *******

###############################################################################################
###############################################################################################
###############################################################################################

class DAM_AggDemandSupplyCurves(ArchetypeLong):
    ''' dataframes leaving here, have already the correct timezone (utc, converted from cet)'''
    def param_updater(self):
        self.eigen_cols = ['SIDE_DESCR']
        self.mode = 'expanded'
        self.index_cols = ['SORT', 'DELIVERY_MTU', 'AA']
        self.index_cols_to_keep = ['DELIVERY_MTU', 'AA']
        self.payload_cols = ['QUANTITY', 'UNITPRICE']
        self.database_tzone = 'UTC'

    # *******  *******   *******   *******   *******   *******   *******
    def pre_proc(self, df: pd.DataFrame) -> pd.DataFrame:

        df = pd.pivot_table(df, index=self.index_cols, columns=self.eigen_cols, values=self.payload_cols)
        # df = df.reset_index(level=self.index_cols, drop=False)
        df.columns = df.columns.remove_unused_levels()
        df = df.swaplevel(axis = 1)
        df = df.sort_index(axis=1)

        df = df.droplevel(level='SORT', axis = 0)
        index_ = df.index
        period_dates_utc = self.period_dates.tz_convert(self.database_tzone).tz_localize(None)
        index_ = index_.set_levels(period_dates_utc, level='DELIVERY_MTU')
        index_ = index_.rename({'DELIVERY_MTU':''})
        df.index = index_

        return df

    # *******  *******   *******   *******   *******   *******   *******
    def transposeAll(self, subfields_dfs):
        return subfields_dfs

    # *******  *******   *******   *******   *******   *******   *******
    def rolling_post_proc(self, subfields_dfs):
        return subfields_dfs
    # *******  *******   *******   *******   *******   *******   *******
    def expost(self, fields_dfs):
        return fields_dfs

    # *******  *******   *******   *******   *******   *******   *******
    def split_cue_points(self, df):
        return {self.field: df}
    # *******  *******   *******   *******   *******   *******   *******

###############################################################################################
class IDM_LIDA1_AggDemandSupplyCurves(DAM_AggDemandSupplyCurves):
    pass
###############################################################################################
class IDM_LIDA2_AggDemandSupplyCurves(DAM_AggDemandSupplyCurves):
    pass
###############################################################################################
class IDM_LIDA3_AggDemandSupplyCurves(DAM_AggDemandSupplyCurves):
    pass
###############################################################################################
class IDM_CRIDA1_AggDemandSupplyCurves(DAM_AggDemandSupplyCurves):
    pass
###############################################################################################
class IDM_CRIDA2_AggDemandSupplyCurves(DAM_AggDemandSupplyCurves):
    pass
###############################################################################################
class IDM_CRIDA3_AggDemandSupplyCurves(DAM_AggDemandSupplyCurves):
    pass
class IDM_IDA1_AggDemandSupplyCurves(DAM_AggDemandSupplyCurves):
    pass
###############################################################################################
class IDM_IDA2_AggDemandSupplyCurves(DAM_AggDemandSupplyCurves):
    pass
###############################################################################################
class IDM_IDA3_AggDemandSupplyCurves(DAM_AggDemandSupplyCurves):
    pass
###############################################################################################
###############################################################################################
###############################################################################################
class ISPEnergyOffers(ArchetypeLong):
    def param_updater(self):
        self.eigen_cols = ['DIR']
        self.mode = 'collapsed'
        self.index_cols = ['ID_PERIOD', 'AA']
        self.index_cols_to_keep = ['ID_PERIOD', 'AA']
        self.payload_cols = ['QUANTITY_MW', 'PRICE']
        self.database_tzone = 'UTC'

    # *******  *******   *******   *******   *******   *******   *******
    def pre_proc(self, df: pd.DataFrame) -> pd.DataFrame:

        df = df.sort_values(by='DIR').reset_index(drop=True)
        df = df.drop(columns='SEG', errors='ignore')
        df = df.replace({'Dn':'Down'})
        # make a AA column to match the rest of the work done for Henex agg demand
        # wherever the time interval changes, put a 1, otherwise 0. Then multiply by the index
        df['AA'] = (df['ID_PERIOD'].diff() != pd.Timedelta(0,'D')).astype(int) * df.index

        # replace zeros with nans, extend from the closest previous non-zero
        # the first instance (first hour of the file) will be still NaN, so replace with zero, and convert to int.
        df['AA'] = (df['AA'].replace(0, np.nan).
                    ffill().
                    fillna(0).
                    astype(int))
        # subtract it from the linearly increasing index, to get the desired: 0 1 2 3 4 ... 150 0 1 2 3 4
        df['AA'] = df.index - df['AA']

        df = pd.pivot_table(df, index=self.index_cols, columns=self.eigen_cols, values=self.payload_cols, dropna=True)

        df = df.swaplevel(axis=1).sort_index(axis = 1)
        index_ = df.index
        period_dates_utc = self.period_dates.tz_convert(self.database_tzone).tz_localize(None)

        index_ = index_.set_levels(period_dates_utc, level='ID_PERIOD')#, verify_integrity = False)
        index_ = index_.rename({'ID_PERIOD': ''})
        df.index = index_
        return df
    # *******  *******   *******   *******   *******   *******   *******
    def transposeAll(self, subfields_dfs):
        return subfields_dfs

    # *******  *******   *******   *******   *******   *******   *******
    def rolling_post_proc(self, subfields_dfs):
        return subfields_dfs

    # *******  *******   *******   *******   *******   *******   *******
    def expost(self, fields_dfs):
        return fields_dfs

    # *******  *******   *******   *******   *******   *******   *******
    def split_cue_points(self, df):
        return {self.field: df}
    # *******  *******   *******   *******   *******   *******   *******

    # *******  *******   *******   *******   *******   *******   *******
###############################################################################################
###############################################################################################
###############################################################################################
class ISPCapacityOffers(ISPEnergyOffers):
    def param_updater(self):
        # *******  *******   *******   *******   *******   *******   *******
        self.eigen_cols = ['DIR', 'SERVICETYPE']
        self.mode = 'expanded'
        self.index_cols = ['ID_PERIOD', 'AA']
        self.index_cols_to_keep = ['ID_PERIOD', 'AA']
        self.payload_cols = ['QUANTITY_MW', 'PRICE']
        self.database_tzone = 'UTC'

    # *******  *******   *******   *******   *******   *******   *******
    def pre_proc(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().pre_proc(df)
        df = df.swaplevel(0,1,axis = 1).sort_index(axis =1) # bring top level the Service (FCR, etc), then the Price or Q, then the direction
        return df
    # *******  *******   *******   *******   *******   *******   *******
    def split_cue_points(self, df):

        subfields = df.columns.get_level_values(level = 0).unique() # get product types (FCR, aFRR, mFRR
        subfields_dfs = {subfield:df[subfield].swaplevel(0,1,axis = 1).sort_index(axis = 1) for subfield in subfields}
        # print(subfields)
        # print(subfields_dfs['FCR'])
        # print(df.head())
        # input('----')
        return subfields_dfs
    # *******  *******   *******   *******   *******   *******   *******
    def transposeAll(self, subfields_dfs):
        return subfields_dfs

    # *******  *******   *******   *******   *******   *******   *******
    def rolling_post_proc(self, subfields_dfs):
        return subfields_dfs

    # *******  *******   *******   *******   *******   *******   *******
    def expost(self, fields_dfs):
        return fields_dfs

    # *******  *******   *******   *******   *******   *******   *******


###############################################################################################
###############################################################################################
###############################################################################################

class DAM_MarketCoupling(_UsualParams):
    def param_updater(self):
        super().param_updater() # required because inherinting from DAM Results, not Archetype directly
        self.mode = 'collapsed'
        self.drop_col_settings.update({'col_names':['CBS_FLOW', '']})

    # *******  *******   *******   *******   *******   *******   *******
    def pre_proc(self, df):

        if self.field == 'BiddingZones':
            self.index_cols = ['SORT', 'DELIVERY_MTU']
            self.eigen_cols = ['BIDDING_ZONE_DESCR'] #placeholder
            self.payload_cols = ['NET_POSITION']
            df = df.fillna("")
            df = self._make_eigen_col(df)
            df = pd.pivot_table(df, index=self.index_cols, columns=self.eigen_cols, values=self.payload_cols)
            df = self._clean_pivot(df)

        elif self.field == 'Interconnectors':
            self.index_cols = ['SORT', 'DELIVERY_MTU']
            self.payload_cols = ['CBS_FLOW']
            self.eigen_cols = ['CBS_DESCR']
            df = df.fillna("")
            df = self._make_eigen_col(df)
            df = pd.pivot_table(df, index=self.index_cols, columns=self.eigen_cols, values=self.payload_cols)
            df = self._clean_pivot(df)

        return df

    # *******  *******   *******   *******   *******   *******   *******
    def split_cue_points(self, df):
        return {self.field: df}

###############################################################################################
class IDM_LIDA1_MarketCoupling(DAM_MarketCoupling):
    pass
class IDM_LIDA2_MarketCoupling(DAM_MarketCoupling):
    pass
class IDM_LIDA3_MarketCoupling(DAM_MarketCoupling):
    pass
class IDM_CRIDA1_MarketCoupling(DAM_MarketCoupling):
    pass
class IDM_CRIDA2_MarketCoupling(DAM_MarketCoupling):
    pass
class IDM_CRIDA3_MarketCoupling(DAM_MarketCoupling):
    pass
class IDM_IDA1_MarketCoupling(DAM_MarketCoupling):
    pass
class IDM_IDA2_MarketCoupling(DAM_MarketCoupling):
    pass
class IDM_IDA3_MarketCoupling(DAM_MarketCoupling):
    pass
###############################################################################################
###############################################################################################
