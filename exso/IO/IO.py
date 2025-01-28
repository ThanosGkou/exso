import os
import traceback
import warnings
import pandas as pd
import exso


# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
# *******  *******   *******   *******   *******   *******   *******
class IO:
    def __init__(self):
        pass

    # *******  *******   *******   *******   *******   *******   *******
    @staticmethod
    def read_file(filepath, usecols = None, timezone_as_read = None, convert_to_timezone = None, logger = None, nrows = None):
        #TODO: Not only here, but also in the export/update: consistent datetime format, not by luck, but purposedly

        if logger:
            logger.info("Reading filepath: {}".format(filepath))

        if not os.path.exists(filepath):
            return pd.DataFrame()

        try:
            if 'SupplyCurves' in filepath.name:
                warnings.simplefilter('ignore')
            df = pd.read_csv(filepath, index_col=0, header=0, encoding='utf-8', sep= exso._list_sep, decimal=exso._decimal_sep, nrows=nrows)
        except:
            df = pd.read_csv(filepath, index_col=0, header=0, encoding='utf-16', sep= exso._list_sep, decimal=exso._decimal_sep)
        if nrows:
            return df
        if usecols:
            df = df[usecols]
        try:
            df.index = pd.to_datetime(df.index, format=exso._dt_format)
        except:
            try:
                df.index = pd.to_datetime(df.index, format='%Y-%m-%d') # e.g. ReservoirFilling rate (daily resolution YYYY-MM-DD)

            except:
                df.index = pd.to_datetime(df.index, format='mixed')

        if timezone_as_read:
            df = df.tz_localize(timezone_as_read, ambiguous='infer')
            if convert_to_timezone:
                df = df.tz_convert(convert_to_timezone)

        if logger:
            logger.info("\tSuccessfully read.")
        return df
    # *******  *******   *******   *******   *******   *******   *******
    @staticmethod
    def write_file(filepath, df, convert_to_timezone=None, include_timezone=False, logger = None, mode = 'a'):
        if logger:
            logger.info("Writing to filepath: {}".format(filepath))

        # make it writeable
        # if os.path.exists(str(filepath)):
        # os.chmod(str(filepath),mode=stat.S_IWUSR)
        frame = df.copy()

        if isinstance(df.index, pd.MultiIndex):
            pass
        else:
            if convert_to_timezone:
                frame = frame.tz_convert(convert_to_timezone)
            if include_timezone == False:
                frame = frame.tz_localize(None)

            frame.index = pd.to_datetime(frame.index, format=exso._dt_format)

        if mode == 'a' and filepath.exists():
            header = False
        else:
            header = True

        if 'Unavailability_Reason' in filepath.name:
            encoding = 'utf-16'
        else:
            encoding = 'utf-8'

        frame.to_csv(filepath, mode=mode, header=header, encoding=encoding, sep= exso._list_sep, decimal=exso._decimal_sep, date_format=exso._dt_format)

        #make it read-only
        # os.chmod(str(filepath), mode=stat.S_IROTH)
        if logger:
            logger.info("\tSuccessfully written.")
    # *******  *******   *******   *******   *******   *******   *******

    def check_integrity(self, df):
        pass
    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******
    # *******  *******   *******   *******   *******   *******   *******

