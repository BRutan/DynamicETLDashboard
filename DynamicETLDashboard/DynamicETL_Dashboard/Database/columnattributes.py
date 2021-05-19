#####################################
# Database/columnattributes.py
#####################################
# Description:
# * Conversion methods for columns 
# dtypes. Get attributes about columns
# (if nullable, unique, etc).

from abc import ABC
from Columns.ColumnAttribute import ColumnAttribute
import numpy as np
from pandas import DataFrame, Series, read_csv
import string

# Note for dtypes:
# < = little-endian (LSB first)
# > = big-endian (MSB first)

class ColumnAttributesGenerator(ABC):
    """
    * Converts DataFrame columns
    with dtype to numpy dtypes
    that can be more easily mapped
    to SQL types, and gets column attributes
    that are common to all SQL languages.
    """
    # https://www.postgresql.org/docs/9.5/datatype.html
    # {'i':' ', 'b':' ', 'u':' ', 'f':' ', 'c':' ', 'm':' ', 'M':' ', 'O':' ', 'S':' ', 'U':' ', 'V':' ', '?': ''}
    __numerictps = {'i','u','f'}
    __underscore = set(string.punctuation)
    __underscore.add(' ')
    __underscore.add('\n')
    @staticmethod
    def GetNumericPrecision(series):
        """
        * Get maximum numeric precision of 
        passed DataFrame series.
        Inputs:
        * series: Series object. Must have numeric dtype.
        Outputs:
        * Maximum [digits, mantissa] of series.
        """
        if not isinstance(series, Series):
            raise ValueError('series must be a DataFrame Series object.')
        elif not any([tp in series.dtype.str for tp in ColumnAttributesGenerator.__numerictps]):
            raise ValueError('series dtypes must be one of %s' % ','.join(ColumnAttributesGenerator.__numerictps))
        series = series.astype('str').dropna()
        digits = [len(val[0:val.find('.')]) for val in series]
        mantissa = [len(val[val.find('.'):]) for val in series]
        maxdigit = np.argmax(digits)
        maxmantissa = np.argmax(mantissa)
        return (digits[maxdigit], mantissa[maxmantissa])

    @staticmethod
    def StandardizeNames(columns):
        """
        * Standardize column names 
        (lowercase, replace punctuation with underscores).
        Inputs:
        * columns: Iterable of strings or DataFrame.
        """
        if not isinstance(columns, DataFrame):
            if not hasattr(columns, '__iter__'):
                raise ValueError('columns must be a DataFrame or iterable of strings.')
            elif not all([isinstance(col, str) for col in columns]):
                raise ValueError('columns must only contain strings if an iterable.')
            out = []
            for col in columns:
                val = col
                for ch in ColumnAttributesGenerator.__underscore:
                    # Replace punctuation and spaces with undercores:
                    val = val.replace(ch, '_') if val != '#' else val.replace(ch, 'num')
                # Compress multiple underscores to single underscore:
                for num in range(len(val), 0, -1):
                    val = val.replace('_' * num, '_')
                val = val.strip('_')
                out.append(val.lower())
            return out
        else:
            cols = ColumnAttributesGenerator.StandardizeNames(columns.columns)
            return columns.rename(columns = {columns.columns[i] : cols[i] for i in range(len(cols))})

    @staticmethod
    def GetMaxStringLen(series):
        """
        * Get maximum number of characters
        necessary for strings in passed
        DataFrame series.
        Inputs:
        * series: Series object. Must have object dtype.
        """
        if not isinstance(series, Series):
            raise ValueError('series must be a DataFrame Series object.')
        elif series.dtype != 'object':
            raise ValueError('series must have object dtype.')
        if len(series) == 0:
            return 0
        lengths = series.dropna().apply(lambda x : len(x))
        lengths = list(lengths)
        maxlenidx = np.argmax(lengths)
        maxlenidx = maxlenidx[0] if hasattr(maxlenidx, '__iter__') else maxlenidx
        return lengths[maxlenidx]

    @staticmethod
    def GetColumnTypes(df):
        """
        * Get specific numpy
        dtypes from passed dataframe 
        columns.
        Inputs:
        * df: Pandas DataFrame or individual Series.
        Output:
        * List of [('columnname', 'numpydtype')].
        """
        if not isinstance(df, (DataFrame, Series)):
            raise ValueError('df must be a DataFrame or Series.')
        if isinstance(df, DataFrame):
            rows = df.to_records()
            descrs = rows.dtype.descr
            return [elem for elem in descrs if not elem[0] == 'index']
        else:
            column = df
            name = column.name
            tp = column.dtype.descr[0][1]
            return [(name, tp)]

    @staticmethod
    def GetColumnAttributes(df):
        """
        * Get attributes about columns
        (whether unique, has nulls, etc).
        Inputs:
        * df
        """
        if not isinstance(df, (DataFrame, Series)):
            raise ValueError('df must be a DataFrame or Series.')
        if isinstance(df, DataFrame):
            attrs = {}
            for col in df.columns:
                attrs[col] = ColumnAttributesGenerator.GetColumnAttributes(df[col])
            return attrs
        else:
            column = df
            out = {}
            out['name'] = column.name
            out['uniques'] = set(column.drop_duplicates())
            out['uniquecount'] = len(out['uniques'])
            out['nullable'] = any(column.isna())
            out['isunique'] = not out['nullable'] and out['uniquecount'] == len(column)
            return out