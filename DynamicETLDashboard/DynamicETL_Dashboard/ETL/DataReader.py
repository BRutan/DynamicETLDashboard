#####################################
# DataReader.py
#####################################
# Description:
# * Convert data in format into pandas DataFrame.

import dateutil.parser as dtparser
import numpy as np
from pandas import DataFrame, isnull, read_csv, read_excel
import re
import os
from DynamicETL_Dashboard.Utilities.Helpers import IsNumeric, StringIsDT

class DataReader:
    """
    * Encapsulate how data is read.
    """
    def __init__(self):
        """
        * Instantiate empty object.
        """
        pass
    ####################
    # Interface Methods:
    ####################
    @staticmethod
    def Read(path, sheetName = None, delim = None):
        """
        * Return pandas dataframe from data at path.
        Inputs:
        * path: path to file.
        Optional:
        * sheetName: Sheet name in xls type file to read.
        * delim: Delimiter if reading delimited file.
        """
        DataReader.__Validate(path, sheetName, delim)
        return DataReader.__ReadData(path, sheetName, delim)

    ####################
    # Private Helpers:
    ####################
    @staticmethod
    def __Validate(path, sheetName, delim):
        errs = []
        if not isinstance(path, str):
            errs.append('path must be a string.')
        elif not os.path.isfile(path):
            errs.append('path must point to file.')
        elif not os.path.exists(path):
            errs.append('File at path does not exist.')
        if not sheetName is None and not isinstance(sheetName, str):
            errs.append('sheetName must be a string.')
        if not delim is None and not isinstance(delim, str):
            errs.append('delim must be a string.')
        if errs:
            raise Exception('\n'.join(errs))

    @staticmethod
    def __ReadData(path, sheetName, delim):
        """
        * Read data at path.
        """
        if path.endswith('.csv'):
            data = read_csv(path, delimiter = (',' if delim is None else delim))
        elif path.endswith('.xls') or path.endswith('.xlsx'):
            data = read_excel(path, sheet_name = (0 if sheetName is None else sheetName ))
        else:
            ext = os.path.split(path)
            raise Exception('%s extension is invalid.' % ext)
        # Convert data into suitable types:
        return DataReader.__ConvertAll(data)

    @staticmethod
    def __ConvertAll(data):
        """
        * Convert all columns into most appropriate type.
        """
        for col in data.columns:
            if DataReader.__IsInt(data[col]):
                data[col] = data[col].astype('int64')
            elif DataReader.__IsFloat(data[col]):
                data[col] = data[col].astype('float64')
            elif DataReader.__IsDT(data[col]):
                data[col] = data[col].astype('datetime64')
        return data

    @staticmethod
    def __IsInt(series):
        """
        * Determine if TimeSeries object could be integer type.
        """
        if all(isnull(series)):
            return False
        for val in series:
            if not str(val).isnumeric() and not isnull(val):
                return False
        return True
    @staticmethod
    def __IsFloat(series):
        """
        * Determine if TimeSeries object is floating point.
        """
        if all(isnull(series)):
            return False
        isfloat = re.compile('^[0-9]+.?[0-9]*$')
        for val in series:
            if not isfloat.match(str(val)) and not isnull(val):
                return False
        return True
    @staticmethod
    def __IsDT(series):
        """
        * Determine if is datetime.
        """
        if all(isnull(series)):
            return False
        for val in series:
            if not StringIsDT(str(val)) and not isnull(val):
                return False
        return True