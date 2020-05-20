#####################################
# DataReader.py
#####################################
# Description:
# * Convert data in format into pandas DataFrame.

from pandas import DataFrame, read_csv, read_excel
import os

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
        if path.endswith('csv'):
            return read_csv(path, delimiter = (',' if delim is None else delim))
        elif path.endswith('xls'):
            return read_excel(path, sheet_name = (0 if sheetName is None else sheetName ))

