#####################################
# ColumnAttributes.py
#####################################
# Description:
# * Immutable container of ColumnAttributes 
# for single entity.

from Columns.ColumnAttribute import ColumnAttribute
from Columns.ColumnRelationships import ColumnRelationships
from datetime import datetime
import os
import pandas
from pandas import DataFrame
from sortedcontainers import SortedDict

class ColumnAttributes(object):
    """
    * Container of multiple ColAttributes for single entity.
    """
    def __init__(self, path, fileDateFormat, sheet = None, delim = None):
        """
        * Instantiate object containing meta information about
        dataset located at path.
        Inputs:
        * path: String path to dataset.
        * fileDateFormat: dictionary containing 'dateformat' and 
        'regex' as attributes.
        Optional:
        * sheet: String name of sheet in workbook located at path.
        * delim: String delimiter for csv file.
        """
        self.__path = path
        self.__colcount = None
        self.__sheetname = sheet
        # Map { ColName -> ColAttribute }:
        self.__attributes = SortedDict()
        self.__error = ''
        self.__GetFileDate(fileDateFormat)
        self.__ParseFile(delim)

    def __eq__(self, attributes):
        """
        * Equality operator overload.
        """
        if not isinstance(attributes, ColumnAttributes):
            raise Exception('equality operator not supported with %s.' % str(type(attributes)))
        for attr in attributes.Attributes:
            if attr not in self.Attributes:
                return False
            if attributes.Attributes[attr] != self.Attributes[attr]:
                return False
        return True

    def __sub__(self, attributes):
        """
        * Subtraction operator overload.
        Set difference that returns all columns 
        with differing metadata.
        """
        if not isinstance(attributes, ColumnAttributes):
            raise Exception('subtraction operator not supported with %s.' % str(type(attributes)))
        diffs = []
        for attr in self.Attributes:
            if attr in attributes.Attributes and self.Attributes[attr] != attributes.Attributes[attr]:
                diffs.append(self.Attributes[attr] - attributes.Attributes[attr])
        return diffs

    ######################
    # Properties:
    ######################
    @property
    def Attributes(self):
        return self.__attributes
    @property
    def Error(self):
        return self.__error
    @property
    def FileDate(self):
        return self.__fileDate
    @property
    def Relationships(self):
        return self.__relationships
    @property
    def RowCount(self):
        return self.__rowCount
    @property
    def Path(self):
        return self.__path
    @RowCount.setter
    def RowCount(self, val):
        self.__rowCount = val

    ######################
    # Interface Methods:
    ######################
    @classmethod
    def LeastRestrictive(cls, colAttrLeft, colAttrRight):
        """
        * Return the ColumnAttributes object that has the least
        restrictive column attributes.
        * colAttrLeft, colAttrRight: ColumnAttributes objects.
        Must have identical column names.
        """
        errs = []
        if not isinstance(colAttrLeft, ColumnAttributes):
            errs.append('colAttrLeft must be a ColumnAttributes object.')
        if not isinstance(colAttrRight, ColumnAttributes):
            errs.append('colAttrRight must be a ColumnAttributes object.')
        if not errs:
            missing = set(colAttrLeft.Attributes).symmetric_difference(set(colAttrRight.Attributes))
            if missing:
                errs.append('The following columns are missing from both colAttrLeft and colAttrRight: %s' % ','.join(missing))
        if errs:
            raise Exception('\n'.join(errs))
        # Return the least restrictive ColumnAttributes object:
        attrs = {}
        for col in colAttrLeft.Attributes:
            pass

    ######################
    # Private Helpers:
    ######################
    def __ParseFile(self, delim = None):
        """
        * Parse column all column attributes in DataFrame.
        """
        try:
            data = self.__GetFileData(delim)
        except BaseException as ex:
            self.__error = str(ex)
            return

        # Store attributes of each column:
        self.__ParseHeaderRow(data)
        for col in data.columns:
            self.__attributes[col].ParseColumn(data[col])
        # Map all relationships between columns:
        self.__relationships = ColumnRelationships(data)

    def __ParseHeaderRow(self, data):
        """
        * Parse header row in file.
        """
        self.__attributes = { col : ColumnAttribute(col) for col in data.columns }
        # Map column name to index and back:
        self.__colNumToCol = { num : col for num, col in enumerate(data.columns) }
        self.__colToColNum = { self.__colNumToCol[num] : num for num in self.__colNumToCol }

    def __GetFileDate(self, format):
        """
        * Extract FileDate from file (every file must have a file date).
        """
        format = { arg.lower() : format[arg] for arg in format }
        filename = os.path.split(self.__path)[1]
        try:
            match = format['regex'].search(filename)[0]
        except Exception as ex:
            raise Exception('File regex does not work with one or more filepaths.')
        self.__fileDate = datetime.strptime(match, format['dateformat'])

    def __GetFileData(self, delim = None):
        """
        * Pull dataset from file at stored path.
        """
        if '.csv' in self.__path:
            return pandas.read_csv(self.__path, delimiter = delim)
        elif '.xls' in self.__path:
            return pandas.read_excel(self.__path, sheet_name = (0 if self.__sheetname is None else self.__sheetname))