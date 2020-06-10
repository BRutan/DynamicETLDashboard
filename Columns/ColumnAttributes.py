#####################################
# ColumnAttributes.py
#####################################
# Description:
# * Immutable container of ColumnAttributes 
# for single entity.

from datetime import datetime
import os
import pandas
from pandas import DataFrame
from sortedcontainers import SortedDict
from Columns.ColumnAttribute import ColumnAttribute
from Columns.ColumnRelationships import ColumnRelationships

class ColumnAttributes(object):
    """
    * Container of multiple ColAttributes for single entity.
    """
    def __init__(self, path, fileDateFormat, sheet = None):
        """
        * Instantiate new object.
        """
        self.__path = path
        self.__colcount = None
        self.__sheetname = sheet
        # Map { ColName -> ColAttribute }:
        self.__attributes = SortedDict()
        self.__error = ''
        self.__GetFileDate(fileDateFormat)
        self.__ParseFile()

    def __iter__(self):
        pass

    def __eq__(self, attributes):
        for attr in attributes.Attributes:
            if attr not in self.Attributes:
                return False
            if attributes.Attributes[attr] != self.Attributes[attr]:
                return False
        return True
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
    def ToFileRow(self, horiz = True):
        """
        * Express as string for output to file.
        Inputs:
        * horiz: Put True if want to print horizontally. Else will be
        vertical.
        """
        row = []
        for attr in self.__attributes:
            if horiz:
                row.append(attr.ToRow())
            else:
                row.append([attr.ToRow()])
        return row

    ######################
    # Private Helpers:
    ######################
    def __ParseFile(self):
        """
        * Parse column all column attributes in DataFrame.
        """
        try:
            data = self.__GetFileData()
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

    def __GetFileData(self):
        if '.csv' in self.__path:
            return pandas.read_csv(self.__path)
        elif '.xls' in self.__path:
            return pandas.read_excel(self.__path, sheet_name = (0 if self.__sheetname is None else self.__sheetname))