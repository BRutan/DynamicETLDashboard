#####################################
# ColumnAttributes.py
#####################################
# Description:
# *

from datetime import datetime
import pandas
from pandas import DataFrame
from sortedcontainers import SortedDict
from Columns.ColumnAttribute import ColumnAttribute
from Columns.ColumnRelationships import ColumnRelationships

class ColumnAttributes(object):
    """
    * Container of multiple ColAttributes for single entity.
    """
    def __init__(self, path, fileDateFormat, key = None):
        """
        * Instantiate new object.
        Optional Inputs:
        * key: Any type that can be used to demarcate groups of column attributes (ex: by a file date).
        """
        self.__key = key
        self.__path = path
        self.__GetFileDate(path, fileDateFormat)
        # Map { ColName -> ColAttribute }:
        self.__attributes = SortedDict()
        self.__error = ''

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
    @RowCount.setter
    def RowCount(self, val):
        self.__rowCount = val

    ######################
    # Interface Methods:
    ######################
    def ParseFile(self, path):
        """
        * Parse column all column attributes in DataFrame.
        Inputs:
        * data: Expecting a DataFrame object.
        """
        try:
            data = self.__GetFileData(path)
        except BaseException as ex:
            self.__error = str(ex)
            return

        # Store attributes of each column:
        self.ParseHeaderRow(data)
        for col in data.columns:
            self.__attributes[col].ParseColumn(data[col])
        # Map all relationships between columns:
        self.__relationships = ColumnRelationships(data)

    def ParseHeaderRow(self, data):
        """
        * Parse header row in file.
        """
        self.__attributes = { col : ColumnAttribute(col) for col in data.columns }
        # Map column name to index and back:
        self.__colNumToCol = { num : col for num, col in enumerate(data.columns) }
        self.__colToColNum = { self.__colNumToCol[num] : num for num in self.__colNumToCol }

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

    def __GetFileDate(self, path, format):
        format = { arg.lower() : format[arg] for arg in format }
        match = format['regex'].search(path)[0]
        self.__fileDate = datetime.strptime(match, format['dateformat'])

    def __GetFileData(self, path):
        if '.csv' in path:
            return pandas.read_csv(path)
        elif '.xls' in path:
            return pandas.read_excel(path)