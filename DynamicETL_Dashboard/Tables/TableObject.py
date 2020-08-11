#####################################
# TableObject.py
#####################################
# Description:
# * Abstract base class for derived
# table objects that can be used
# in various SQL or database languages.

from abc import ABC, abstractmethod
import copy

class TableObject(ABC):
    """
    * Abstract base class for derived table objects 
    that can be used in various SQL or database languages.
    """
    def __init__(self, name):
        """
        * Initialize base class members.
        """
        self.__columns = {}
        self.__tablename = name

    ##################
    # Properties:
    ##################
    @property
    def Columns(self):
        """
        * Maps { ColumnName -> ColumnDefinitionObj }.
        """
        return copy.deepcopy(self.__columns)
    @property
    def TableName(self):
        return self.__tablename
    ##################
    # Interface Methods:
    ##################
    @abstractmethod
    def GenerateTableDef(self, path):
        """
        * Generate table definition at path.
        """
        pass
    @abstractmethod
    def ReadDefinitionFile(self, path):
        """
        * Convert table definition file
        into TableObject.
        """
        pass
    @abstractmethod
    def TableDifference(self, table, path):
        """
        * Generate a column update script 
        at path based upon difference between
        two tables.
        """
        pass