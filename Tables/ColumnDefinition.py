#####################################
# ColumnDefinition.py
#####################################
# Description:
# * Abstract base class for column
# definitions.

from abc import ABC, abstractmethod
import copy

class ColumnDefinition(ABC):
    """
    * Abstract base class for column
    definitions.
    """
    def __init__(self, name, attributes, type):
        """
        * Set base class member variables.
        Inputs:
        * name: string name of column.
        * attributes: dictionary mapping { Attribute -> Value } based upon
        derived column-type language.
        * type: string column type, based upon derived column-type language.
        """
        ColumnDefinition.__Validate(name, attributes, type)
        self.__columnname = name
        self.__attributes = attributes
        self.__type = type

    ###############
    # Properties:
    ###############
    @property
    def Attributes(self):
        return copy.deepcopy(self.__attributes)
    @property
    def ColumnName(self):
        return self.__columnname
    @property
    def Type(self):
        return self.__type
    ###############
    # Interface Methods:
    ###############
    @abstractmethod
    def ToTableDefinitionString(self):
        """
        * Return string line that can
        be used in a table definition 
        script.
        """
        pass
    @abstractmethod
    def ReadTableDefinitionString(self, tableDefString):
        """
        * Convert passed string located in 
        table definition script into a
        derived ColumnDefinition object.
        """
        pass
    ###############
    # Private Helpers:
    ###############
    @staticmethod
    def __Validate(name, attributes, type):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(name, str):
            errs.append('name must be a string.')
        if not isinstance(attributes, dict):
            errs.append('attributes must be a dictionary.')
        if not isinstance(type, str):
            errs.append('type must be a string.')
        if errs:
            raise Exception('\n'.join(errs))