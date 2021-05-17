#####################################
# Database/base.py
#####################################
# Description:
# * Abstract base class for database related
# objects.

from abc import ABC, abstractmethod
import copy
import numpy as np

############################
# Database Base Class:
############################
class DBInterface(ABC):
    """
    * Abstract base class for database
    related objects.
    """
    def __init__(self, connection):
        """
        * Store connection.
        """
        self.__connection = connection
        self.__cursor = connection.cursor()
        
    ##############
    # Properties:
    ##############
    @property
    def Connection(self):
        return self.__connection
    @property
    def Cursor(self):
        return self.__cursor
    ##############
    # Interface Methods:
    ##############


############################
# Table Base Class:
############################
class TableObject(ABC):
    """
    * Abstract base class for derived table objects 
    that can be used in various SQL or database languages.
    """
    def __init__(self, name, dbname, servername, language):
        """
        * Initialize base class members.
        Inputs:
        * name: String name of table.
        * dbname: String name of database (can be None if not attached to database yet).
        * servername: String name of server (can be None if not attached to server yet).
        * language: SQL language flavor.
        """
        self.__checkinputs(name, dbname, servername, language)
        self.__columns = {}
        self.__tablename = name
        self.__dbname = database
        self.__servername = servername
        self.__language = language

    def __sub__(self, table):
        """
        * Subtraction operator overload. 
        Generate ALTER COLUMN script based upon 
        difference between two tables.
        Inputs:
        *
        """
        if not isinstance(table, TableObject):
            raise Exception('subtraction operator not supported with %s.' % str(type(table)))
        elif not (self.Name == table.Name and self.Database == sqlTable.Database and self.Server == sqlTable.Server):
            raise Exception('')
        pass

    def __eq__(self, table):
        """
        * Determine if all aspects of table are the same.
        Inputs:
        * 
        """
        if not isinstance(table, TableObject):
            raise Exception('subtraction operator not supported with %s.' % str(type(table)))
        
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
    def DatabaseName(self):
        return self.__dbname
    @property
    def Name(self):
        return self.__name
    @property
    def ServerName(self):
        return self.__servername
    @property
    def TableName(self):
        return self.__tablename
    ##################
    # Interface Methods:
    ##################
    @abstractmethod
    def ImportTableDef(self, path):
        """
        * Import table definition using
        file.
        """
        pass
    @abstractmethod
    def ReadDefinitionFile(self, path):
        """
        * Convert table definition file
        into TableObject.
        """
        pass
    ##################
    # Private Helpers:
    ##################
    def __checkinputs(self, name, dbname, servername, language):
        """
        * Ensure constructor inputs are valid.
        """
        errs = []
        if not isinstance(name,str):
            errs.append('name must be a string.')
        if not dbname is None and not isinstance(dbname, str):
            errs.append('dbname must be a string if provided.')
        if not servername is None and not isinstance(servername, str):
            errs.append('servername must be a string.')
        if not isinstance(language, str):
            errs.append('language must be a string.')
        if errs:
            raise ValueError('\n'.join(errs))
