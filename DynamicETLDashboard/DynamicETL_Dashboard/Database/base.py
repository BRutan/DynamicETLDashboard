#####################################
# Database/base.py
#####################################
# Description:
# * Abstract base class for database related
# objects.

from abc import ABC, abstractmethod
import copy

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
    def __init__(self, name, database, server):
        """
        * Table object that uses common
        SQL functionality and attributes.
        """
        SQLTableObject.__Validate(database, server)
        self.__NormalizeAndSetMembers(database, server)
        super(self).__init__(name)

    def __sub__(self, sqlTable):
        """
        * Subtraction operator overload. 
        Generate ALTER COLUMN script based upon 
        difference between two tables.
        Inputs:
        * sqlTable: SQLTableObject to compare this table
        against. Must have same name and belong to same database
        as this table.
        """
        if not isinstance(sqlTable, SQLTableObject):
            raise Exception('subtraction operator not supported with %s.' % str(type(sqlTable)))
        elif not (self.Name == sqlTable.Name and self.Database == sqlTable.Database and self.Server == sqlTable.Server):
            raise Exception('sqlTable must have same Name and belong to same Database.')
        pass

    ###############
    # Properties:
    ###############
    @property
    def Database(self):
        return self.__database
    @property
    def Server(self):
        return self.__server
    ###############
    # Private Helpers:
    ###############
    def __NormalizeAndSetMembers(self, database, server):
        """
        * Normalize members and store state in object.
        I.E. put all members in lowercase since SQL
        in general is case sensitive for database object
        names.
        """
        self.__database = database.lower()
        self.__server = server.lower()
    @staticmethod
    def __Validate(database, server):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(database, str):
            errs.append('database must be a string.')
        if not isinstance(server, str):
            errs.append('server must be a string.')
        if errs:
            raise Exception('\n'.join(errs))

# Alt:
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
    