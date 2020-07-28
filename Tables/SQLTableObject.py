#####################################
# SQLTableObject.py
#####################################
# Definition:
# * Table object that uses common
# SQL functionality and attributes.

from abc import ABC, abstractmethod
from Tables.TableObject import TableObject

class SQLTableObject(TableObject):
    """
    * Table object that uses common
    SQL functionality and attributes.
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
    