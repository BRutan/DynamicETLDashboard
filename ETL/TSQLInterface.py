#####################################
# TSQLInterface.py
#####################################
# Description:
# * Perform SELECT, INSERT, UPDATE, etc queries on TSQL server 
# databases.

from pandas import DataFrame, read_sql
import pyodbc
import sqlalchemy

class TSQLInterface:
    """
    * Perform SELECT, INSERT, UPDATE, etc queries on TSQL server
    databases.
    """
    __connectString = 'Driver={SQL Server};Server=%s;Database=%s;Trusted_Connection=yes;'
    def __init__(self, server, database):
        """
        * Connect to server and target database.
        """
        self.__Validate(server, database)
        self.__server = None
        self.__database = None
        self.__connection = None
        self.__cursor = None
        self.Connect(server, database)

    def __del__(self):
        """
        * Close connection if opened.
        """
        self.__CloseConnection()
    ######################
    # Interface Methods:
    ######################
    def Connect(self, server, database):
        """
        * Attempt to connect to target server. Must be
        run before executing queries.
        Inputs:
        * server: Target server to connect to.
        * database: Database schema on server.
        """
        self.__Validate(server, database)
        self.__CloseConnection()
        self.__server = server
        self.__database = database
        try:
            self.__connection = pyodbc.connect(TSQLInterface.__connectString % (server, database))    
        except Exception as ex:
            errs = ['Failed to connect to %s::%s' % (self.__server, self.__database)]
            code = ex.args[0].strip('[').strip(']')
            reason = ex.args[1].split(']')[4]
            reason = reason[0:reason.rfind('.') + 1]
            errs.append('Code: %s' % code)
            errs.append('Reason: %s' % reason)
            raise Exception('\n'.join(errs))

    def Select(self, query):
        """
        * Return dataframe containing requested data.
        """
        return read_sql(query, self.__connection)

    def Insert(self, data, table):
        """
        * Insert data into table on current connection.
        """
        errs = []
        if not isinstance(data, DataFrame):
            errs.append('data must be a DataFrame.')
        if not isinstance(table, str):
            errs.append('table must be a string.')
        if errs:
            raise Exception('\n'.join(errs))

        data.to_sql(table, self.__connection)

    ######################
    # Private helpers:
    ######################
    def __CloseConnection(self):
        """
        * Close connection if opened.
        """
        if not self.__cursor is None:
            self.__cursor.close()
            self.__cursor = None
        if not self.__connection is None:
            self.__connection.close()
            self.__connection = None
    def __IsConnected(self):
        """
        * Determine if connected to instance.
        """
        return not self.__connection is None
    def __Validate(self, server, database):
        """
        * Validate construction parameters.
        """
        errs = []
        if not isinstance(server, str):
            errs.append('server must be a string.')
        if not isinstance(database, str):
            errs.append('database must be a string.')
        if errs:
            raise Exception('\n'.join(errs))
        