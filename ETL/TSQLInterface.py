#####################################
# TSQLInterface.py
#####################################
# Description:
# * Perform SELECT, INSERT, UPDATE, etc queries on TSQL server 
# databases.

import numpy as np
from pandas import DataFrame, isnull, notnull, read_sql
import pyodbc
from sqlalchemy import create_engine

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
        self.__connectString = None
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
        try:
            if any(['localhost' in server, '.' in server, '(localdb)' in server]):
                connect_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=.;DATABASE=%s;Trusted_Connection=yes" % database
                self.__connection = pyodbc.connect(connect_string)
                self.__server = '.'
            else:
                connect_string = TSQLInterface.__connectString % (server, database)
                self.__connection = pyodbc.connect(connect_string)
                self.__server = server
        except Exception as ex:
            errs = ['Failed to connect to %s::%s' % (self.__server, self.__database)]
            code = ex.args[0].strip('[').strip(']')
            reason = ex.args[1].split(']')[4]
            reason = reason[0:reason.rfind('.') + 1]
            errs.append('Code: %s' % code)
            errs.append('Reason: %s' % reason)
            raise Exception('\n'.join(errs))
        self.__database = database
        self.__connectString = connect_string

    def Select(self, query):
        """
        * Return dataframe containing requested data.
        """
        return read_sql(query, self.__connection)

    def Insert(self, data, table, identity_insert = False):
        """
        * Insert data into table on current connection.
        Inputs:
        * data: DataFrame with appropriate columns mapped in target table containing data.
        * table: Table name string that exists in connected database.
        Optional:
        * identity_insert: Put True if data contains an ID column in target table.
        """
        errs = []
        if not isinstance(data, DataFrame):
            errs.append('data must be a DataFrame.')
        if not isinstance(table, str):
            errs.append('table must be a string.')
        if not isinstance(identity_insert, bool):
            errs.append('identity_insert must be a boolean.')
        if not self.__IsConnected():
            errs.append('Need to call Connect() before calling this function.')
        if errs:
            raise Exception('\n'.join(errs))

        # Convert datetimes to proper type before insertion:
        data = TSQLInterface.__CleanData(data)
        table = TSQLInterface.__WrapName(table)
        cols = ','.join(['[' + col + ']' for col in data.columns])
        values = ','.join(['?' for col in data.columns])
        insert_query = 'INSERT INTO %s (%s) VALUES (%s)' % (table,cols,values)
        cursor = self.__connection.cursor()
        if identity_insert:
            cursor.execute('SET IDENTITY_INSERT %s ON' % table)
        cursor.fast_executemany = False
        cursor.executemany(insert_query, list(data.itertuples(index=False,name=None)))
        cursor.commit()

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
        self.__connectString = None
    
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
    def __PandasInsert(self, table, data):
        """
        * Insert using Pandas Dataframe and Sqlalchemy engine. 
        """
        table = TSQLInterface.__WrapName(self.__database) + '.' + TSQLInterface.__WrapName(table)
        engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % self.__connectString)
        data.to_sql(name=table, con=engine)
    
    @classmethod
    def __CleanData(cls, data):
        """
        * Convert NaTs to None, datetime2s to datetime, to properly handle
        datetime insertion.
        """        
        #data = data.where(notnull(data), other = None)
#        for col in data.columns:
#            if data[col].dtype.type == np.datetime64:
#                data[col] = [val if not val == np.datetime64('NaT') else None for val in data[col]]
        return data.fillna('')

    @classmethod
    def __WrapName(cls, name):
        """
        * Wrap names in brackets.
        """
        if not name.startswith('['):
            name = '[' + name
        if not name.endswith(']'):
            name += ']'
        return name