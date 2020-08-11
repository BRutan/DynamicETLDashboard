#####################################
# InsertProcedureGenerator.py
#####################################
# Description:
# * Generate data insertion stored procedures 
# for use with ASP.Net WebApis to directly insert data
# into target tables.

import os
from Tables.TSQLInterface import TSQLInterface

class InsertProcedureGenerator:
    """
    * Generate data insertion stored procedures 
    for use with ASP.Net WebApis to directly insert data
    into target tables.
    """
    def __init__(self, server, database):
        """
        * Connect to server and database.
        Inputs:
        * server: T-SQL server to connect to. Must be a string.
        * database: Database on server to connect to. Must be a string.
        """
        self.ChangeConnection(server, database)

    ################
    # Properties:
    ################
    @property
    def Database(self):
        """
        * Current connected database.
        """
        return self.__database
    @property
    def Server(self):
        """
        * Current connected server.
        """
        return self.__server
    ################
    # Interface Methods:
    ################
    def ChangeConnection(self, server, database):
        """
        * Connect to different server::database.
        Inputs:
        * server: T-SQL server to connect to. Must be a string.
        * database: Database on server to connect to. Must be a string.
        """
        connect = InsertProcedureGenerator.__Validate(server, database)
        self.__SetProperties(connect, server, database)
        
    def Generate(self, tablename, procname, outpath):
        """
        * Generate insert procedure for tablename at 
        outpath.
        Inputs:
        * tablename: table to generate insertion
        procedure for. Must be a string.
        * procname: Name for stored procedure. Must be a string.
        * outpath: Full path to .sql file to store
        insertion procedure. Must be a string.
        """
        errs = []
        if not isinstance(tablename, str):
            errs.append('tablename must be a string.')
        if not isinstance(procname, str):
            errs.append('procname must be a string.')
        if not isinstance(outpath, str):
            errs.append('outpath must be a string.')
        elif not outpath.endswith('.sql'):
            errs.append('outpath must point to .sql file.')
        else:
            folder, file = os.path.split(outpath)
            if not os.path.isdir(folder):
                errs.append('folder %s does not exist.' % folder)
        if errs:
            raise Exception('\n'.join(errs))
        # Generate insertion procedure:
        try:
            attrs = self.__connection.GetColumnAttributes(tablename)
            if attrs is None:
                raise Exception('Table does not exist.')
        except Exception as ex:
            tup = (self.__server, self.__database, tablename, str(ex))
            raise Exception('Could not connect to %s::%s::%s. Reason: %s.' % tup)
        InsertProcedureGenerator.__GenerateScript(attrs, tablename, procname, self.__database, outpath)

    ################
    # Private Helpers:
    ################
    @staticmethod
    def __GenerateScript(attrs, tablename, procname, db, outpath):
        """
        * Generate insert procedure .sql script at
        outpath.
        """
        # Get columns and type strings:
        vars = []
        cols = []
        varTypes = []
        for num, col in enumerate(attrs['ColumnName']):
            colType = attrs['ColumnType'].iloc[num]
            varName = col.replace(' ','')
            if not num == len(attrs) - 1:
                cols.append('%s,' % TSQLInterface.WrapName(col))
                vars.append('@%s,' % varName)
                varTypes.append('@%s %s,' % (varName, colType))
            else:
                cols.append(TSQLInterface.WrapName(col))
                vars.append('@%s' % varName)
                varTypes.append('@%s %s' % (varName, colType))
        # Generate all lines:
        lines = ['USE %s;' % TSQLInterface.WrapName(db)]
        lines.append('GO\n')
        lines.append('CREATE PROCEDURE [dbo].%s (' % TSQLInterface.WrapName(procname))
        lines.extend(varTypes)
        lines.append(') AS BEGIN')
        lines.append('INSERT INTO [dbo].%s(' % TSQLInterface.WrapName(tablename))
        lines.extend(cols)
        lines.append(')')
        lines.append('VALUES (')
        lines.extend(vars)
        lines.append(') END; \n GO')
        try:
            with open(outpath, 'w') as f:
                f.write('\n'.join(lines))
        except Exception as ex:
            folder, filename = os.path.split(outpath)
            raise Exception('Could not write %s. Reason: %s.' % (filename, str(ex)))

    @staticmethod
    def __Validate(server, database):
        """
        * Validate constructor parameters, attempt
        to connect to server.
        """
        errs = []
        if not isinstance(server, str):
            errs.append('server must be a string.')
        if not isinstance(database, str):
            errs.append('database must be a string.')
        try:
            if not errs:
                connection = TSQLInterface(server, database)
        except Exception as ex:
            errs.append('Failed to connect to %s::%s. Reason: %s' % (server, database, str(ex)))
        if errs:
            raise Exception('\n'.join(errs))
        return connection

    def __SetProperties(self, connect, server, database):
        """
        * Set object properties from constructor parameters.
        """
        self.__connection = connect
        self.__database = database
        self.__server = server