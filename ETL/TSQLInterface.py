#####################################
# TSQLInterface.py
#####################################
# Description:
# * Perform SELECT, INSERT, UPDATE, etc queries on TSQL server 
# databases.

from itertools import combinations
from numba import jit
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
        if not isinstance(query, str):
            raise Exception('query must be a string.')
        elif not 'select' in query.lower():
            raise Exception('query must be a SELECT statement.')
        return read_sql(query, self.__connection)

    def Execute(self, query, returnsvals = False):
        """
        * Execute non-SELECT/INSERT query.
        Inputs:
        * query: string SQL query.
        Optional:
        * returnsvals: Put True if expecting query to return
        values.
        """
        errs = []
        if not isinstance(query, str):
            errs.append('query must be a string.')
        elif 'select' in query.lower() or 'insert' in query.lower():
            errs.append('Use Insert/Select if making an insert/select query.')
        if not isinstance(returnsvals, bool):
            errs.append('returnsvals must be a boolean.')
        if errs:
            raise Exception('\n'.join(errs))
        if not returnsvals:
            cursor = self.__connection.cursor()
            cursor.execute(query)
            cursor.commit()
        else:
            return read_sql(query, self.__connection)

    def ExecuteStoredProcedure(self, procname, params = None, returnsvals = False):
        """
        * Execute stored procedure with or without parameters.
        Inputs:
        * procname: String procedure to execute.
        Optional:
        * params: Parameters to use in procedure.
        * returnsvals: Put True if should return values.
        """
        errs = []
        if not isinstance(procname, str):
            errs.append('procname must be a string.')
        if not params is None and not hasattr(params, '__iter__'):
            errs.append('params must be an iterable if provided.')
        elif not params is None:
            params = [str(param) for param in params]
        if not isinstance(returnsvals, bool):
            errs.append('returnsvals must be a boolean.')
        if errs:
            raise Exception('\n'.join(errs))
        query = "EXEC %s" % procname
        if not params is None:
            query += "'%s'" % ','.join(params)
        if not returnsvals:
            cursor = self.__connection.cursor()
            cursor.execute(query)
            cursor.commit()
        else:
            return read_sql(query, self.__connection)

    def GetColumnAttributes(self, tablename):
        """
        * Return set of column names and attributes that
        belong to table in current Database as a DataFrame.
        Inputs:
        * tablename: string table in connection.
        """
        if not isinstance(tablename, str):
            raise Exception('tablename must be a string.')
        results = self.GetAllTableAttributes()
        if not results:
            None
        # Filter out so only attributes for table are present:
        return results.loc[results['TableName'] == tablename]

    def GetAllTableAttributes(self, schema = None):
        """
        * Get all attributes for all tables at all
        schemas, or given schema, at current server connection.
        Optional:
        * schema: string name of schema at current server.
        """
        if not schema is None and not isinstance(schema, str):
            raise Exception('schema must be a string if provided.')
        query = ['SELECT t.name AS [TableName], t.object_id, t.principal_id,']
        query.append('t.schema_id, t.parent_object_id, t.type, t.type_desc, t.create_date,')
        query.append('t.modify_date, t.is_ms_shipped, t.is_published, t.is_schema_published, t.lob_data_space_id,')
        query.append('t.filestream_data_space_id, t.max_column_id_used, t.lock_on_bulk_load, t.uses_ansi_nulls, ')
        query.append('t.is_replicated, t.has_replication_filter, t.is_merge_published, t.is_sync_tran_subscribed, t.has_unchecked_assembly_data,')
        query.append('t.text_in_row_limit, t.large_value_types_out_of_row, t.is_tracked_by_cdc, t.lock_escalation, t.lock_escalation_desc,')
        query.append('t.is_filetable, t.is_memory_optimized, t.durability, t.durability_desc, t.temporal_type, t.temporal_type_desc,')
        query.append('t.history_table_id, t.is_remote_data_archive_enabled, t.is_external, s.name as [SchemaName],')
        query.append('s.schema_id, s.principal_id, f.is_hidden as [SchemaIsHidden], f.column_ordinal, f.name as [ColumnName], f.is_nullable as [ColumnIsNullable],')
        query.append('f.system_type_id, f.system_type_name as [ColumnType], f.max_length as [ColumnMaxLength],')
        query.append('f.precision as [ColumnPrecision], f.scale as [ColumnScale], f.collation_name as [ColumnCollationName],')
        query.append('f.user_type_id, f.user_type_database, f.user_type_schema, f.user_type_name, ')
        query.append('f.assembly_qualified_type_name, f.xml_collection_id, f.xml_collection_database,')
        query.append('f.xml_collection_schema, f.xml_collection_name, f.is_xml_document,')
        query.append('f.is_case_sensitive, f.is_fixed_length_clr_type, f.source_server,')
        query.append('f.source_database, f.source_schema, f.source_table, f.source_column,')
        query.append('f.is_identity_column, f.is_part_of_unique_key, f.is_updateable,')
        query.append('f.is_computed_column, f.is_sparse_column_set, f.ordinal_in_order_by_list,')
        query.append('f.order_by_is_descending, f.order_by_list_length, f.error_number,')
        query.append('f.error_severity, f.error_state, f.error_message, f.error_type, f.error_type_desc')
        query.append('FROM sys.tables AS t INNER JOIN sys.schemas AS s')
        query.append('ON t.[schema_id] = s.[schema_id] CROSS APPLY sys.dm_exec_describe_first_result_set')
        query.append("(N'SELECT * FROM ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name), N'', 0) AS f")
        if not schema is None:
            query.append("WHERE s.name = '%s'" % TSQLInterface.__WrapName(schema))
        query = ' '.join(query)
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
        cols = ','.join([TSQLInterface.__WrapName(col) for col in data.columns])
        values = ','.join(['?' for col in data.columns])
        insert_query = 'INSERT INTO %s (%s) VALUES (%s)' % (table,cols,values)
        cursor = self.__connection.cursor()
        if identity_insert:
            cursor.execute('SET IDENTITY_INSERT %s ON' % table)
        cursor.fast_executemany = False
        cursor.executemany(insert_query, list(data.itertuples(index=False,name=None)))
        cursor.commit()

    @classmethod
    def PrimaryKeys(cls, data, maxCombs = None, ignoreCols = None, findFirst = False):
        """
        * Find some combination of columns that can work as a primary key
        in passed dataset.
        Inputs:
        * data: DataFrame containing data to find primary key for. 
        Will return None if no combination works.
        Optional:
        * maxCombs: Maximum number of columns to combine.
        * ignoreCols: Iterable containing string columns to ignore.
        * findFirst: Return first primary key, else will return all possible 
        primary keys.
        """
        errs = []
        if not isinstance(data, DataFrame):
            errs.append('data must be a DataFrame.')
        if not maxCombs is None and not isinstance(maxCombs, (int, float)):
            errs.append('maxCombs must be numeric.')
        elif not maxCombs is None and maxCombs < 1:
            errs.append('maxCombs must be >= 1.')
        if not ignoreCols is None and not hasattr(ignoreCols, '__iter__'):
            errs.append('ignoreCols must be an iterable of strings if provided.')
        elif not ignoreCols is None and any([not isinstance(col, str) for col in ignoreCols]):
            errs.append('ignoreCols must only contain strings.')
        if not isinstance(findFirst, bool):
            errs.append('findFirst must be a boolean.')
        if errs:
            raise Exception('\n'.join(errs))
        checkCols = set(data.columns) - set(ignoreCols if not ignoreCols is None else [])
        # Replace nas with blanks:
        data = data[checkCols].replace(np.nan, '', regex=True)
        maxCombs = (len(checkCols) if maxCombs is None else maxCombs) + 1
        maxCombs = min(len(checkCols), maxCombs) + 1
        pKeys = []
        for numCols in range(1, maxCombs):
            allcombos = combinations(checkCols, numCols)
            for comb in allcombos:
                if TSQLInterface.__CheckComb(comb, pKeys):
                    cols = list(comb)
                    if len(set(data[cols].itertuples(index=False,name=None))) == len(data):
                        if findFirst:
                            return set(cols)
                        pKeys.append(set(cols))
        return pKeys

    @staticmethod
    @jit(nopython=True)
    def PrimaryKeysNumba(data, comboSets, findFirst = False):
        """
        * Use Numba to find some combination of columns that can work as a primary key
        in passed dataset.
        Inputs:
        * data: Numpy array containing data to find primary key for. 
        Will return None if no combination works.
        * comboSets: List of list of all possible combinations of target columns, for target choice
        sizes.
        Optional:
        * findFirst: Return first primary key, else will return all possible 
        primary keys.
        """
        errs = []
        if not isinstance(data, np.array):
            errs.append('data must be a numpy array.')
        if not maxCombs is None and not isinstance(maxCombs, (int, float)):
            errs.append('maxCombs must be numeric.')
        elif not maxCombs is None and maxCombs < 1:
            errs.append('maxCombs must be >= 1.')
        if not ignoreCols is None and not hasattr(ignoreCols, '__iter__'):
            errs.append('ignoreCols must be an iterable of strings if provided.')
        elif not ignoreCols is None and any([not isinstance(col, str) for col in ignoreCols]):
            errs.append('ignoreCols must only contain strings.')
        if not isinstance(findFirst, bool):
            errs.append('findFirst must be a boolean.')
        if errs:
            raise Exception('\n'.join(errs))
        checkCols = set(data.columns) - set(ignoreCols if not ignoreCols is None else [])
        data = data[checkCols]
        maxCombs = (len(checkCols) if maxCombs is None else maxCombs) + 1
        maxCombs = min(len(checkCols), maxCombs) + 1
        pKeys = []
        for combSet in comboSets:
            for comb in combSet:
                if __CheckCombNumba(comb, pKeys):
                    cols = list(comb)
                    if len(set(data[cols].itertuples(index=False,name=None))) == len(data):
                        if findFirst:
                            return set(cols)
                        pKeys.append(set(cols))
        return pKeys

    ######################
    # Private helpers:
    ######################
    @classmethod
    def __CheckComb(cls, comb, pKeys):
        """
        * Determine if any subset of combinations is present in passed pkeys,
        since any combination that includes a pkey will also be a pkey.
        """
        if not pKeys:
            return True
        comb = set(comb)
        for key in pKeys:
            if len(key - comb) == 0:
                return False
        return True

    @staticmethod
    @jit(nopython=True)
    def __CheckCombNumba(cls, comb, pKeys):
        """
        * Determine if any subset of combinations is present in passed pkeys,
        since any combination that includes a pkey will also be a pkey.
        """
        if not pKeys:
            return True
        comb = set(comb)
        for key in pKeys:
            if len(key - comb) == 0:
                return False
        return True

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