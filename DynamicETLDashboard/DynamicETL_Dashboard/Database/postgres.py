#####################################
# Database/postgres.py
#####################################
# Description:
# * Objects related to interacting
# with SQL database that uses PostGres
# as SQL flavor language.

from Database.base import DBInterface
import psycopg2

class PostGresInterface(DBInterface):
    """
    * Interface into PostGRES SQL database.
    """
    def __init__(self, dbname, user, password):
        """
        * Initiate new connection.
        """
        connect = psycopg2.connect('dbname=%s user=%s' % (dbname, user, password))
        super().__init__(connect)

    ##############
    # Properties:
    ##############

    ##############
    # Interface Methods:
    ##############
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
        if not isinstance(returnsvals, bool):
            errs.append('returnsvals must be a boolean.')
        if errs:
            raise ValueError(','.join(errs))
        try:
            if returnsvals:
                return super().Cursor.execute(query)
            else:
                super().Cursor.execute(query)
        except Exception as ex:
            raise RuntimeError('Failed to execute. Reason: %s' % str(ex))

    def GetAllTableAttributes(self, schema = None, table = None):
        """
        * Return table attributes for schema.
        Optional:
        * schema: Name of specific schema (string).
        * table: Name of specific table (string). 
        """
        if not schema is None and not isinstance(schema, str):
            raise ValueError('schema must be a string if provided.')
        query = ['SELECT * FROM information_schema.columns']
        opt = []
        if not schema is None:
            opt.append('schema = %s' % schema)
        if not table is None:
            opt.append('table = %s' % table)
        if opt:
            query.extend(opt)
        try:
            results = self.Execute(' '.join(query), True)
        except Exception as ex:
            pass
        # Return as derived TableObject:
        out = PostGresTable(table, '', schema)
        out.GenerateTableDef()
        return out

class PostGresTable(SQLTableObject):
    """
    * Contains column names and meta
    attributes for T-SQL tables.
    """
    def __init__(self, tablename, database, server):
        """
        * Contains column names and meta
        attributes for T-SQL tables.
        """
        super().__init__(tablename, database, server)

    ################
    # Properties:
    ################

    ################
    # Interface Methods:
    ################
    def GenerateTableDef(self, path, template = None):
        """
        * Generate table definition at path, using
        template if necessary.
        Inputs:
        * path: string path to .sql file to
        store table definition script.
        Optional:
        * template: 
        """
        pass
    
    def TableDifference(self, table, path):
        """
        * Generate a column update script 
        at path.
        Inputs:
        * table: TSQLTable object. Must have same name
        as this table.
        * path: Path to .sql file to output
        alter column script.
        """
        pass
    ################
    # Private Helpers:
    ################


############################
# Table Class:
############################
class PostGresColumnDefinition(ColumnDefinition):
    """
    * Column definition appropriate
    for use in PostGres SQL language.
    """
    def __init__(self, **kwargs):
        """
        * Column definition appropriate
        for use in T-SQL language.
        """
        super().__init__(kwargs['name'], kwargs['attributes'], kwargs['type'])

    ###############
    # Interface Methods:
    ###############
    @classmethod
    def GenerateTSQLColumnDefinition(columnData, name = None):
        """
        * Create a TSQLColumnDefinition object
        using passed dataset.
        Inputs:
        * columnData: pandas DataFrame or any iterable
        representing column data.
        """
        errs = []
        if not name is None and not isinstance(name, str):
            errs.append('name must be a string.')
        if not hasattr(columnData, '__iter__'):
            errs.append('columnData must be a pandas DataFrame or an iterable.')
        if errs:
            raise Exception('\n'.join(errs))

        if name is None:
            if isinstance(columnData, DataFrame):
                # Use name of column from DataFrame:
                pass
            else:
                # Use default column name if not supplied:
                name = "Column"
        colName = name
        attributes = {}
        return TSQLColumnDefinition(name = colName, attributes = attributes, type = type)

    def ToTableDefinitionString(self):
        """
        * Return string line that can be used 
        in a TSQL table definition script.
        """
        pass

    def ReadTableDefinitionString(self, tableDefString):
        """
        * Convert passed string located in 
        table definition script into a
        derived ColumnDefinition object.
        Inputs:
        * tableDefString: Passed string that must 
        match pattern "[ColumnName] [Type] [[MetaAttributes],...]".
        """
        if not isinstance(tableDefString, str):
            raise Exception('tableDefString must be a string.')

    ###############
    # Private Helpers:
    ###############


