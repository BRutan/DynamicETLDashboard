#####################################
# Database/postgres.py
#####################################
# Description:
# * Objects related to interacting
# with SQL database that uses PostGres
# as SQL flavor language.

from Database.base import DBInterface, TableObject
from Database.columnattributes import ColumnAttributesGenerator
from pandas import read_csv, DataFrame, Series
import psycopg2
import os

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

class PostGresTable(TableObject):
    """
    * Contains column names and meta
    attributes for PostGresSQL tables.
    """
    def __init__(self, name, dbname, server):
        """
        * Contains column names and meta
        attributes for T-SQL tables.
        """
        super().__init__(name, dbname, server, 'postgresql')

    ################
    # Interface Methods:
    ################
    @staticmethod
    def GenerateTableDef(df, tablename, outpath = None, template = None):
        """
        * Convert passed dataframe with dtypes into
        CREATE TABLE script with appropriate types for PostGresSQL.
        Inputs:
        * df: Pandas dataframe.
        * tablename: String name for table.
        Optional:
        * outpath: string path to output table definition.
        * template: path to file to insert table definition. 
        Must have one %s placed to indicate where to place
        table definition.
        """
        errs = []
        if not isinstance(df, DataFrame):
            errs.append('df must be a DataFrame.')
        if not isinstance(tablename, str):
            errs.append('tablename must be a string.')
        if not outpath is None and not isinstance(outpath, str):
            errs.append('outpath must be a string if provided.')
        if not template is None:
            if not isinstance(template, str):
                errs.append('template must be a string path if provided.')
            elif not os.path.exists(template):
                errs.append('template does not exist at path.')
        if errs:
            raise ValueError('\n'.join(errs))
        # Convert types:
        types = PostGresColumnConverter.GetColumnTypes(df)
        attributes = PostGresColumnConverter.GetColumnAttributes(df)
        definition = ['CREATE TABLE %s' % tablename, '(']
        for col in types:
            definition.append('%s %s %s,')
        definition.append(')')
        if not outpath is None:
            with open('%s%s.sql' % (outpath, tablename), 'w') as f:
                f.write('\n'.join(definition))
        else:
            return '\n'.join(definition)

    def ImportTableDef(self, path):
        """
        * Import table definition using
        file.
        """
        pass

    def ReadDefinitionFile(self, path):
        """
        * Convert table definition file
        into TableObject.
        """
        pass
    
    ################
    # Private Helpers:
    ################


############################
# Table Class:
############################
class PostGresColumnConverter(ColumnAttributesGenerator):
    """
    * Map numpy dtypes to postgresql
    types and attributes for usage in creating tables.
    """
    # https://www.postgresql.org/docs/9.5/datatype.html
    __conversionTable = {'i':{'8' : 'bigint', '4':'integer', '2':'smallint'},
                         'u':{'8' : 'bigint', '4':'integer', '2':'smallint'},
                         'b':'bool', 
                         'f':{'8':'double precision','4':'real'}, 
                         'c':'character varying', 
                         'm':'character varying', 
                         'M':'character varying', 
                         'O':'character varying', 
                         'S':'character varying', 
                         'U':'character varying UTF8', 
                         'V':' ',
                         '?':' '}
    @staticmethod
    def GetColumnTypes(df):
        """
        * Get column types appropriate for PostGreSQL from
        numpy dtypes.
        """
        if not isinstance(df, (DataFrame, Series)):
            raise ValueError('df must be a pandas DataFrame or Series.')
        out = {}
        tps = ColumnAttributesGenerator.GetColumnTypes(df)
        if isinstance(df, DataFrame):
            for cell in tps:
                name = cell[0]
                nptp = cell[1]
                if '<' in nptp:
                    nptp = nptp.replace('<', '')
                    prec = nptp[1:]
                    nptp = nptp[0]
                    tp = PostGresColumnConverter.__conversionTable[nptp][prec]
                elif '|' in nptp:
                    nptp = nptp.replace('|', '')
                    PostGresColumnConverter.__conversionTable[nptp]
                else:
                    tp = PostGresColumnConverter.__conversionTable[nptp]
                out[name] = tp
        else:
            name = cell[0]
            nptp = cell[1]
            if '<' in nptp:
                nptp = nptp.replace('<', '')
                prec = nptp[1:]
                nptp = nptp[0]
                tp = PostGresColumnConverter.__conversionTable[nptp][prec]
            else:
                tp = PostGresColumnConverter.__conversionTable[nptp]
            out[name] = tp
        return out

    @staticmethod
    def GetColumnAttributes(df):
        """
        * Get column attributes from dataframe.
        """
        return ColumnAttributesGenerator.GetColumnAttributes(df)


df = read_csv(r'C:\Shared\ETF Global\Projects\indexiq funds and basket dags\csv_output\IndexIQ_processed_constituents.csv')
df = df[[col for col in df.columns if not 'Unnamed: ' in col]]
PostGresTable.GenerateTableDef(df,'indexiq_table',r'C:\Shared\ETF Global\Projects\indexiq funds and basket dags',None)