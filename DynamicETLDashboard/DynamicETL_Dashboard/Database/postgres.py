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

    def GetAllTableAttributes(self, schema = None):
        """
        * Return table attributes for schema.
        """
        pass

