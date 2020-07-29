#####################################
# TSQLTable.py
#####################################
# Description:
# * Contains column names and meta
# attributes for T-SQL tables.

from Tables.SQLTableObject import SQLTableObject

class TSQLTable(SQLTableObject):
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
