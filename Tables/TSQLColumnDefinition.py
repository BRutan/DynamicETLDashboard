#####################################
# TSQLColumnDefinition.py
#####################################
# Description:
# * Column definition appropriate
# for use in T-SQL language.

from pandas import DataFrame, read_csv

from Tables.ColumnDefinition import ColumnDefinition

class TSQLColumnDefinition(ColumnDefinition):
    """
    * Column definition appropriate
    for use in T-SQL language.
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
    ###############
    # Private Helpers:
    ###############

