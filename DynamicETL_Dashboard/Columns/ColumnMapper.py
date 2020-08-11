#####################################
# ColumnMapper.py
#####################################
# Description:
# * Static class that maps input columns to target columns in
# T-SQL table based upon DynamicETL.Service 
# appsettings.json config, as well as perform conversions
# to types specified.

from Columns.ColumnConverter import ColumnConverter
from pandas import Dataframe

class ColumnMapper:
    """
    * Static class that maps input columns to target columns in
    T-SQL table based upon DynamicETL.Service 
    appsettings.json config, as well as perform conversions
    to types specified.
    """
    @staticmethod
    def MapColumns(testdata, etlname, serviceappsettings):
        """
        * Generate dictionary mapping columns to appropriate
        columns in table.
        Inputs:
        * testdata: pandas Dataframe containing data with columns
        to be mapped before comparing with table dataset.
        * etlname: string name of etl. Must be present in the 
        serviceappsettings json dictionary.
        * serviceappsettings: json dictionary containing
        DynamicETL.Service configuration.
        Output:
        * Returns testdata DataFrame with mapped columns.
        """
        errs = []
        if not isinstance(testdata, DataFrame):
            errs.append('testdata must be a pandas DataFrame.')
        if not isinstance(etlname, str):
            errs.append('etlname must be a string.')
        if not isinstance(serviceappsettings, dict):
            errs.append('serviceappsettings must be a json dictionary.')
        elif not "Etls" in serviceappsettings:
            errs.append('serviceappsettings is missing the "Etls" attribute.')
        elif isinstance(etlname, str) and not etlname in serviceappsettings['Etls']:
            errs.append('etlname is not configured in the "Etls" attribute in the serviceappsettings json file.')
        if errs:
            raise Exception('\n'.join(errs))
        
        etlConfig = serviceappsettings['Etls'][etlname]
        if not "FieldOverride" in etlConfig:
            return testdata
        override = etlConfig["FieldOverride"]
        if "Fields" not in override:
            return testdata
        colMaps = override["Fields"]
        for colMap in colMaps:
            # Skip if not properly configured:
            if not 'From' in ColMap or not 'To' in colMap:
                continue
            testdata = testdata.rename(columns = {colMap['From'] : colMap['To']})
            # Convert data type using converter if configured:
            if "Converter" in colMap:
                pass

        return testdata

