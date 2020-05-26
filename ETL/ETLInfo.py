#####################################
# ETLInfo.py
#####################################
# Description:
# * Aggregates all useful information about
# an ETL (tablename, server, file locations).

class ETLInfo:
    """
    * Immutable object with aggregated information 
    regarding ETLs present in various appsettings files.
    """
    def __init__(self, etlname, paths):
        """
        * Collect all information regarding passed etl.
        Inputs:
        * etlname: String name of ETL want information about.
        * paths: Dictionary with following keys:
        - 
        """
        ETLInfo.__Validate(etlname, paths)



    ###################
    # Properties:
    ###################
    @property
    def InputFolders(self):
        return self.__inputfolders
    @property
    def ServerName(self):
        return self.__servername
    @property
    def TableName(self):
        return self.__tablename
    ###################
    # Private Helpers:
    ###################
    def __CollectInfo(self, etlname, paths):
        """
        * Collect all information regarding etlname
        using appsettings files indicated in paths.
        """
        self.__inputfolders = None
        self.__servername = None
        self.__tablename = None

    @staticmethod
    def __Validate(etlname, paths):
        """
        * Validate constructor arguments.
        """
        errs = []
        if not isinstance(etlname, str):
            errs.append('etlname must be a string.')
        if not isinstance(paths, dict):
            errs.append('paths must be a dictionary.')
        if errs:
            raise Exception('\n'.join(errs))




