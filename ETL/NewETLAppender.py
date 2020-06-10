#####################################
# NewETLAppender.py
#####################################
# Description:
# * Validate new ETL and append to build script for use in DynamicETL.Service.

from ETL.ETLObj import ETLObj
import json
import os
from Utilities.Helpers import LoadJsonFile

class NewETLAppender:
    """
    * Append new ETL to existing DynamicETL.Service appsettings-template.json file.
    """
    def __init__(self, etlname, appsettingsobj, kwargs = None):
        """
        * Append new etl with name to appsettings-template.json
        file.
        Inputs:
        * etlname: String name of new ETL. Cannot be present already in appsettings-template.json
        file.
        * appsettingsobj: Path to .json file containing DynamicETL.Service appsettings-template.json
        attributes, or json dictionary containing appsettings-template.json attributes.
        Optional Inputs:
        * kwargs: Dictionary containing attributes consistent with ETLObj class.
        """
        NewETLAppender.__Validate(etlname, appsettingsobj, kwargs)
        self.__appsettingstemplate = json.load(open(appsettingsobj, 'rb')) if isinstance(appsettingsobj, str) else appsettingsobj
        self.__AppendNewETL(etlname, kwargs)

    ####################
    # Interface Methods:
    ####################
    def AppendNewETL(self, newetlname, kwargs = None):
        """
        * Append additional ETL to appsettings-template file.
        Inputs:
        * newetlname: String name of ETL. Must not be already configured
        in loaded appsettings-template.json file.
        Optional Inputs:
        * kwargs: Dictionary containing attributes to fill
        generated ETL object with.
        """
        errs = []
        if not isinstance(newetlname, str):
            errs.append('newetlname must be a string.')
        elif newetlname in self.__appsettingstemplate['Etls']:
            errs.append('%s ETL already configured in appsettings-template.json file.')
        if not kwargs is None and not isinstance(kwargs, dict):
            errs.append('kwargs must be a dictionary if provided.')
        self.__AppendNewETL(newetlname, kwargs)

    def OutputUpdatedFile(self, path):
        """
        * Output updated appsettings-template.json file 
        to json file at path.
        Inputs;
        * path: String pointing to output .json file.
        """
        if not isinstance(path, str):
            raise Exception('path must be a string.')
        elif not path.endswith('.json'):
            raise Exception('path must point to .json file.')
        json.dump(self.__appsettingstemplate, open(path, 'w'), indent = 4)

    ####################
    # Private Helpers:
    ####################
    def __AppendNewETL(self, etlname, kwargs):
        """
        * Append new etl to appsettings-template.json file.
        """
        kwargs['etlname'] = etlname
        newETL = ETLObj(kwargs)
        self.__appsettingstemplate['Etls'][etlname] = newETL.ToJson()
    
    @staticmethod
    def __Validate(etlname, appsettingsobj, kwargs):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(etlname, str):
            errs.append('etlname must be a string.')
        if not isinstance(appsettingsobj, (str, dict)):
            errs.append('appsettingsobj must be a string or json dictionary.')
        elif isinstance(appsettingsobj, str):
            if not appsettingsobj.endswith('.json'):
                errs.append('appsettingspath must point to a json file.')
            elif not os.path.exists(appsettingsobj):
                errs.append('appsettings-template.json file at path does not exist.')
            else:
                try:
                    vals = json.load(open(appsettingsobj, 'rb'))
                    if not 'Etls' in vals:
                        errs.append('"Etls" attribute missing from appsettings-template.json file.')
                    elif isinstance(etlname, str) and etlname in vals['Etls']:
                        errs.append('An ETL named %s has already been configured in appsettings-template.json file.' % etlname)
                except Exception as ex:
                    errs.append('Could not load json file at appsettingspath. Reason: %s' % str(ex))
        elif isinstance(appsettingsobj, dict):
            if not 'Etls' in appsettingsobj:
                errs.append('"Etls" attribute missing from appsettings json objecgt.')
            elif isinstance(etlname, str) and etlname in appsettingsobj['Etls']:
                errs.append('An ETL named %s has already been configured in appsettings-template.json file.' % etlname)
        if not kwargs is None and not isinstance(kwargs, dict):
            errs.append('kwargs must be a dictionary if provided.')
        if errs:
            raise Exception('\n'.join(errs))