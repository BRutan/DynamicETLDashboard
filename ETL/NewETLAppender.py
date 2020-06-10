#####################################
# NewETLAppender.py
#####################################
# Description:
# * Validate new ETL and append to build script for use in DynamicETL.Service.

import json
import os
from Utilities.Helpers import LoadJsonFile

class NewETLAppender:
    """
    * Append new ETL to existing DynamicETL.Service appsettings-template.json file.
    """
    def __init__(self, etlname, appsettingspath, **kwargs = None):
        """
        * Append new etl with name to appsettings-template.json
        file.
        Inputs:
        * etlname: String name of new ETL. Cannot be present already in appsettings-template.json
        file.
        * appsettingspath: Path to .json file containing DynamicETL.Service appsettings-template.json
        attributes.
        Optional Inputs:
        * kwargs: Dictionary containing one or more of following keys:

        """
        NewETLAppender.__Validate(etlname, appsettingspath, kwargs)
        self.__appsettingstemplate = json.load(open(appsettingspath, 'rb'))
        self.__AppendNewETL(etlname, appsettingspath, kwargs)

    ####################
    # Interface Methods:
    ####################
    def AppendNewETL(self, newetlname, **kwargs = None):
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
        json.dump(self.__appsettingstemplate, open(path, 'wb'))

    ####################
    # Private Helpers:
    ####################
    def __AppendNewETL(self, etlname, **kwargs):
        """
        * Append new etl to appsettings-template.json file.
        """
        kwargs['etlname'] = etlname
        newETL = ETLObj(kwargs)
        this.__appsettingstemplate['Etls'][etlname] = newETL
    
    @staticmethod
    def __Validate(etlname, appsettingspath, **kwargs):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(etlname, str):
            errs.append('etlname must be a string.')
        if not isinstance(appsettingspath, str):
            errs.append('appsettingspath must be a string.')
        elif not appsettingspath.endswith('.json'):
            errs.append('appsettingspath must point to a json file.')
        elif not os.path.exists(appsettingspath):
            errs.append('appsettings-template.json file at path does not exist.')
        else:
            try:
                vals = json.load(open(appsettingspath, 'rb'))
                if not 'Etls' in vals:
                    errs.append('"Etls" attribute missing from appsettings-template.json file.')
                elif isinstance(etlname, str) and etlname in vals['Etls']:
                    errs.append('An ETL named %s has already been configured in appsettings-template.json file.' % etlname)
            except Exception as ex:
                errs.append('Could not load json file at appsettingspath. Reason: %s' % str(ex))
        if not kwargs is None and not isinstance(kwargs, dict):
            errs.append('kwargs must be a dictionary if provided.')
        if errs:
            raise Exception('\n'.join(errs))

class ETLObj(object):
    """
    * Immutable object to verify new ETL fields, store and append to
    DynamicETL build script.
    """
    __Fields = { "Source" : "", "Destination" : "", "TableName" : "", "OnError" : {}, 
                 "FieldOverride" : { "Fields" : [] }, "DataReader" : {}, "InputOperations" : [{ "TypeName" : ""}],
                 "PreOperations" : [ { "TypeName" : ""} ], "PostOperations" : [ { "TypeName" : ""} ] }
    __DefaultFields = { "Source" : "FileVault1", "Destination" : "SqlMetricsDyetl", "TableName" : "", 
                       "OnError" : {"Method": "Email","EmailFrom": "{DefaultErrorEmailFrom}","EmailTo": "{RiskDashboardEmailTo}"}, 
                        "FieldOverride" : { }, "DataReader" : { "TypeName":"Excel"}, 
                        "InputOperations" : [{ "TypeName" : "AddRunDate"}, {"TypeName" : "AddFileDate"}],
                        "PreOperations" : [], "PostOperations" : [{ "TypeName" : "DeleteDuplicateRecords"}]}
    __ReqFields = { "Source" : False, "Destination" : False, "TableName" : False }
    def __init__(self, **kwargs = None):
        """
        * Create new ETL using fields listed in json file at path.
        Inputs;
        * kwargs: Named arguments list or dictionary with following parameters:
            * etlname: Name of etl.
            * 
        """
        ETLObj.__Validate(kwargs)
        self.__fields = ETLObj.__Fields
        self.__FillDefault(kwargs)
        self.__FillSpecified(kwargs)
    ####################
    # Properties:
    ####################
    @property
    def Source(self):
        return self.__fields['Source']
    @property
    def Destination(self):
        return self.__fields['Destination']
    @property
    def TableName(self):
        return self.__fields['TableName']
    @property
    def Fields(self):
        return self.__fields.copy()
    ####################
    # Interface Methods:
    ####################
    def LoadNewETLJSON(self, path):
        """
        * Load ETL in json at path.
        """
        if not isinstance(path, str):
            raise Exception("path must be a string.")
        elif '.json' not in path:
            raise Exception("path must point to a .json file.")
        elif not os.path.exists(path):
            raise Exception("File at path does not exist.")
        try:
            etlJSON = json.load(open(path, 'rb').read())
        except:
            raise Exception("Could not read json at path.")
        # Ensure all required fields were entered:
        ETLObj.CheckFields(etlJSON)
        self.__LoadFields(etlJSON)

    @classmethod
    def GenerateRawETL(cls, path):
        """
        * Create skeleton ETL json node to be filled in.
        Inputs:
        * path: String pointing to .json file.
        """
        if not isinstance(path, str):
            raise Exception('path must be a string.')
        elif not path.endswith('.json'):
            raise Exception('path must point to a .json file.')
        json.dump(ETLObj.__Fields, open(path, 'wb'))

    @classmethod
    def CheckFields(cls, jsonVals):
        """
        * Ensure all fields are present and formatted correctly.
        """
        keys = list(jsonVals.keys())
        errs = []
        name = "Unknown"
        if len(keys) > 1:
            errs.append("Top key must be the ETL name.")
        else:
            name = keys[0]
            req = ETLObj.__ReqFields.copy()
            invalid = []
            for field in req:
                if field in jsonVals:
                    req[field] = True

            invalid_subs = None
            for field in jsonVals:
                if field not in ETLObj.__Fields:
                    invalid.append(field)
                elif field in ETLObj.__Fields and isinstance(field, dict):
                    # Ensure all subfields are valid:
                    invalid_subs = ETLObj.__CheckSubFields(field)
                    if invalid_subs:
                        invalid.append(invalid_subs)

            missing = [field for field in req if not req[field]]
            if missing:
                errs.append(''.join(['The following required fields are missing: ', ','.join(missing)]))
            if invalid:
                errs.append(''.join(['The following fields are invalid: ', ','.join(invalid)]))
        if errs:
            raise InvalidETL(name,'\n'.join(errs))

    #####################
    # Private Helpers:
    #####################
    @staticmethod
    def __Validate(**kwargs):
        """
        * Validate constructor parameters.
        """
        if not kwargs is None:
            pass

    def __FillDefault(self, **kwargs):
        """
        * Fill default values if not specified in
        parameters.
        """
        default = ETLObj.__DefaultFields
        attrs = { attr.lower() : attr for attr in default }
        if kwargs is None:
            for attr in attrs:
                setattr(self, attrs[attr], default[attrs[attr]])
        else:
            kwargs = { key.lower() : kwargs[key] for key in kwargs }
            for attr in default:
                if not attr.lower() in kwargs:
                    setattr(self, attrs[attr.lower()], default[attrs[attr.lower()]])

    def __FillSpecified(self, **kwargs):
        """
        * Fill appropriate attributes with 
        values specified in arguments.
        """
        if kwargs is None:
            return
        attrs = { attr.lower() : attr for attr in ETLObj.__DefaultFields }
        kwargs = { key.lower() : kwargs[key] for key in kwargs }
        failed = []
        for arg in kwargs:
            if arg in attrs:
                try:
                    setattr(self, attrs[arg], kwargs[arg])
                except Exception as ex:
                    errs.append('Arg: %s, Reason: %s' % (arg, str(ex)))
        if failed:
            failed.insert(0, "The following arguments are invalid:")
            raise Exception('\n'.join(failed))
