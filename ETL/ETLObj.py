#####################################
# ETLObj.py
#####################################
# Description:
# * Object contains all fields used for ETLs in DynamicETL.Service 
# appsettings-template.json file

from abc import ABC, abstractmethod
import json
import os
import re

class ETLObj(object):
    """
    * Object contains all fields used for ETLs in DynamicETL.Service appsettings-template.json file.
    """
    __Fields = { "Source" : "", "Destination" : "", "TableName" : "", "OnError" : {}, 
                 "FieldOverride" : { "Fields" : [] }, "DataReader" : {}, "InputOperations" : [{ "TypeName" : ""}],
                 "PreOperations" : [ { "TypeName" : ""} ], "PostOperations" : [ { "TypeName" : ""} ] }
    __DefaultFields = { "Source" : "FileVault1", "Destination" : "SqlMetricsDyetl", "TableName" : "", 
                        "OnError" : { "Method": "Email", "EmailFrom": "{DefaultErrorEmailFrom}","EmailTo": "{RiskDashboardEmailTo}"}, 
                        "DataReader" : { "TypeName":"Excel" },"InputOperations" : [{ "TypeName" : "AddRunDate"}, {"TypeName" : "AddFileDate"}],
                        "PreOperations" : [{"TypeName" : "AddFileDate", "ConfigValue": "{RegPattern:'(?<date>\\\\d{8})\\\\.', DateFormat:'yyyyMMdd'}"}], 
                        "PostOperations" : [{ "TypeName" : "DeleteDuplicateRecords"}]}
    __ReqFields = { "Source" : False, "Destination" : False, "TableName" : False }
    __ValidSources = { "filevault1" : "FileVault1", "network" : "Network" }
    __ValidDestinations = {"sqllocal" : "SqlLocal", "sqlmetricsdyetl" : "SqlMetricsDyetl", 
                           "sqlcentralcompliancemetrics" : "SqlCentralComplianceMetrics", "sqlcompl2" : "SqlCompl2",
                           "sqlpolicycoverage" : "SqlPolicyCoverage", "sqlfidessa" : "SqlFidessa" }
    def __init__(self, kwargs = None):
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
    def Fields(self):
        return self.__fields.copy()
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
    def OnError(self):
        if 'OnError' in self.__fields:
            return self.__fields['OnError']
        else:
            return None
    @property
    def FieldOverride(self):
        if 'FieldOverride' in self.__fields:
            return self.__fields['FieldOverride']
        else:
            return None
    @property
    def DataReader(self):
        return self.__fields['DataReader']
    @property
    def InputOperations(self):
        return self.__fields['InputOperations']
    @property
    def PreOperations(self):
        return self.__fields['PreOperations']
    @property
    def PostOperations(self):
        return self.__fields['PostOperations']
    @Source.setter
    def Source(self, val):
        if not isinstance(val, str):
            raise Exception('Source must be a string.')
        elif not val.lower() in ETLObj.__ValidSources:
            raise Exception('Source must be one of %s (case insensitive).' % ','.join(ETLObj.__ValidSources.values()))
        self.__fields['Source'] = ETLObj.__ValidSources[val.lower()]
    @Destination.setter
    def Destination(self, val):
        if not isinstance(val, str):
            raise Exception('Destination must be a string.')
        elif not val.lower() in ETLObj.__ValidDestinations:
            raise Exception('Destination must be one of %s (case insensitive).' % ','.join(ETLObj.__ValidDestinations.values()))
        self.__fields['Destination'] = ETLObj.__ValidDestinations[val.lower()]
    @TableName.setter
    def TableName(self, val):
        if not isinstance(val, str):
            raise Exception('TableName must be a string.')
        self.__fields['TableName'] = val
    @OnError.setter
    def OnError(self, val):
        if not isinstance(val, dict):
            raise Exception('OnError must be a dictionary.')
        self.__fields['OnError'] = val
    @FieldOverride.setter
    def FieldOverride(self, val):
        if not isinstance(val, list):
            raise Exception('FieldOverride must be a list.')
        elif any([not isinstance(entry,dict) for entry in val]):
            raise Exception('FieldOverride must only contain dictionaries.')
        self.__fields['FieldOverride'] = val
    @DataReader.setter
    def DataReader(self, val):
        if not isinstance(val, dict):
            raise Exception('DataReader must be a dictionary.')
        self.__fields['DataReader'] = val
    @InputOperations.setter
    def InputOperations(self, val):
        if not isinstance(val, list):
            raise Exception('InputOperations must be a list.')
        elif any([not isinstance(entry, dict) for entry in val]):
            raise Exception('InputOperations must only contain dictionaries in list.')
        self.__fields['InputOperations'] = val
    @PreOperations.setter
    def PreOperations(self, val):
        if not isinstance(val, list):
            raise Exception('PreOperations must be a list.')
        elif any([not isinstance(entry, dict) for entry in val]):
            raise Exception('PreOperations must only contain dictionaries in list.')
        self.__fields['PreOperations'] = val
    @PostOperations.setter
    def PostOperations(self, val):
        if not isinstance(val, list):
            raise Exception('PostOperations must be a list.')
        elif any([not isinstance(entry, dict) for entry in val]):
            raise Exception('PostOperations must only contain dictionaries in list.')
        self.__fields['PostOperations'] = val
    ####################
    # Interface Methods:
    ####################
    @classmethod
    def LoadNewETLJSON(self, jsonObj):
        """
        * Generate ETLObj from json at path.
        Inputs:
        * jsonObj: String pointing to .json file or dictionary containing
        json attributes.
        """
        if not isinstance(jsonObj, (str, dict)):
            raise Exception("jsonObj must be a string or dictionary.")
        elif isinstance(jsonObj, str):
            if not jsonObj.endswith('.json'):
                raise Exception("jsonObj must point to a .json file.")
            else:
                try:
                    etlJSON = json.load(open(path, 'rb'))
                    ETLObj.CheckFields(etlJSON)
                except Exception as ex:
                    raise Exception("Invalid JSON at path. Reason: %s" % str(ex))
                return ETLObj(etlJSON)
        else:
            return ETLObj(jsonObj)
        
    def ToJson(self):
        """
        * Generate json dict node object from attributes.
        """
        out = {}
        for attr in ETLObj.__Fields:
            if attr in self.__fields:
                out[attr] = self.__fields[attr]
        return out

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
        json.dump(ETLObj.__DefaultFields, open(path, 'wb'))

    @classmethod
    def CheckFields(cls, jsonVals):
        """
        * Ensure all fields are present and formatted correctly.
        """
        keys = list(jsonVals)
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
            raise Exception('\n'.join(errs))

    #####################
    # Private Helpers:
    #####################
    @staticmethod
    def __Validate(kwargs):
        """
        * Validate constructor parameters.
        """
        if not kwargs is None and not isinstance(kwargs, dict):
            raise Exception('kwargs must be a dictionary if provided.')

    def __FillDefault(self, kwargs):
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

    def __FillSpecified(self, kwargs):
        """
        * Fill appropriate attributes with 
        values specified in arguments.
        """
        if kwargs is None:
            return
        attrs = { attr.lower() : attr for attr in ETLObj.__Fields }
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



class ConfigVal(ABC):
    """
    * Abstract base class for ConfigVals in appsettings files.
    """
    REType = type(re.compile(''))
    ##################
    # Interface Methods:
    ##################
    @abstractmethod
    def ToJSON(self):
        pass

class DateRegexPattern(ConfigVal):
    """
    * Regular expression pattern for dates used
    in DynamicETL.Service appsettings files.
    """
    __reType = type(re.compile(''))
    def __init__(self, pattern, dateformat):
        """
        * 
        """
        DateRegexPattern.__Validate(pattern, dateformat)
        self.Pattern = pattern
        self.DateFormat = dateformat

    ##################
    # Properties:
    ##################
    @property
    def Pattern(self):
        return self.__pattern
    @property
    def DateFormat(self):
        return self.__dateformat
    @Pattern.setter
    def Pattern(self, val):
        self.__pattern = val
    @DateFormat.setter
    def DateFormat(self, val):
        self.__dateformat = val
    ##################
    # Interface Methods:
    ##################
    def ToJson(self):
        """
        * Convert to JSON attribute.
        """
        pass
    ##################
    # Private Helpers:
    ##################
    @staticmethod
    def __Validate(pattern):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(pattern, (str, ConfigVal.REType)):
            raise Exception('pattern must be a string or regular expression object.')
