#####################################
# NewETLAppender.py
#####################################
# Description:
# * Validate new ETL and append to build script for use in DynamicETL.Service.

import json
import os

class NewETLAppender:
    """
    * Check that ETLs are formatted properly and append to 
    existing DynamicETL build script.
    """
    def __init__(self, newetlpath, buildscriptpath):
        """
        * Ensure new ETL attributes are properly formatted,
        append to new attribute path. 
        """
        errs = []
        if isinstance(newetlpath, str):
            newetlpath = [newetlpath]
        if not isinstance(newetlpath, list):
            errs.append('newetlpath must be a string path or list of string paths to json ETL attribute files.')
        else:
            notjson = set([path for path in newetlpath if not '.json' in path])
            missing = [os.path.basename(path) for path in newetlpath if not path in notjson and os.path.exists(path)]
            if notjson:
                errs.append('The following do not point to json files: {\n%s\n}' % '\n'.join([os.path.basename(path) for path in notjson]))
            if missing:
                errs.append('The following json files do not exist at path: {\n%s\n}' % '\n'.join(missing))

        if not isinstance(buildscriptpath, str):
            errs.append('buildscriptpath must be a string path or list of string paths.')
        elif not os.path.exists(buildscriptpath):
            errs.append('buildscriptpath does not exist.')
        if errs:
            raise Exception('\n'.join(errs))
        # Validate all ETLs:
        self.__etls = {}
        self.__failed = {}
        for etlpath in newetlpath:
            try:
                name = os.path.basename(etlpath)
                self.__etls[name] = ETLObj(etlpath)
            except Exception as ex:
                self.__failed[name] = ex
        
        if self.__failed:
            raise Exception('\n'.join())

        self.__newetls = newetlpath
        self.__buildscript = buildscriptpath

    ####################
    # Private Helpers:
    ####################
    def __AppendToScript(self):
        """
        * Append new etls to script.
        """
        if not self.__etls:
            return

        buildscriptJSON = json.load(self.__buildscript)
        for name in self.__etls:
            buildscriptJSON['etl']['name'] = self.__etls[name].Fields
            


class ETLObj(object):
    """
    * Immutable object to verify new ETL fields, store and append to
    DynamicETL build script.
    """
    __Fields = { "Source" : "", "Destination" : "", "TableName" : "", "OnError" : {}, 
                 "FieldOverride" : { "Fields" : [] }, "DataReader" : {}, "InputOperations" : [{ "TypeName" : ""}],
                 "PreOperations" : [ { "TypeName" : ""} ], "PostOperations" : [ { "TypeName" : ""} ] }
    __ReqFields = { "Source" : False, "Destination" : False, "TableName" : False }
    def __init__(self, path):
        """
        * Create new ETL using fields listed in json file at path.
        """
        self.LoadNewETLJSON(path)
    ####################
    # Properties:
    ####################
    @property
    def Fields(self):
        return self.__fields.copy()
    ####################
    # Interface Methods:
    ####################
    def __LoadNewETLJSON(self, path):
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
    def GenerateRawETL(cls, path = None):
        """
        * Create skeleton ETL json node to be filled in.
        """
        with open('RawEtl.json' if not path else path) as f:
            json.dump(ETLObj.__Fields, f)

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

    @classmethod
    def __CheckSubFields(cls, field):
        """
        * Ensure all subfields are valid.
        """
        for subfield in field:
            pass
    #####################
    # Private Helpers:
    #####################
    def __LoadFields(self, jsonObj):
        """
        * Load all fields from pulled json object.
        """

        pass


class InvalidETL(Exception):
    """
    * Exception raised if ETL attributes are invalid.
    """
    def __init__(self, name, message):
        self.__name = name
        self.__message = message

    def __str__(self):
        return "%s:%s" % (self.__name, self.__message)
