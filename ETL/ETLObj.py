#####################################
# ETLObj.py
#####################################
# Description:
# *

import json
import os

class ETLObj(object):
    """
    * Controls creation of all aspects related to ETL pipeline.
    """
    __Fields = { "Source" : "", "Destination" : "", "TableName" : "", "OnError" : {}, 
                 "FieldOverride" : { "Fields" : [] }, "DataReader" : {}, "InputOperations" : [{ "TypeName" : ""}],
                 "PreOperations" : [ { "TypeName" : ""} ], "PostOperations" : [ { "TypeName" : ""} ] }
    __ReqFields = { "Source" : False, "Destination" : False, "TableName" : False }
    def __init__(self, path = None):
        """
        * Create raw etl, or one from path.
        """
        if path:
            self.PullETL(path)
        else:
            self.__fields = ETLObj.__Fields.copy()

    @property
    def Fields(self):
        return self.__fields.copy()

    ####################
    # Interface Methods:
    ####################
    def PostETL(self):
        pass
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
        self.__LoadFields(etlJobJSON)

    @staticmethod
    def GenerateRawETL(path = None):
        """
        * Create skeleton ETL json node to be filled in.
        """
        with open('RawEtl.json' if not path else path) as f:
            json.dump(ETLObj.__Fields, f)

    @staticmethod
    def CheckFields(jsonVals):
        """
        * Ensure all fields are present and formatted correctly.
        """
        req = ETLObj.__ReqFields.copy()
        errs = []
        invalid = []
        for field in req:
            if field in jsonVals:
                req[field] = True
        
        for field in jsonVals:
            if field not in ETLObj.__Fields:
                invalid.append(field)

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
    def __LoadFields(self, jsonObj):
        """
        * Load all fields from pulled json object.
        """

        pass