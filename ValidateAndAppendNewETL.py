#####################################
# ValidateAndAppendNewETL.py
#####################################
# Description:
# * Generate updated appsettings-template.json file for use
# in 

import json
from ETL.NewETLAppender import NewETLAppender
from jsonargparse import ArgumentParser, ActionJsonSchema, ParserError
from jsonschema import validate
from Utilities.Helpers import CheckPath, CheckRegex

argschema = {}

def GetArgsFromJson():
    """
    * Get arguments for this script from local json file.
    """
    args = json.load("ValidateAndAppendNewETL.json")


def ValidateAndAppendNewETL():
    print("-------------------------------")
    print("ValidateAndAppendNewETL:")
    print("-------------------------------")
    # Validate application parameters:
    args = GetArgsFromJson()
    # Validate new ETLs and append to existing appsettings file:
    appender = NewETLAppender(args.path, args.buildscriptpath)
    


if __name__ == '__main__':
    ValidateAndAppendNewETL()