#####################################
# ValidateAndAppendNewETL.py
#####################################
# Description:
# * Validate new ETL parameters.

import json
from ETL.NewETLCreator import NewETLCreator
from jsonargparse import ArgumentParser, ActionJsonSchema, ParserError
from jsonschema import validate
from Utilities.Helpers import CheckPath, CheckRegex

argschema = {}

def ValidateAndAppendNewETL():
    print("-------------------------------")
    print("ValidateAndAppendNewETL:")
    print("-------------------------------")
    # Validate application parameters:
    argfile = json.load("ValidateAndAppendNewETL.json")
    desc = "Validate new ETL attributes and append to existing DynamicETL appsettings json file."
    parser = ArgumentParser(prog="ValidateAndAppendNewETL", description=desc)
    parser.add_argument('cfg', action=ActionJsonSchema(schema = argfile))
    try:
        args = parser.parse_args()
    except ParserError as ex:
        print("Error:")
        print(str(ex))
    # Validate new ETLs and append to existing appsettings file:

    


if __name__ == '__main__':
    ValidateAndAppendNewETL()