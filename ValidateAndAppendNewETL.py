#####################################
# ValidateAndAppendNewETL.py
#####################################
# Description:
# * 

import json
from ETL.NewETLCreator import NewETLCreator
from jsonargparse import ArgumentParser
from Utilities.Helpers import CheckPath, CheckRegex

def ValidateAndAppendNewETL():
    parser = ArgumentParser(prog="ValidateAndAppendNewETL", 
                            description="Validate new ETL attributes and append to existing DynamicETL appsettings json file.")
    parser.add_argument()

    args = json.load("ValidateAndAppendNewETL.json")



if __name__ == '__main__':
    ValidateAndAppendNewETL()