#####################################
# Test.py
#####################################
# Description:
# * 

########### TODO:
# 1. Add method to check new ETL test by querying relevant table in localdb
# and comparing to test input file.

from argparse import ArgumentParser
import pandas
from pandas import DataFrame
import os

def Test():
    parser = ArgumentParser('Test')
    parser.add_argument('targetpath', type=str)
    
    args = parser.parse_args()
    targetpath = args.targetpath

    filePaths = [os.path.join(targetpath, file) for file in os.listdir(targetpath) if '.csv' in file]
    success = []

    currDrive = os.getcwd()
    currDrive = currDrive[0:currDrive.find('\\') + 1]
    
    for path in filePaths:
        try:
            data = pandas.read_csv(path)
            success.append(path)
        except:
            pass

    print('\n'.join(success))

if __name__ == "__main__":
    Test()