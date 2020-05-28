#####################################
# GenerateFileTransferConfig.py
#####################################
# Description:
# * Using FileTransferService webportal,
# pull all transfers for all etls and place
# in summarized json file. 

import os
from Filepaths.FileTransferServiceAggregator import FileTransferServiceAggregator
from Utilities.LoadArgs import ETLDashboardJsonArgs

def GenerateFileTransferConfig():
    try:
        args = ETLDashboardJsonArgs()
    except Exception as ex:
        print('Failed to pull arguments from ETLDashboard.json.')
        print('Reason: %s' % str(ex))
        input('Press enter to exit.')
        os._exit(0)

    # Pull all configuration files from web portal:
    try:
        FileTransferServiceAggregator(args[''])


if __name__ == '__main__':
    GenerateFileTransferConfig()
