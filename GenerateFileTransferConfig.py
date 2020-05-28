#####################################
# GenerateFileTransferConfig.py
#####################################
# Description:
# * Using FileTransferService webportal,
# pull all transfers for all etls and place
# in summarized json file. 

import os
from Filepaths.FileTransferServiceAggregator import FileTransferServiceAggregator
from Utilities.LoadArgs import ETLDashboardJsonArgs, GenerateFileTransferConfigJsonArgs

def GenerateFileTransferConfig():
    print("------------------------------")
    print('GenerateFileTransferConfig')
    print("------------------------------")
    try:
        print("Pulling arguments from ETLDashboard.json.")
        args = ETLDashboardJsonArgs()
        args.update(GenerateFileTransferConfigJsonArgs())
    except Exception as ex:
        print('Failed to pull arguments from ETLDashboard.json.')
        print('Reason: %s' % str(ex))
        input('Press enter to exit.')
        os._exit(0)

    # Pull all configuration files from web portal:
    try:
        print("Aggregating all transfers from web portal at")
        print("%s" % args['filetransferurl'])
        agg = FileTransferServiceAggregator(args['filetransferurl'], args['chromedriverpath'], args['groupregex'])
        agg.OutputLookup()
    except Exception as ex:
        print('Failed to aggregate from filetransferurl.')
        print('Reason: %s' % ex)
        input('Press enter to exit.')
        os._exit(0)

if __name__ == '__main__':
    GenerateFileTransferConfig()
