#####################################
# AddFileWatcherConfig.py
#####################################
# Description:
# * Using AddFileWatcherConfig.json, generate
# new xml FileWatcher schema for QA/UAT/STG/PROD
# paths, and append into FileWatcher configuration.

import os
from Utilities.LoadArgs import ETLDashboardJsonArgs

def AddFileWatcherConfig():
    """
    * Generate filetransfer
    """
    try:
        args = ETLDashboardJsonArgs()
    except Exception as ex:
        print('Failed to load args from ETLDashboard.json')
        print('Reason: %s' % str(ex))
        input('Press enter to exit.')
        os._exit(0)


if __name__ == '__main__':
    AddFileWatcherConfig()