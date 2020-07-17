#####################################
# PullSampleFiles.py
#####################################
# Description:
# * Pull sample files corresponding to 
# one or more ETLs before testing the
# pipeline, to output folder.

from ETL.SampleFilePuller import SampleFilePuller
from Utilities.Helpers import PrintExceptionAndExit
from Utilities.LoadArgs import PullSampleFilesJsonArgs

def PullSampleFiles():
    """
    * Pull sample files corresponding to 
    one or more ETLs before testing the
    pipeline, to output folder.
    """
    try:
        args = PullSampleFilesJsonArgs()
        funcArgs = MapETLsToConfig(args)
        SampleFilePuller.PullFiles(**funcArgs)
    except Exception as ex:
        msg = "Failed to run PullSampleFiles.py. Reason: %s." % str(ex)
        PrintExceptionAndExit(msg)

def MapETLsToConfig(args):
    """
    *
    """
    pass


if __name__ == '__main__':
    PullSampleFiles()
