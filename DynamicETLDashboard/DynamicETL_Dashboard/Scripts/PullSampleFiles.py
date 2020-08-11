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
        funcArgs = InjectDependency(args)
        SampleFilePuller.PullFiles(**funcArgs)
    except Exception as ex:
        msg = "Failed to run PullSampleFiles.py. Reason: %s." % str(ex)
        PrintExceptionAndExit(msg)

def InjectDependency(args):
    """
    * Return dictionary mapping all arguments
    from PullSampleFiles.json to SampleFilePuller.PullFiles():
    """
    funcArgs = {}
    # DynamicETL.Service appsettings:
    args['serviceappsettings']['Etls']

    return funcArgs


if __name__ == '__main__':
    PullSampleFiles()