#####################################
# PostArgsFactory.py
#####################################
# Description:
# * Generate postargs.json files useful
# for posting ETL jobs to DynamicETL.WebAPI.

import json
import os

class PostArgsFactory:
    """
    * Singleton factory class to generate DynamicETL.WebAPI postargs.
    """
    __req = set("outpath","datafilepath", "etlname")
    ####################
    # Interface Methods:
    ####################
    @staticmethod
    def GeneratePostArgs(**kwargs):
        """
        * Generate postargs at path using 
        provided arguments.
        Inputs:
        * kwargs: Named parameter list
        with following required keys:
            * outpath: String path to output postargs json file.
            * etlname: String name of ETL.
            * datafilepath: String path to datafile to be posted to WebAPI.  
        """
        kwargs = { key.lower() : kwargs[key] for key in kwargs }
        PostArgsFactory.__Validate(kwargs)
        out = {'id' : 10, 'fileid' : 10}
        out['subject'] = kwargs['etlname']
        out['arg'] = "{'FilePath':'%s'}" % kwargs['datafilepath']
        out['fileName'] = os.path.split(kwargs['datafilepath'])[1]
        json.dump(out, open(kwargs['outpath'], 'r'), indent = 4)

    @staticmethod
    def __Validate(kwargs):
        """
        * Validate arguments.
        """
        errs = []
        missing = set(PostArgsFactory.__req) - set(kwargs)
        if missing:
            errs.append('The following required arguments are missing: %s' % ','.join(missing))
        elif any([not isinstance(kwargs[key],str) for key in kwargs]):
            errs.append('All kwargs entries must be keys.')
        if 'datafilepath' in kwargs and isinstance(kwargs['datafilepath'], str) and not os.path.isfile(kwargs['datafilepath']):
            errs.append('datafilepath does not point to existing file.')
        if 'outpath' in kwargs and isinstance(kwargs['outpath'], str) and not kwargs['outpath'].endswith('.json'):
            errs.append('outpath must point to json file.')
        if errs:
            raise Exception('\n'.join(errs))
