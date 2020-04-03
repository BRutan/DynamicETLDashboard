#####################################
# Arguments.py
#####################################
# Description:
# *  

class Arguments(object):
    """
    * Validate and store script arguments.
    """
    __ReqArgs = { 'datapath' : False, 'reportpath' : False, 'filedatereg' : False, 'tablename' : False }
    def __init__(self, args):
        args = { arg.lower() : args[arg] for arg in args }
        Arguments.__CheckArgs(args)
        self.datapath = args['datapath'].replace('R:\\', '\\\\wanlink.us\\dfsroot\\APPS\\')
        self.reportpath = args['reportpath']
        self.filedateinfo = { key.lower() : args['filedatereg'][key] for key in args['filedatereg'] }
        self.filedateinfo['regex'] = re.compile(self.filedateinfo['regex'])
        self.tablename = args['tablename']
        self.filenamereg = None
        if 'filenamereg' in args:
            self.filenamereg = re.compile(args['filenamereg'])
        if 'convert' in args:
            # Convert all files before pulling:
            self.datapath = args['convert']['convertpath'].replace('R:\\', '\\\\wanlink.us\\dfsroot\\APPS\\')
    
    @staticmethod
    def __CheckArgs(args):
        """
        * Check argument validity, throw exception if any failed.
        """
        errs = []
        req = Arguments.__ReqArgs.copy()
        for arg in args:
            if arg in req:
                req[arg] = True

        missing = [arg for arg in req if not req[arg]]
        if missing:
            raise Exception(' '.join(['The following required arguments are missing:', ','.join(missing)]))

        if '.' in args['datapath']:
            errs.append('datapath must point to folder.')
        elif not os.path.exists(args['datapath']):
            errs.append(' '.join(['(datapath)', args['datapath'], 'does not exist.']))

        if not IsRegex(args['filedatereg']['Regex']):
            errs.append(' '.join(['(filedatereg)', args['filedatereg']['Regex'], 'Not a valid regular expression.']))

        if '.' not in args['reportpath']:
            errs.append('(reportpath) Must point to a file.')
        elif '\\' in args['reportpath'] and not os.path.exists(args['reportpath'][0:args['reportpath'].rfind('\\')]):
            errs.append('(reportpath) Enclosing folder does not exist.')
        elif os.path.exists(args['reportpath']):
            path = args['reportpath']
            args['reportpath'] = "%s_1.%s" % (path[0:path.find('.') - 1], path[path.find('.') + 1:])

        if 'filenamereg' in args and not IsRegex(args['filenamereg']):
            errs.append(' '.join(['(filenamereg)', args['filenamereg'], 'is not a valid regular expression.']))

        if 'convert' in args:
            if not ('convertpath' in args['convert'] and 'extension' in args['convert']):
                errs.append('convert requires "extension" and "convertpath" as attributes.')
            elif '.' not in args['convert']['extension']:
                errs.append('%s is invalid conversion extension.' % args['convert']['extension'])
            if 'convertpath' in args['convert'] and not os.path.exists(args['convert']['convertpath']):
                errs.append('convertpath does not exist.')

        if errs:
            raise Exception("\n".join(errs))

