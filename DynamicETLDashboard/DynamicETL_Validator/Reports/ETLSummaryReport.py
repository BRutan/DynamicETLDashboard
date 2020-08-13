#####################################
# ETLSummaryReport.py
#####################################
# Description:
# * Class that creates specific summary reports
# for ETLs detailing result after ETL has completed following DynamicEtl.Service
# run in production.

from datetime import datetime
import dateutil.parser as dtparse
from DynamicETL_Dashboard.ETL.DataReader import DataReader
import os
import xlsxwriter

class ETLSummaryReport:
    """
    * Class that creates specific summary reports
    for ETLs detailing result after ETL has completed following DynamicEtl.Service
    run in production.
    """
    __headerFormat = {'bold': True, 'font_color': 'white', 'bg_color' : 'black'}
    __optional = { 'preops' : (list, str), 'inputops' : (list, str), 'postops' : (list, str) }
    __req = { 'etl' : str, 'tablename' : str, 'database' : str, 'sourcedata' : dict, 'insertdata' : dict, 
              'server' : str, 'status' : str, 'starttime' : (str, datetime), 'endtime' : (str, datetime) }
    def __init__(self, **kwargs):
        """
        * Accumulate data necessary to generate report 
        using passed arguments.
        Inputs:
        * etl: Name of ETL.
        * tablename: Name of table associated with ETL.
        * database: Database where ETL table exists.
        * sourcedata: json dictionary containing all data rows from original source.
        * insertdata: json dictionary containing all data rows inserted into target.
        * server: Server hosting database and table.
        * status: String detailing whether ETL completed successfully or not.
        * starttime: Datetime when ETL started processing.
        * endtime: Datetime when ETL stopped processing.
        Optional:
        * pre/input/post: Pre/Input/Post operations performed during ETL.
        """
        # Normalize and validate arguments:
        kwargs = { arg.lower() : kwargs[arg] for arg in kwargs if isinstance(arg, str) }
        ETLSummaryReport.__Validate(True, **kwargs)
        self.__GetProperties(**kwargs)
        
    #################
    # Properties:
    #################
    @property
    def Database(self):
        return self.__database
    @property
    def EndTime(self):
        return self.__endtime
    @property
    def ETLName(self):
        return self.__etlname
    @property
    def ETLStatus(self):
        return self.__etlstatus
    @property
    def InputOperations(self):
        return self.__inputoperations
    @property
    def InsertionRowCount(self):
        return self.__insertionrowcount
    @property
    def PostOperations(self):
        return self.__postoperations
    @property
    def PreOperations(self):
        return self.__preoperations
    @property
    def Server(self):
        return self.__server
    @property
    def SourceRowCount(self):
        return self.__sourcerowcount
    @property
    def StartTime(self):
        return self.__starttime
    @property
    def TableName(self):
        return self.__tablename

    #################
    # Interface Methods:
    #################
    @classmethod
    def ArgsAreValid(cls, databody, errs = None):
        """
        * Indicate whether or not databody is valid and
        can be used to generate reports.
        Inputs;
        * databody: json dictionary containing arguments
        to generate report with.
        Optional:
        * errs: List to include errors.
        """
        if not errs is None and not isinstance(errs, list):
            raise ValueError('errs must be a list if provided.')
        issues = ETLSummaryReport.__Validate(False, **databody)
        if issues and errs:
            errs = issues
        if issues:
            return False
        return True

    @classmethod
    def RequiredArguments(cls):
        """
        * Return required arguments and types
        necessary to generate report.
        """
        return copy.deepcopy(ETLSummaryReport.__req)

    def GenerateReport(self, reportpath):
        """
        * Generate ETLSummaryReport at path.
        """
        if not isinstance(reportpath, str):
            raise Exception('reportpath must be a string.')
        elif not reportpath.endswith('xlsx'):
            raise Exception('reportpath must point to .xlsx file.')
        # Generate report:
        wb = xlsxwriter.Workbook(reportpath)
        self.__FillData(wb)
        wb.close()

    #################
    # Private Helpers:
    #################
    def __FillData(self, wb):
        """
        * Fill data into the workbook.
        """
        headerFormat = wb.add_format(ETLSummaryReport.__headerFormat)
        summarySheet = wb.add_worksheet('Summary')
        # chgSheet.write(rowNum, 0, "FileDate:", headerFormat)
        properties = [attr for attr in dir(self) if not attr.startswith('_') and not callable(getattr(self, attr))]
        for row, prop in enumerate(properties):
            summarySheet.write(row, 0, prop, headerFormat)
            obj = getattr(self, prop)
            if isinstance(obj, datetime):
                val = obj.strftime('%m/%d/%Y %H:%M:%S')
            elif not isinstance(obj, str) and hasattr(obj, '__iter__'):
                val = ';'.join(obj)
            else:
                val = obj
            summarySheet.write(row, 1, val)

    def __GetProperties(self, **kwargs):
        """
        * Accumulate all properties from kwargs 
        before analyzing.
        """
        self.__database = kwargs['database']
        
        self.__etlname = kwargs['etl']
        self.__etlstatus = kwargs['status']
        self.__sourcerowcount = 0
        self.__insertionrowcount = 0
        self.__server = kwargs['server']
        self.__starttime = dtparse.parse(kwargs['starttime'])
        self.__endtime = dtparse.parse(kwargs['endtime'])
        self.__tablename = kwargs['tablename']
        self.__inputoperations = []
        self.__postoperations = []
        self.__preoperations = []
        self.__SetOps(**kwargs)
        self.__AccumulateStatistics(**kwargs)
        
    def __SetOps(self, **kwargs):
        """
        * Set attributes about operations performed.
        """
        if 'inputops' in kwargs:
            if isinstance(kwargs['inputops'], str):
                kwargs['inputops'] = [kwargs['inputops']]
            self.__inputoperations = set(kwargs['inputops'])
        if 'postops' in kwargs:
            if isinstance(kwargs['postops'], str):
                kwargs['postops'] = [kwargs['postops']]
            self.__postoperations = set(kwargs['postops'])
        if 'preops' in kwargs:
            if isinstance(kwargs['preops'], str):
                kwargs['preops'] = [kwargs['preops']]
            self.__preoperations = set(kwargs['preops'])

    def __AccumulateStatistics(self, **kwargs):
        """
        * Accumulate statistics regarding ETL.
        """
        self.__ReviewData(**kwargs)
        #self.__CompareSources(**kwargs)

    def __ReviewData(self, **kwargs):
        """
        * Accumulate ETL performance attributes by analysing sent dataset.
        """
        # Assuming that data has following form: {{ColName->Value}}:
        colSource = list(kwargs['sourcedata'].keys())[0]
        colInsert = list(kwargs['insertdata'].keys())[0]
        self.__sourcerowcount = len(kwargs['sourcedata'][colSource])
        self.__insertionrowcount = len(kwargs['insertdata'][colInsert])

    def __CompareSources(self, **kwargs):
        """ DEPRECATED
        * Compare the input file versus the data that was
        loaded into the table.
        """
        # NOTE: this is deprecated, but potentially can be reimplemented, since
        # api is naive about data source to maintain loose coupling.
        errs = []
        try:
            inputData = DataReader.Read(kwargs['inputfilepath'], delim = kwargs['delim'])
            self.__filerowcount = len(inputData)
        except Exception as ex:
            folder, filename = os.path.split(kwargs['inputfilepath'])
            errs.append('Could not pull data from %s. Reason: %s.' % (filename, str(ex)))
        # Query server to get inserted row count:
        try:
            interface = TSQLInterface(self.__server, self.__database)
            fdStr = kwargs['filedate'].strftime('%m-%d-%Y')
            data = interface.Select("SELECT * FROM [%s] WHERE [FileDate] = '%s'" % (self.__tablename, fdStr))
            self.__insertionrowcount = len(data)
        except Exception as ex:
            errs.append('Could not query %s::%s. Reason: %s' % str(ex))
        if errs:
            raise Exception('\n'.join(errs))

    @staticmethod
    def __Validate(raiseErr, **kwargs):
        """
        * Validate constructor parameters.
        """
        errs = []
        # Ensure all required arguments present:
        missing = set(ETLSummaryReport.__req) - set(kwargs)
        if missing:
            raise Exception('The following required arguments are missing: %s' % ','.join(missing))
        # Validate required arguments:
        for arg in ETLSummaryReport.__req:
            _type = ETLSummaryReport.__req[arg]
            if not isinstance(kwargs[arg], _type):
                if hasattr(_type, '__iter__'):
                    errs.append('%s must be one of: %s.' % ','.join([str(tp) for tp in _type]))
                else:
                    errs.append('%s must be a %s.' % str(tp))
        # Validate optional arguments:
        for op in ETLSummaryReport.__optional:
            _type = ETLSummaryReport.__optional[op]
            if op in kwargs and not isinstance(kwargs[op], _type):
                if hasattr(_type, '__iter__'):
                    errs.append('%s must be one of: %s.' % ','.join([str(tp) for tp in _type]))
                else:
                    errs.append('%s must be a %s.' % str(tp))    
        if errs and raiseErr:
            raise Exception('\n'.join(errs))
        elif errs:
            return errs
