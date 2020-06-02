
from ETL.DataReader import DataReader
from ETL.DataComparer import DataComparer
from ETL.TSQLInterface import TSQLInterface

def compare():
    interface = TSQLInterface('nj1qasql13', 'MetricsDyetl')
    query = ['select * ']
    query.append("from tbl_CyberSecurity_Exceptions_GS ")
    query.append(" where fileDate = '2020-4-13'")
    query.append(" order by [Employee ID] asc, [Group] asc")
    query = ''.join(query)
    data_valid = interface.Select(query)
    ignoreCols = ['ID', 'fileDate', 'RunDate']
    pKey = TSQLInterface.PrimaryKeys(data_valid, 4, ignoreCols = ignoreCols, findFirst = True)
    interface = TSQLInterface('.', 'MetricsDyetl')
    data_test = interface.Select(query)
    DataComparer.GenerateComparisonReport('CyberSecurityExceptionsDiff_Post.xlsx', data_test, data_valid, ignoreCols, pKey)


def getdata():
    interface = TSQLInterface('.', 'MetricsDyetl')
    query = "SELECT * FROM tbl_CyberSecurity_Exceptions_GS"
    data = interface.Select(query)
    data.to_csv('SecurityExceptionsComp.csv')
    pKey = TSQLInterface.PrimaryKeys(data, 4, ignoreCols = ['ID', 'fileDate', 'RunDate'], findFirst = True)
    return pKey

def insert():
    interface = TSQLInterface('nj1qasql13', 'MetricsDyetl')
    data = DataReader.Read('C:\\Users\\berutan\\Desktop\\Projects\\New ETL\\GS.SecurityExceptions.v1\\SecurityExceptionsComp.csv')
    interface.Insert(data, 'tbl_CyberSecurity_Exceptions_GS')
    out = interface.Select("SELECT * FROM tbl_CyberSecurity_Exceptions_GS where fileDate = '2020-04-13'")
    return out

def pulldata():
    # Pull all missing tbl_CyberSecurity_Exceptions_GS filedates from Metrics
    # to later input into production folder:
    interface = TSQLInterface('nj1sql13', 'Metrics')
    results = interface.Select("SELECT DISTINCT fileDate FROM tbl_CyberSecurity_Exceptions_GS")
    existingfiles_metrics = set(results['fileDate'])
    interface = TSQLInterface('nj1sql13', 'MetricsDyetl')
    results = interface.Select("SELECT DISTINCT fileDate FROM tbl_CyberSecurity_Exceptions_GS")
    existingfiles_metricsdyetl = set(results['fileDate'])
    missing = existingfiles_metrics - existingfiles_metricsdyetl
    if missing:
        pass

    

if __name__ == '__main__':
    pulldata()