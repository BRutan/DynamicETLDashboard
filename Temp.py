
from ETL.DataReader import DataReader
from ETL.DataComparer import DataComparer
from ETL.TSQLInterface import TSQLInterface
from ETL.DynamicETLIssueParser import DynamicETLIssueParser

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
    interface = TSQLInterface('nj1sql13', 'Metrics')
    query = "EXEC dbo.AdHocReport_StuDurnin_DeskVolumes '2020-05-29'"
    results = interface.Execute(query, True)
    results.to_csv("StuDurninResults.csv")

    
def testlogreader():
    reader = DynamicETLIssueParser('\\\\nj1app20\\logs')
    reader.GenerateFile('FileVaultETLErrors_6_4_2020.csv')



if __name__ == '__main__':
    #testlogreader()
    pulldata()