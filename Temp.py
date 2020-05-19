
from ETL.TSQLInterface import TSQLInterface

def temp():
    interface = TSQLInterface('nj1sql13', 'MetricsDyetl')
    query = 'SELECT * FROM [MetricsDyetl].[dbo].[tbl_CyberSecurity_Exceptions_GS]'
    data = interface.Select(query)
    interface.Connect('localhost', 'MetricsDyetl')
    interface.Insert(data, 'tbl_CyberSecurity_Exceptions_GS', True)
    
def temp2():
    interface = TSQLInterface('.', 'MetricsDyetl')
    query = 'SELECT * FROM [MetricsDyetl].[dbo].[tbl_CyberSecurity_Exceptions_GS]'
    interface.Select(query)

if __name__ == '__main__':
    temp()