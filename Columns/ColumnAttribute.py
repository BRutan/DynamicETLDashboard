#####################################
# ColumnAttribute.py
#####################################
# Description:
# * 

import dateutil.parser as dateparser
from decimal import Decimal
from pandas import DataFrame
from Columns.ColumnRelationships import RelationshipEnum
import re

class ColumnAttribute(object):
    """
    * Row in report.
    """
    __numericPattern = re.compile('^[0-9]+(\.[0-9]+)$')
    __floatPattern = re.compile('\.[0-9]')
    __datePattern = re.compile('^[0-9]{1,2}[//-_][0-9]{1,2}//[0-9]{2,4}$')
    __pandaTypeToSQLType = { 'object' : 'varchar(max)', 'int64' : 'int', 'datetime64[ns]' : 'datetime', 'float64' : 'decimal' }
    def __init__(self, name):
        """
        * Instantiate attributes for data column.
        """
        if not isinstance(name, str):
            raise Exception('name must be a string.')
        self.__colName = name
        self.__isNullable = None
        self.__isUnique = None
        self.__uniqueCount = None
        self.__uniques = None
        self.__type = None
        
    def __eq__(self, col):
        if type(col) != type(self):
            raise ValueError('Not a ColumnAttribute.')
        if col.ColumnName != self.ColumnName:
            return False
        if col.Type != self.Type:
            return False
        if col.IsUnique != self.IsUnique:
            return False
        if col.IsNullable != self.IsNullable:
            return False    
        return True
    
    ###################
    # Properties:
    ###################
    @property
    def ColumnName(self):
        return self.__colName
    @property
    def IsNullable(self):
        return self.__isNullable
    @property
    def IsUnique(self):
        return self.__isUnique
    @property
    def Type(self):
        return self.__type
    @property
    def UniqueCount(self):
        return self.__uniqueCount
    @property
    def Uniques(self):
        return self.__uniques
    ###################
    # Interface Methods:
    ###################
    def ParseColumn(self, column):
        """
        * Determine the type and attributes of the column.
        Inputs:
        * column: Expecting pandas Series or list-like container.
        """
        uniques = set(column.drop_duplicates())
        self.__uniqueCount = len(uniques)
        self.__isNullable = True if [val for val in column.isna() if val] else False
        self.__isUnique = True if self.__uniqueCount == len(column) else False
        # See if Pandas DataFrame has determined types, or check column types manually:
        typeStr = str(column.dtype)
        if uniques:
            typeStr = str(DataFrame(uniques)[0].dtype)
        if self.__uniqueCount < 25:
            self.__uniques = uniques
        if typeStr != 'object':
            typeStr = ColumnAttribute.DTypeToTSQLType(typeStr.lower())
        else:
            # Verify that DataFrame has determine type effectively:
            #typeStr = ColumnAttribute.__DetermineColType(uniques)
            #if typeStr == 'object':
            #   typeStr = 'varchar(max)'
            typeStr = 'varchar(max)'
        self.__type = typeStr

    def ToReportCell(self, header):
        """
        * Output row information based upon attribute.
        """
        if header.lower() == 'name':
            return self.ColumnName
        return getattr(self, header)

    @classmethod
    def DTypeToTSQLType(cls, typeStr):
        """
        * Convert dtype to appropriate TSQL type.
        """
        if 'int' in typeStr:
            if typeStr == 'int64':
                return 'bigint'
            if typeStr == 'int32':
                return 'int'
            if typeStr == 'int16':
                return 'smallint'
            else:
                return 'tinyint'
        if 'float' in typeStr:
            return 'decimal'
        if 'datetime' in typeStr:
            return 'datetime'
        if 'bool' in typeStr:
            return 'bool'
    @classmethod
    def IsInt(val):
        try:
            temp = int(val)
            return True
        except:
            return False
    @classmethod
    def IsFloat(val):
        try:
            temp = float(val)
            return True
        except:
            return False
    @classmethod
    def IsDatetime(cls, val):
        """
        * Determine if value is a datetime object.
        """
        try:
            temp = dateparser.parse(val)
            return True
        except:
            return False
    ################
    # Private Helpers:
    ################
    @staticmethod
    def __IntType(val):
        """
        * Find minimum storage size for integer type:
        """
        val = int(val)
        if val > 2 ** 31 - 1 or val < -2 ** 31:
            return 'bigint'
        if val > 2 ** 15 - 1or val < -2 ** 15:
            return 'int'
        if val > 255 or val < 0:
            return 'smallint'
        return 'tinyint'
    @staticmethod
    def __FloatType(val):
        """
        * Find minimum storage size for floating point type:
        """
        val = Decimal(val).as_tuple()
        scale = abs(val.exponent)
        prec = len(val.digits[0:scale + 1])
        return 'decimal(%d,%d)' % (prec,scale)

    @staticmethod
    def __DetermineColType(column):
        """
        * Determine appropriate type for data.
        """
        typeStr = ColumnAttribute.__GetType(column[0])
        if typeStr == 'object':
            return typeStr
        for num in xrange(1, column):
            if typeStr != ColumnAttribute.__GetType(column[num]):
                # Set type to object if of mixed types:
                return 'object'
        return typeStr
    @staticmethod
    def __GetType(val):
        if ColumnAttribute.IsInt(val):
            typeStr = 'int'
        elif ColumnAttribute.IsFloat(val):
            typeStr = 'float'
        elif ColumnAttribute.IsDatetime(val):
            typeStr = 'datetime'
        else:
            typeStr = 'object'
        return typeStr
            
            
    