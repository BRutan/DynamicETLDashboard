#####################################
# ColumnAttribute.py
#####################################
# Description:
# * 

from decimal import Decimal
from pandas import DataFrame
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
    def Relationships(self):
        return self.__relationships
    @property
    def Type(self):
        return self.__type
    @property
    def UniqueCount(self):
        return self.__uniqueCount
    ###################
    # Interface Methods:
    ###################
    def ParseColumn(self, column):
        """
        * Determine the type and attributes of the column.
        Inputs:
        * column: Expecting pandas Series or list-like container.
        """
        uniques = set([val for num, val in enumerate(column) if not column.isna()[num]])
        self.__uniqueCount = len(uniques)
        self.__isNullable = True if len([val for val in column.isna() if val]) > 0 else False
        self.__isUnique = True if self.__uniqueCount == len(column) else False
        # See if Pandas DataFrame has determined types, or check column types manually:
        typeStr = str(column.dtype)
        if uniques and self.__isNullable:
            # Get specific type:
            typeStr = str(DataFrame(uniques)[0].dtype)
        if typeStr != 'object':
            self.__type = ColumnAttribute.DTypeToTSQLType(typeStr.lower())
        else:
            #self.__type = ColumnAttribute.ParseCells(uniques)
            self.__type = 'varchar(max)'

    def ToReportCell(self, header):
        """
        * Output row information based upon attribute.
        """
        if header.lower() == 'name':
            return self.ColumnName
        return getattr(self, header)

    @staticmethod
    def DTypeToTSQLType(typeStr):
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
        
    @staticmethod
    def IsNumeric(val):
        return ColumnAttribute.__numericPattern.match(val) is None

    ################
    # Private Helpers:
    ################
    @staticmethod
    def __IntType(val):
        # Find minimum storage size for integer type:
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
        # Find minimum storage size for floating point type:
        val = Decimal(val).as_tuple()
        scale = abs(val.exponent)
        prec = len(val.digits[0:scale + 1])
        return 'decimal(%d,%d)' % (prec,scale)

    @staticmethod
    def __ParseCells(column):
        """
        * Parse attributes of all cells.
        """
        for val in column:
            val = val.strip()
            # Determine type:
            self.__SetType(val)
            if val not in self.__uniquevals:
                self.__uniqueVals[val] = True

    