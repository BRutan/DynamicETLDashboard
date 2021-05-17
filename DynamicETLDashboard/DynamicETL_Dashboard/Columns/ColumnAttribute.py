#####################################
# ColumnAttribute.py
#####################################
# Description:
# * Immutable object containing target column attributes.

from Columns.ColumnRelationships import RelationshipEnum
import copy
import dateutil.parser as dateparser
from decimal import Decimal
from pandas import DataFrame
import numpy as np
import re
from sortedcontainers import SortedSet

class ColumnAttribute(object):
    """
    * Immutable object containing target column's attribute.
    """
    __datePattern = re.compile('^[0-9]{1,2}[//-_][0-9]{1,2}//[0-9]{2,4}$')
    __dtypeToSQLType = { 'o' : 'varchar(max)', 'int64' : 'int', 'datetime64[ns]' : 'datetime', 'float64' : 'decimal(30,10)' }
    __floatPattern = re.compile('\.[0-9]')
    __numericPattern = re.compile('^[0-9]+(\.[0-9]+)$')
    __tSQLTypes = set(__dtypeToSQLType.values())
    def __init__(self, name, isnullable = None, isunique = None, uniquecount = None, uniques = None, type = None):
        """
        * Instantiate attributes for data column.
        """
        if not isinstance(name, str):
            raise Exception('name must be a string.')
        self.__colName = name
        self.__isNullable = isnullable
        self.__isUnique = isunique
        self.__uniqueCount = uniquecount
        self.__uniques = uniques
        self.__type = type
        
    def __eq__(self, col):
        """
        * Equality operator. Checks if column has
        same name and meta-data.
        """
        if not isinstance(col, ColumnAttribute):
            raise ValueError('equality operator not supported with %s.' % str(type(col)))
        if col.ColumnName != self.ColumnName:
            return False
        if col.IsUnique != self.IsUnique:
            return False
        if col.IsNullable != self.IsNullable:
            return False    
        if col.Type != self.Type:
            return False
        if (col.Uniques is None) ^ (self.Uniques is None):
            return False
        elif not col.Uniques is None and not self.Uniques is None and set(col.Uniques).symmetric_difference(set(self.Uniques)):
            return False
        return True

    def __sub__(self, col):
        """
        * Subtraction operator. Returns a ColumnAttributesDiff 
        object detailing difference in name and meta-data
        between lhs and rhs columns.
        """
        if not isinstance(col, ColumnAttribute):
            raise ValueError('subtraction operator not supported with %s.' % str(type(col)))
        diff = ColumnAttributeDiff.GetDifference(self, col)
        if diff.HasDiff:
            diff.ColumnName = col.ColumnName
            return diff
        return None
    
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
    @classmethod
    def LeastRestrictive(cls, columnLeft, columnRight):
        """
        * Return new ColumnAttribute with the least
        restrictive meta attributes.
        Inputs:
        * columnLeft, columnRight: ColumnAttribute objects.
        Must have same ColumnName.
        """
        errs = []
        if not isinstance(columnLeft, ColumnAttribute):
            errs.append('columnLeft must be a ColumnAttribute object.')
        if not isinstance(columnRight, ColumnAttribute):
            errs.append('columnRight must be a ColumnAttribute object.')
        if not errs:
            if columnLeft.ColumnName != columnRight.ColumnName:
                errs.append('columnLeft and columnRight have different column names')
        if errs:
            raise Exception('\n'.join(errs))

        # Return column with the least restrictive meta-attributes of either column:
        colName = columnLeft.ColumnName
        allUniques = set(columnLeft.Uniques).union(columnRight.Uniques)
        allUniques = DataFrame({colName : allUniques})
        column = ColumnAttribute(colName)
        column._ColumnAttribute__isNullable = columnLeft.IsNullable or columnRight.IsNullable
        column._ColumnAttribute__isUnique = columnLeft.IsUnique and columnRight.IsUnique
        column._ColumnAttribute__uniqueCount = max(columnLeft.UniqueCount, columnRight.UniqueCount)
        column._ColumnAttribute__uniques = copy.deepcopy(allUniques)
        column._ColumnAttribute__type = cls.LeastRestrictiveType(columnLeft.Type, columnRight.Type)

        return column

    @classmethod
    def LeastRestrictiveType(cls, typeLeft, typeRight):
        """
        * Return the least restrictive type.
        Inputs:
        * typeLeft, typeRight: string types that must be present
        in TSQLTypes.
        """
        errs = []
        if not isinstance(typeLeft, str):
            errs.append('typeLeft must be a string.')
        elif not typeLeft in ColumnAttribute.__tSQLTypes:
            errs.append('typeLeft not present in TSQLTypes.')
        if not isinstance(typeRight, str):
            errs.append('typeRight must be a string.')
        elif not typeRight in ColumnAttribute.__tSQLTypes:
            errs.append('typeRight not present in TSQLTypes.')
        if errs:
            raise Exception('\n'.join(errs))
        # Return the least restrictive type:
        # Default case:
        if typeLeft == typeRight:
            return typeLeft
        # Check for string types:
        if typeLeft == 'varchar(max)' or typeRight == 'varchar(max)':
            return 'varchar(max)'
        # Check numeric types:
        if 'decimal' in typeLeft or 'decimal' in typeRight:
            return typeLeft
        if typeLeft == 'int64' or typeRight == 'int64':
            return 'int64'
        # Check date types:
        if 'datetime' in typeLeft or 'datetime' in typeRight:
            return 'datetime'
        
    @classmethod
    def TSQLTypes(cls):
        """
        * Return set of all T-SQL types that currently 
        get converted to.
        """
        return copy.deepcopy(__tSQLTypes)

    def ParseColumn(self, column):
        """
        * Determine the type and attributes of the column.
        Inputs:
        * column: Expecting pandas Series or list-like container.
        """
        uniques = column.drop_duplicates()
        self.__uniqueCount = len(uniques.dropna())
        self.__isNullable = any(column.isna())
        self.__isUnique = True if self.__uniqueCount == len(column) else False
        if self.__uniqueCount < 25:
            self.__uniques = uniques.dropna()
        # Determine the narrowest column type:
        if self.__uniqueCount == 0:
            # If all NULLS, default to varchar(max):
            typeStr = 'varchar(max)'
        else:
            # See if Pandas DataFrame has determined types, or check column types manually:
            typeStr = DataFrame(list(uniques)).dtypes[0].name.lower()
            if typeStr != 'object':
                typeStr = ColumnAttribute.DTypeToTSQLType(typeStr.lower())
            else:
                # Verify that DataFrame has determine type effectively:
                typeStr = ColumnAttribute.__DetermineColType(uniques)
                if typeStr == 'o':
                    typeStr = 'varchar(max)'
                elif typeStr in ColumnAttribute.__dtypeToSQLType:
                    typeStr = ColumnAttribute.__dtypeToSQLType[typeStr]
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
            return 'decimal(30, 10)'
        if 'datetime' in typeStr:
            return 'datetime'
        if 'bool' in typeStr:
            return 'bit'
    @classmethod
    def IsInt(cls, val):
        try:
            temp = int(val)
            return True
        except:
            return False
    @classmethod
    def IsFloat(cls, val):
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
        if val > 2 ** 15 - 1 or val < -2 ** 15:
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
        if typeStr == 'o':
            return typeStr
        for num, val in enumerate(column):
            if num != 0 and typeStr != ColumnAttribute.__GetType(val):
                # Set type to object if of mixed types:
                return 'o'
        return typeStr
    @staticmethod
    def __GetType(val):
        if ColumnAttribute.IsInt(val) or ColumnAttribute.IsFloat(val):
            typeStr = 'float'
        elif ColumnAttribute.IsDatetime(val):
            typeStr = 'datetime'
        else:
            typeStr = 'o'
        return typeStr
        
    
class ColumnAttributeDiff:
    """
    * Object containing difference
    in name and meta-data between two columns.
    """
    def __init__(self):
        """
        * Instantiate new empty difference
        object.
        """
        self.__DefaultInitialize()

    #################
    # Properties:
    #################
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
    def HasDiff(self):
        return self.__hasdiff
    @property
    def Type(self):
        return self.__type
    @property
    def UniqueCount(self):
        return self.__uniqueCount
    @property
    def Uniques(self):
        return self.__uniques
    @ColumnName.setter
    def ColumnName(self, val):
        if not isinstance(val, str):
            raise Exception('ColumnName must be a string.')
        self.__colName = val
    @IsNullable.setter
    def IsNullable(self, val):
        if not isinstance(val, bool):
            raise Exception('IsNullable must be boolean.')
        self.__isNullable = val
    @IsUnique.setter
    def IsUnique(self, val):
        if not isinstance(val, bool):
            raise Exception('IsUnique must be boolean.')
        self.__isUnique = val
    @HasDiff.setter
    def HasDiff(self, val):
        if not isinstance(val, bool):
            raise Exception('HasDiff must be boolean.')
        self.__hasdiff = val
    @Type.setter
    def Type(self, val):
        if not isinstance(val, str):
            raise Exception('Type must be a string.')
        self.__type = val
    @UniqueCount.setter
    def UniqueCount(self, val):
        if not isinstance(val, (float, int)):
            raise Exception('UniqueCount must be numeric.')
        elif not int(val) >= 0:
            raise Exception('UniqueCount must be non-negative.')
        self.__uniqueCount = int(val)
    @Uniques.setter
    def Uniques(self, val):
        if not hasattr(val, '__iter__') and not isinstance(val, str) and not val is None:
            raise Exception('Uniques must be an iterable or None.')
        self.__uniques = set(val) if not val is None else val
    #################
    # Interface Methods:
    #################
    @classmethod
    def GetDifference(cls, lhs, rhs):
        """
        * Generate ColumnAttributeDiff object detailing 
        differences between another column.
        """
        ColumnAttributeDiff.__Validate(lhs, rhs)
        diff = ColumnAttributeDiff()
        properties = [attr for attr in dir(lhs) if not attr.startswith('_') and not callable(getattr(lhs, attr))]
        for prop in properties:
            if ColumnAttributeDiff.__CompareAttr(prop, lhs, rhs):
                setattr(diff, prop, getattr(rhs, prop))
                diff.HasDiff = True
        return diff
    #################
    # Private Helpers:
    #################
    @staticmethod
    def __Validate(lhs, rhs):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(lhs, ColumnAttribute):
            errs.append('lhs must be a ColumnAttribute.')
        if not isinstance(rhs, ColumnAttribute):
            errs.append('rhs must be a ColumnAttribute.')
        if errs:
            raise Exception('\n'.join(errs))

    def __DefaultInitialize(self):
        """
        * Default initialize object.
        """
        self.__colName = None
        self.__hasdiff = False
        self.__isNullable = None
        self.__isUnique = None
        self.__type = None
        self.__uniqueCount = None
        self.__uniques = None

    @staticmethod
    def __CompareAttr(prop, lhs, rhs):
        """
        * Handle comparison of ColumnAttribute
        properties, based upon type.
        """
        lhsAttr = getattr(lhs, prop)
        rhsAttr = getattr(rhs, prop)
        if (lhsAttr is None) ^ (rhsAttr is None):
            return False
        elif lhsAttr is None and rhsAttr is None:
            return True
        elif hasattr(lhsAttr, '__iter__') and not isinstance(lhsAttr, str):
            if len(lhsAttr) != len(rhsAttr):
                return False
            elif set(lhsAttr) != set(rhsAttr):
                return False
            return True
        return lhsAttr != rhsAttr
