#####################################
# ColumnRelationships.py
#####################################
# Description:
# * Map all relationships between columns.

from abc import abstractmethod, ABC
from enum import Enum
from itertools import combinations, product
import numpy as np
from pandas import DataFrame
from sortedcontainers import SortedDict

class ColumnRelationships(object):
    """
    * Immutable object that store relationships between columns.
    """
    def __init__(self, data):
        """
        Inputs:
        * data: Expecting dataframe of columns.
        """
        self.__relationships = ColumnRelationships.MapRelationships(data)
    ###############
    # Properties:
    ###############
    @property
    def Relationships(self):
        return self.__relationships
    ###############
    # Interface Methods:
    ###############
    def ToDataFrame(self, countinfo = True):
        """
        * Return full symmetric dataframe matrix as representation of object.
        Inputs:
        * countinfo: If True, fills cell with "leftcount_rightcount", else fills with
        full relationship name, ex "one_one".
        """
        return ColumnRelationships.__AsDataFrame(self.__relationships, countinfo)

    @classmethod
    def MapRelationships(cls, data):
        """
        * Map all relationships between columns in passed data.
        Inputs:
        * data: Expecting dataframe of columns.
        Output:
        * Returns lower triangular Dataframe of relationships with columns as dimensions.
        """
        results = SortedDict({col : {} for col in data.columns})
        combs = combinations(data.columns, 2)
        for comb in combs:
            if comb[0] != comb[1]:
                results[comb[0]][comb[1]] = cls.__MapRelationships(data, comb[0], comb[1])
        return results
    ###############
    # Private Helpers:
    ###############
    @classmethod
    def __MapRelationships(cls, data, col1, col2):
        """
        * Determine if one-to-one/one-to-many/many-to-one/many-to-many relationship exists
        between columns.
        """
        left_max = data[[col1, col2]].groupby(col1).count().max()[0]
        right_max = data[[col1, col2]].groupby(col2).count().max()[0]
        left_uniq_count = len(data[col1].drop_duplicates())
        right_uniq_count = len(data[col2].drop_duplicates())
        if left_max==1:
            if right_max==1:
                enum = RelationshipEnum.ONE_TO_ONE 
            else:
                enum = RelationshipEnum.ONE_TO_MANY
        else:
            if right_max==1:
                enum = RelationshipEnum.MANY_TO_ONE
            else:
                enum = RelationshipEnum.MANY_TO_MANY
        return ColumnRelationship(enum, left_uniq_count, right_uniq_count)

    @staticmethod
    def __AsDataFrame(data, countinfo):
        """
        * Convert stored dictionary into symmetric DataFrame.
        """
        sorted_keys = sorted(list(data.keys()))
        newdata = SortedDict()
        for key in sorted_keys:
            newdata[key] = SortedDict()
            for subkey in sorted_keys:
                if subkey == key:
                    newdata[key][subkey] = '='
                elif subkey not in data[key]:
                    newdata[key][subkey] = str(data[subkey][key].Reverse()) if countinfo == True else data[subkey][key].Reverse().TypeStr
                else:
                    newdata[key][subkey] = str(data[key][subkey]) if countinfo == True else data[key][subkey]
        
        return DataFrame(newdata)

class RelationshipEnum(Enum):
    ONE_TO_ONE = 0
    ONE_TO_MANY = 1
    MANY_TO_ONE = 2
    MANY_TO_MANY = 3

class ColumnRelationship:
    """
    * Immutable class representing relationship
    between two columns.
    """
    __countstr = '%d_%d'
    __typestrs = { RelationshipEnum.ONE_TO_ONE: "one_one",
                            RelationshipEnum.ONE_TO_MANY : "one_many",
                            RelationshipEnum.MANY_TO_MANY : "many_many", 
                            RelationshipEnum.MANY_TO_ONE : "many_one" }
    def __init__(self, enum, leftcount, rightcount):
        self.__type = enum
        self.__leftcount = leftcount
        self.__rightcount = rightcount
    def __eq__(self, val):
        return self.__type == val.Type
    def __str__(self):
        """
        * Return TypeStr by default.
        """
        return self.TypeStr
    #############
    # Properties:
    #############
    @property
    def CountStr(self):
        """
        * Return string of form "<KeyUniqueCount>_<ValueUniqueCount>".
        """
        return self.__countstr % (self.__leftcount, self.__rightcount)
    @property
    def Type(self):
        """
        * Return relationship name string, ex: "one_one".
        """
        return self.__type
    @property
    def TypeStr(self):
        return ColumnRelationship.__typestrs[self.__type]
    ##############
    # Interface Methods:
    ##############
    def Reverse(self):
        """
        * Return new relationship with reversed characteristics.
        """
        enum = self.__type
        if self.__type == RelationshipEnum.MANY_TO_ONE:
            enum = RelationshipEnum.ONE_TO_MANY
        elif self.__type == RelationshipEnum.ONE_TO_MANY:
            enum = RelationshipEnum.MANY_TO_ONE
        return ColumnRelationship(enum, self.__rightcount, self.__leftcount) 