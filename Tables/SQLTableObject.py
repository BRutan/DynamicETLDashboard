#####################################
# SQLTableObject.py
#####################################
# Definition:
# * Table object that uses common
# SQL functionality and attributes.

from abc import ABC, abstractmethod
from Tables.TableObject import TableObject

class SQLTableObject(TableObject):
    """
    * Table object that uses common
    SQL functionality and attributes.
    """
    def __init__(self, name, schema):
        """
        * Table object that uses common
        SQL functionality and attributes.
        """
        pass

    ###############
    # Properties:
    ###############
    @property
    def Schema(self):
        return self.__schema
    