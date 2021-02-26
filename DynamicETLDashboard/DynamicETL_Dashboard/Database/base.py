#####################################
# Database/base.py
#####################################
# Description:
# * Abstract base class for database related
# objects.

from abc import ABC, abstractmethod

class DBInterface(ABC):
    """
    * Abstract base class for database
    related objects.
    """
    def __init__(self, connection):
        """
        * Store connection.
        """
        self.__connection = connection
        
    ##############
    # Properties:
    ##############
    @property
    def Connection(self):
        return self.__connection
    ##############
    # Interface Methods:
    ##############
    

