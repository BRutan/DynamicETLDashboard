##############################################################################
## ExceptionContainerType.py
##############################################################################
## Description:
## * Interface for handling exceptions that can have multiple issues, to be used 
## in conjunction with Fatal/NonFatal base types and multiple inheritance.

from Exceptions.ETLDashboardExcept import ETLDashboardExcept
from abc import ABCMeta, abstractmethod
from datetime import datetime
from sortedcontainers import SortedList
import sys

__all__ = ['ExceptionContainerType']

class ExceptionContainerType(ETLDashboardExcept):
    """
    * Abstract Fatal exception base class that differentiates NonFatal exception to store list with multiple items.
    """
    __meta__ = ABCMeta
    ##########################################################
    ## Constructors/"Magic Methods":
    ##########################################################
    def __init__(self, callingFunc, specific = '', timestamp = datetime.now(), targetList = SortedList()):
        """
        * Overloaded Constructor.
        """
        if sys.version_info[0] > 2:
            super().__init__(callingFunc, specific, timestamp)
        else: 
            super(ExceptionContainerType, self).__init__(callingFunc, specific, timestamp)
        # Set the contents of the container:
        self.Contents = SortedList()
        self.Merge(targetList)
        
    def __contains__(self, item):
        """
        * Allow this class to use "in" operator.
        """
        try:
            self.Contents.index(item)
            return True
        except ValueError:
            return False

    ##########################################################
    ## Class Methods:
    ##########################################################
    def Message(self):
        """
        * Return string representation of all contents.
        """
        return ', '.join(self.Contents)
    
    def Merge(self, container):
        """
        * Merge passed list with this object.
        Inputs:
        * container: Can either be a list, SortedList, ExceptionContainerType object or None.
        """
        if isinstance(container, ExceptionContainerType):
            # Update this object's time stamp with earlier time stamp if necessary:
            self.TimeStamp = (container.TimeStamp if container.TimeStamp < self.TimeStamp else self.TimeStamp)
            container = container.Contents
        elif container is None:
            # Exit if no container was passed.
            return

        # Add all items and make the list unique:
        for item in container:
            self.Add(item)

    def TypesMatch(self, obj, derivedType = None):
        """
        * Raise ValueError if the passed object is not of necessary type.
        """
        match = (isinstance(obj, SortedList) or isinstance(obj, list) or obj is None)
        if not derivedType is None:
            match = match or isinstance(obj, derivedType)

        if not match:
            raise ValueError("Parameter must be list, SortedList, ExceptionContainerType or None.")
        
    def __Unique(self):
        """
        * Make the contents unique.
        """
        self.Contents = SortedList(set(self.Contents))

    ##########################################################
    ## Accessors:
    ##########################################################
    @property
    def Contents(self):
        """
        * Return the stored content.
        """
        return self.__Contents

    @property
    def ErrorCount(self):
        """
        * Return # of errors in container.
        """
        return len(self.Contents)

    @property
    def HasErrors(self):
        """
        * Return boolean indicating that the object has errors.
        """
        return len(self.__Contents) > 0

    ##########################################################
    ## Mutators:
    ##########################################################
    @Contents.setter
    def Contents(self, container):
        """
        * Set the content list. Will cause the original container to be replaced by new container.
        Inputs:
        * container: Expecting a list, ExceptionContainerType or SortedList.
        """
        # Raise exception if type does not match:
        self.TypesMatch(container)
        if isinstance(container, ExceptionContainerType):
            # Get the stored internal container from the passed ExceptionContainerType object:
            container = container.Contents
        # Reset the internal container and merge with new container:
        self.__Contents = SortedList()
        self.Merge(container)

    def Add(self, value):
        """
        * Add only unique strings or tuples with unique primary keys to the container.
        """
        if isinstance(value, str) and value and value not in self.Contents:
            self.Contents.add(value)
        elif isinstance(value, tuple) and value[0] not in [first for first, second in self.Contents]:
            self.Contents.add(value)