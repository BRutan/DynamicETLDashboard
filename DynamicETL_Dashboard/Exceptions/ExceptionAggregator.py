##############################################################################
## ExceptionAggregator.py
##############################################################################
## Description:
## * Object stores and converts fatal and non-fatal exceptions to error messages, and
## delegates responsibility of contents to log file.
##  ***********
##  Note that these objects are portable to any application that implements custom exceptions, 
##  that only require the base class to copy the NonFatal and Fatal paradigm and implement 
##  same methods listed in this implementation.

import Exceptions.Fatal as Fatals
import Exceptions.NonFatal as NonFatals
import Exceptions.ExceptionContainerType as Container
import datetime
from LogFile import LogFile
import sys

__all__ = [ 'ExceptionAggregator' ]

class ExceptionAggregator(NonFatals.NonFatal):
    " Used to store and convert non-fatal exceptions into messages and input into log file. "
    ##########################################################
    ## Constructors:
    ##########################################################
    def __init__(self, exceptAgg = None):
        """ Default constructor. Instantiate new aggregator to store all exception types.
        Inputs:
        * commandArgs: Expecting a CommandLineArgs object.
        * exceptAgg: Passing an ExceptionAggregator object will cause this object to copy the passed object.
        """
        # ExceptDict has the following mapping: { ExceptionType -> ExceptionObject }:
        self.__ExceptDict = {}
        self.Contents = exceptAgg

    ##########################################################
    ## Mutators:
    ##########################################################
    def Add(self, exception):
        """
        * Add exception to internal list. 
        Inputs:
        * exception: Expecting any derived Fatal or NonFatal exception.
        """
        if not isinstance(exception, Fatals.Fatal) and not isinstance(exception, NonFatals.NonFatal):
            raise ValueError("Exception must be a derived Fatal or NonFatal exception object.")

        # If an ExceptionAggregator was passed then merge and exit:
        if isinstance(exception, ExceptionAggregator):
            self.Merge(exception)
            return

        ######################
        # Add exception to internal container:
        ######################
        if self.IsContainerType(exception) and type(exception) not in self.Contents.keys():
            # Add container type exception as element of dictionary:
            self.Contents[type(exception)] = exception
        elif self.IsContainerType(exception):
            # If exception is a ContainerType (fatal or nonfatal) then call that exception's Merge method to combined the two exceptions:
            self.Contents[type(exception)].Merge(exception)
        else:
            # Set or replace the non-container type exception:
            self.Contents[type(exception)] = exception
    
    def Merge(self, exceptionAgg):
        """
        * Merge passed ExceptionAggregator with this object. 
        Inputs:
        * exceptionAgg: Expecting an ExceptionAggregator object or None. If None is passed then will skip.
        """
        if exceptionAgg is None:
            return
        elif not isinstance(exceptionAgg, ExceptionAggregator):
            raise ValueError("exceptionAgg must be an ExceptionAggregator object.")

        ############################
        # Merge contents:
        ############################
        for exceptionType in exceptionAgg.Contents.keys():
            if ExceptionAggregator.IsContainerType(exceptionType):
                # Add the ContainerType exception:
                self.Add(exceptionAgg.Contents[exceptionType])
            else:
                # Replace or set non-container types:
                self.Add(item)
        
    ##########################################################
    ## Class Methods:
    ##########################################################
    def StdoutMessage(self):
        """
        * Convert all of stored exceptions into a single compact error message using each
        exceptions' Message() function.
        Outputs:
        * output: String detailing each concise error message.
        """
        message = ''
        # Return blank string if no strings loaded:
        if not self.HasErrors:
            return message
        ######################
        # Aggregate the error message:
        ######################
        for exceptType in self.Contents.keys():
            message += '%s\n' % self.Contents[exceptType].Message(False)
        
        return message
        
    def GenerateLogFile(self, valueDate, runTime, noLog = False):
        """
        * Return LogFile object and print all exceptions using contents of this object.
        Inputs:
        * valueDate: Expecting a date/datetime object.
        * runTime: Expecting a string.
        """
        # Ensure that value date is a datetime or date:
        noLog = (False if not isinstance(noLog, bool) else noLog)
        if not isinstance(valueDate, datetime.datetime) and not isinstance(valueDate, datetime.date):
            raise ValueError('valueDate must be a datetime or date object.')
        # Exit if no log file was specified on command line:
        if not self.HasErrors or noLog:
            return
        ######################
        # Generate Log File, Add all exceptions and print:
        ######################
        try:
            logFile = LogFile(valueDate, runTime)
            for exceptionType in self.Contents.keys():
                logFile.Append(self.Contents[exceptionType])

            # Print to stored path:
            logFile.Print()
        except Exception as err:
            # Append exception to stored list if failed to generate the log file:
            self.Add(NonFatals.LogFileFailed(callingFunc = '', logPath = logFile.Path, specific = err.message))

    def PrintExceptionScreen(self):
        """
        * Print all exception messages to screen with concise formatting.
        """
        if self.ErrorCount > 0:
            print('The following issues have occurred: ')
            print(self.StdoutMessage())
    
    @staticmethod
    def IsContainerType(exception):
        """
        * Indicate whether the exception is a container type or not:
        """
        return isinstance(exception, Container.ExceptionContainerType)

    ##########################################################
    ## Properties:
    ##########################################################
    @property
    def Contents(self):
        """
        * Return the internal Exception dictionary.
        """
        return self.__ExceptDict

    @property
    def HasErrors(self):
        """
        * Indicate if aggregator has any errors loaded.
        """
        return len(self.Contents.keys()) > 0

    @property
    def ErrorCount(self):
        """
        * Return the # of specific exceptions.
        """
        return len(self.Contents.keys())

    ##########################################################
    ## Mutators:
    ##########################################################
    @Contents.setter
    def Contents(self, container):
        """
        * Set the contents of this object. Note that this will reset the stored container.
        Inputs:
        * container: Expecting an ExceptionAggregator or None.
        """
        if container is None:
            return
        elif isinstance(container, ExceptionAggregator):            
            container = container.Contents
        else:
            raise ValueError("Contents must be an ExceptionAggregator object.")
        # Reset the container and merge all contents:
        self.__ExceptDict = {}
        self.Merge(container)