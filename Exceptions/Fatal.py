##############################################################################
## Fatal.py
##############################################################################
## Description:
## * Define all fatal exceptions that will cause application to print issue
## to command line and close if encountered

from abc import ABCMeta, abstractmethod
from datetime import datetime
from Exceptions.ETLDashboardExcept import ETLDashboardExcept as base
import Exceptions.ExceptionContainerType as Container
from sortedcontainers import SortedList
import sys

__all__ = [ 'CommandLineErrors', 'ConfigFilesMissing', 'Fatal' ]

########################################################################################################
# Base Classes:
########################################################################################################
class Fatal(base):
    " Abstract base class, exception has ability to print issue to stdout and close application immediately. "
    # Make this class an abstract for other more precise fatal errors to inherit:
    __metaclass__ = ABCMeta
    def __init__(self, callingFunc, specific, timestamp = datetime.now()):
        " Overloaded constructor. "
        if sys.version_info[0] > 2:
            super().__init__(callingFunc, specific, timestamp)
        else:
            super(Fatal, self).__init__(callingFunc, specific, timestamp)

    def MessageHeader(self):
        " Return fatal message header for printing to stdout. "
        outmessage = "####################################################################"
        outmessage += "\n# Fatal Error: "
        outmessage += "\n####################################################################"

        return outmessage

    def HandleAndExit(self, granular = False):
        """
        * Print message to stdout and exit application immediately. 
        Inputs:
        * granular: Expecting a boolean that will determine the detail of the message.
        """
        granular = (False if not isinstance(granular, bool) else granular)
        print(self.MessageHeader())
        print(self.Message(granular))
        print('Exiting application.')

########################################################################################################
# Derived Classes:
########################################################################################################
class CommandLineErrors(Container.ExceptionContainerType, Fatal):
    " Exception indicates issues that occurred with command line inputs. "
    __Concise = '%d command line errors were improperly passed.'
    __Granular = ''
    ##########################################################
    ## Constructors:
    ##########################################################
    def __init__(self, callingFunc, cmdLineInput = '', invalidArgs = SortedList(), specific = '', timestamp = datetime.now(), targetList = SortedList()):
        " Overloaded constructor. "
        if sys.version_info[0] > 2:
            super().__init__(callingFunc, specific, timestamp, targetList)
        else:
            super(CommandLineErrors, self).__init__(callingFunc, specific, timestamp, targetList)
        
        self.InvalidArgs = invalidArgs
        self.Add(cmdLineInput)

    ##########################################################
    ## Properties:
    ##########################################################
    @property
    def HasErrors(self):
        " Return indicator that errors occurred. "
        return ((super() if sys.version_info[0] > 2 else super(CommandLineErrors, self)).HasErrors or len(self.InvalidArgs) > 0)
        
    @property
    def InvalidArgs(self):
        " Return access to the invalid arguments list."
        return self.__invalid

    ##########################################################
    ## Mutators:
    ##########################################################    
    @InvalidArgs.setter
    def InvalidArgs(self, invalidArgs):
        """
        * Set the invalid arguments list.
        Inputs:
        * invalidArgs: Expecting list, SortedList or None. If None passed then will 
        instantiate property to SortedList.
        """
        if isinstance(invalidArgs, list):
            self.__invalid = SortedList(invalidArgs)
        elif isinstance(invalidArgs, SortedList):
            self.__invalid = invalidArgs
        elif invalidArgs is None:
            self.__invalid = SortedList()    
        else:
            raise ValueError('InvalidArgs must be list, SortedList or None.')

    ##########################################################
    ## Class Methods:
    ##########################################################    
    def Message(self, granular = False):
        """
        * Return the granular or concise message.
        """
        if granular:
            granular = ''
            if len(self.Contents) > 0:
                granular = "The following command line errors were improperly set: \n{ %s }" % (',\n'.join([arg + ' : ' + msg for arg, msg in self.Contents]))
            # Include information about invalid arguments being passed if any occurred:
            if len(self.InvalidArgs) > 0:
                granular += ('\n' if granular else '') + 'The following arguments are invalid: \n{ %s }' % (','.join(self.InvalidArgs))
            return granular
        else:
            return CommandLineErrors.__Concise % self.ErrorCount()

    def Merge(self, cmdLineErrors):
        """
        * Merge the passed container/CommandLineErrors object.
        """
        # Below method will throw exception if type is incorrect:
        self.TypesMatch(cmdLineErrors, CommandLineErrors)
        # Merge the passed container:
        if sys.version_info[0] > 2:
            super().Merge(cmdLineErrors)
        else:
            super(CommandLineErrors, self).Merge(cmdLineErrors)
        
class ConfigFilesMissing(Container.ExceptionContainerType, Fatal):
    " Exception indicates that configuration files were missing. "
    __Concise = '%d configuration files could not be loaded.'
    __Granular = 'The following {Configuration Files : Path } could not be loaded:\n%s'
    ##########################################################
    ## Constructors:
    ##########################################################
    def __init__(self, callingFunc, fileName = '', filePath = '', specific = '', timestamp = datetime.now(), targetList = SortedList()):
        """
        * Overloaded constructor.
        Inputs:
        * callingFunc: Expecting string.
        * fileName: Name of missing configuration file.
        * filePath: Path to missing configuration file.
        * configPaths: Expecting a list of tuples of { Name, Path }.
        * 
        """
        if sys.version_info[0] > 2:
            super().__init__(callingFunc, specific, timestamp, targetList)
        else:
            super(ConfigFilesMissing, self).__init__(callingFunc, specific, timestamp, targetList)

        self.MissingConfigs = SortedList()
        if fileName and filePath:
            self.MissingConfigs.add((fileName, filePath))

    ##########################################################
    ## Properties:
    ##########################################################        
    @property
    def MissingConfigs(self):
        """
        * Return access to the container containing ConfigPaths.
        """
        return self.__MissingConfigs

    @property
    def HasErrors(self):
        """
        * Indicate that this exception object has errors.
        """
        return self.ErrorCount() > 0

    def ErrorCount(self):
        """
        * Return number of errors this exception contains.
        """
        return len(self.MissingConfigs) + len(self.Contents)

    ##########################################################
    ## Mutators:
    ##########################################################        
    @MissingConfigs.setter
    def MissingConfigs(self, missingConfigs):
        """
        * Set the configuration paths member, containing all configuration paths that were missing.
        Note that calling this setter will reset the member.
        Inputs:
        * pathList: Expecting a list or SortedList.    
        """
        # Throw exception if pathList doesn't match container type:
        self.TypesMatch(missingConfigs, ConfigFilesMissing)
        # Reset the configuration paths (using SortedList) and add all missing paths:
        self.__MissingConfigs = SortedList()
        self.__MergeMissingConfigs(missingConfigs)

    def __MergeMissingConfigs(self, missingConfigs):
        """
        * Merge all string elements of configPaths with this object's member.
        Inputs:
        * configPaths: Expecting a list, SortedList or ConfigFilesMissing object.    
        """
        if isinstance(missingConfigs, ConfigFilesMissing):
            # Merge using the ConfigPaths member:
            self.__MergeMissingConfigs(missingConfigs.MissingConfigs)
        else:
            # Below function will throw ValueError if type is not appropriate:
            self.TypesMatch(missingConfigs, ConfigFilesMissing)
            for name, path in missingConfigs:
                # Add only unique strings to the path list:
                if isinstance(name, str) and name and name not in [name for name, path in self.__MissingConfigs]:
                    self.__MissingConfigs.add((name, path))

    ##########################################################
    ## Class Methods:
    ##########################################################    
    def Merge(self, configFilesMissing):
        """
        * Before merging the main container, merge the auxilliary configuration file paths container.
        Inputs:
        * configFilesMissing: Expecting a ConfigFilesMissing object.
        """
        # Below method will throw exception if type does not match:
        self.TypesMatch(configFilesMissing, ConfigFilesMissing)
        # Merge the configuration filepath container if ConfigFilesMissing was passed:
        if isinstance(configFilesMissing, ConfigFilesMissing):
            self.__MergeMissingConfigs(configFilesMissing)
        # Merge main container stored in base class:
        if sys.version_info[0] > 2:
            super().Merge(configFilesMissing)
        else:
            super(ConfigFilesMissing, self).Merge(configFilesMissing)

    def Message(self, granular = False):
        """
        * Return the granular or concise message.
        """
        if granular:
            return ConfigFilesMissing.__Granular % (',\n'.join([name + ':\n' + path for name, path in self.MissingConfigs if name and path]))
        else:
            return ConfigFilesMissing.__Concise % self.ErrorCount()