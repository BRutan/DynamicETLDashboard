##############################################################################
## LogFile.py
##############################################################################
## Description:
## * File containing all errors that is output at application conclusion.
## Note: To make this object portable to other projects, just need to have Fatal and NonFatal
## modules that define pertinent custom exceptions that implement all called functions
## in this current module.

from sortedcontainers import SortedList
from DirectoryTypes.FileType import FileType
import Exceptions.Fatal as Fatals
import Exceptions.NonFatal as NonFatals
import Exceptions.MerlinPlottingExcept as base
from getpass import getuser
import os

__all__ = [ 'LogFile' ]

class LogFile(FileType):
    ##########################################################
    ## Static Variables:
    ##########################################################
    # All header column names to be output into file:
    __HeaderLines = ['Username:', 'Message:', 'Calling Function:', 'TimeStamp:']
    # Store username (only one user per application):
    __User = getuser()
    # Set the maximum message length:
    __MessageBufferLength = 105
    # Used to output each line entry:
    __LineFormatStringRaw = '{0:<20}{1:<%d}{2:<60}{3:<20}'
    ##########################################################
    ## Constructors:
    ##########################################################
    def __init__(self, valueDate, runTime, filePath = ''):
        " Overloaded constructor. Instantiate a log file. "
        if not isinstance(filePath, str):
            raise ValueError('filePath must be a string.')
        elif filePath == '':
            # Replace default with current working directory:
            filePath = self.AppendHyphenIfNecessary(os.getcwd()) + 'LogFile/MerlinPlotting LogFile.txt'
        # Output log file into new file if > 10 MB:
        filePath = self.HandleLargeFiles(filePath)
        # Create the target folder if does not exist:
        self.CreateFolderIfDoesNotExist(self.ExtractFolderName(filePath))
        self.Path = filePath
        # Fatal and non-fatal error dictionaries to organize exceptions, using time stamp as primary key for each sub-dictionary:
        self.Errors = { Fatals.Fatal : SortedList(), NonFatals.NonFatal : SortedList() }
        self.__LineFormatString = LogFile.__LineFormatStringRaw % (LogFile.__MessageBufferLength + 5)

    ##########################################################
    ## Accessors:
    ##########################################################
    def ErrorCount(self):
        """
        * Return # of exceptions in log file.
        """
        return len(self.Errors[Fatals.Fatal]) + len(self.Errors[NonFatals.NonFatal])

    ##########################################################
    ## Public Mutators:
    ##########################################################
    def Append(self, exception):
        """
        * Append fatal exception to log file. 
        Inputs:
        * exception: Expecting a Fatal or NonFatal error.
        """
        # Ensure that passed error was a fatal one:
        if not isinstance(exception, Fatals.Fatal) and not isinstance(exception, NonFatals.NonFatal):
            raise ValueError('Please only pass Fatal/NonFatal exceptions.')
        elif isinstance(exception, Fatals.Fatal):
            # Append Fatal error:
            self.Errors[Fatals.Fatal].add(exception)
        else:
            # Append NonFatal error:
            self.Errors[NonFatals.NonFatal].add(exception)

    ##########################################################
    ## Class Methods:
    ##########################################################
    def Print(self):
        """
        * Print contents of log file to output path. 
        """
        ########################
        # Skip if no errors loaded:
        ########################
        allLines = []
        prevLines = []
        # Write headers:
        allLines.extend(self.__Headers())

        if self.ErrorCount == 0:
            return
        elif self.CheckPath(self.Path):
            # Store all current lines except header in list to prepend in descending chronological order:
            with open(self.Path, 'r') as f:
                prevLines = f.readlines()
                # Get all lines except headers, and remove newline characters:
                prevLines = [line.replace('\n', '') for line in (prevLines[2:len(prevLines)] if len(prevLines) > 2 else [])] 

        # Output to log file (order is { Date+Time, User, Message, CallingFunction }:
        try:
            with open(self.Path, 'w') as file:
                ########################
                # Print all contents to file:
                ########################
                for exceptType in self.Errors.keys():
                    for currExcept in self.Errors[exceptType]:
                        # Split long messages over multiple lines, repeating all other columns:
                        allLines.extend(self.__FormattedMessages(currExcept, True))

                # Add all previous lines at end:
                allLines.extend(prevLines)
                # Need to append newline to each line before printing:
                allLines = [line + '\n' for line in allLines]
                file.writelines(allLines)

        except Exception as err:
            # Raise non fatal exception if occurred:
            raise NonFatals.LogFileFailed("LogFile::Print()", logPath = self.Path, specific = err.message)

    def __FormattedMessages(self, exception, granular = True):
        """
        * Return formatted message using passed fatal/non-fatal exception.
        Inputs:
        * exception: Expecting any derived MerlinPlottingExcept.
        * granular: True if want the "granular" message from the exception.
        """
        messages = []
        # Return blank list if exception is not derived from MerlinPlottingExcept:
        if not isinstance(exception, base.MerlinPlottingExcept):
            return messages
    
        # Set granular to default if anything other than boolean passed:
        if not isinstance(granular, bool):
            granular = True        

        messageChunks = self.__MessageChunks(exception.Message(granular))

        # Remove blank message chunks:
        messageChunks = [message for message in messageChunks if message]

        for message in messageChunks:
            messages.append(self.__LineFormatString.format(LogFile.__User, message, exception.CallingFunction, exception.TimeStampToStr(True))) 

        return messages

    def __Headers(self):
        """
        * Return string containing headers with correct spacing.
        """        
        # Clear out the old file and write header columns:
        output = []
        output.append(self.__LineFormatString.format(LogFile.__HeaderLines[0], LogFile.__HeaderLines[1], LogFile.__HeaderLines[2], LogFile.__HeaderLines[3])) 
        output.append('-' * 250)

        return output

    def __MessageChunks(self, message):
        """
        * Return list containing message split over multiple lines if necessary in order to fit into "Message" column.
        in log file.
        Inputs:
        * message: Expecting a string.
        Outputs:
        * chunks: List containing string divided into chunks.
        """
        # Divide message into multiple chunks to spread over multiple lines:
        chunkSize = LogFile.__MessageBufferLength
        length = len(message)
        output = []
        # Replace any tabs present in message with spaces:
        message = message.replace('\t', '')
        # First split message wherever newlines appear:        
        chunks = message.split('\n')
        # Split each submessage into chunks using the buffer length:
        for chunk in chunks:
            output.extend([chunk[index:(index + chunkSize if index + chunkSize < length else length)] for index in range(0, length, chunkSize)])

        return output