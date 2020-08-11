#####################################
# Reporter.py
#####################################
# Description:
# * Abstract base class to abstract away
# how report is written in derived classes.

from abc import ABC, abstractmethod
from pandas import DataFrame
import xlsxwriter

class Reporter(ABC):
    """
    * Abstract base class to abstract away
    how report is written in derived classes.
    """
    def __init__(self, path):
        pass