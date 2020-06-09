#####################################
# ETLDashboard.py
#####################################
# Definition:
# * GUI for ETL dashboard, contains all sub
# windows.

from tkinter import Tk
from tkinter.ttk import Notebook
from GUI.ETLInfoWindow import ETLInfoWindow
from GUI.TestETLPipelineWindow import TestETLPipelineWindow

class ETLDashboard(object):
    """
    * Main window grouping for script windows.
    """
    def __init__(self, windows):
        self.__etldashboard = Tk("ETLDashboard", "ETLDashboard", "ETLDashboard")
        self.__windows = Notebook(self.__etldashboard)
        self.__testetlpipelinewindow = TestETLPipelineWindow()
        self.__etlinfowindow  = ETLInfoWindow()

        self.__window.title("ETLDashboard")
        self.__window.mainloop()
    ##############
    # Interface Methods:
    ##############

    ##############
    # Private Helpers:
    ##############


