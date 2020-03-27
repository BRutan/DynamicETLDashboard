#####################################
# ETLDashboard.py
#####################################
# Definition:
# * GUI for ETL dashboard, contains all sub
# windows.

from ETLViewer import ETLViewer
from tkinter import Tk
from tkinter.ttk import Button, Checkbutton, Combobox, Frame, Entry, Label, PanedWindow, RadioButton, OptionMenu

class Dashboard(object):
    """
    * Dashboard for ETL addition and checking process.
    """
    def __init__(self):
        self.__window = Tk(className = 'Dashboard')
        self.__window.title("New ETL Deployer")
        self.__window.mainloop()
