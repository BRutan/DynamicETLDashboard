#####################################
# ScriptWindow.py
#####################################
# Description:
# * Abstract base class for windows
# used to execute scripts.

from abc import ABC, abstractmethod
import tkinter as tk
from tkinter.ttk import Button, Checkbutton, Combobox, Frame, Entry, Label, PanedWindow, RadioButton, OptionMenu


class ScriptWindow(ABC):
    """
    * 
    """
    def __init__(self, name):
        """
        * Initialize script window 
        """
        ScriptWindow.__Validate(name)
        self.__name = name
        self.__buttons = {}
        self.__checkbutons = {}
        self.__comboboxes = {}
        self.__frames = {}
        self.__entries = {}
        self.__labels = {}
        self.__panedwindows = {}
        self.__radiobuttons = {}
        self.__optionmenus = {}

    ##################
    # Interface Methods:
    ##################


    ##################
    # Private Helpers:
    ##################
    @abstractmethod
    def __Validate(name):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(name, str):
            errs.append('name must be a string.')
        if errs:
            raise Exception('\n'.join(errs))



