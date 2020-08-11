#####################################
# ETLViewer.py
#####################################
# Description:
# * Window for viewing information about
# existing ETLs. Uses data generated from
# GenerateETLInfo script.

from GUI.ScriptWindow import ScriptWindow
from GenerateETLInfo import GenerateETLInfo

class ETLInfoWindow(ScriptWindow):
    
    def __init__(self):
        """
        * Initialize window for getting ETL
        information.
        """
        super().__init__("ETLInfo")
