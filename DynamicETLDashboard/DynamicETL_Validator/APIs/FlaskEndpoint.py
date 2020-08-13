#####################################
# FlaskEndpoint.py
#####################################
# Description:
# * Wrapper class for required and 
# properties necessary to create Flask endpoints.
# Can read and write to xml templates.

from APIs.APITemplate import APITemplate

class FlaskEndpoint(FlaskAPITemplate):
    """
    * Wrapper class for required and optional
    properties necessary to create Flask endpoints.
    Can read and write from/to xml templates.
    """
    def __init__(self, route):
        pass