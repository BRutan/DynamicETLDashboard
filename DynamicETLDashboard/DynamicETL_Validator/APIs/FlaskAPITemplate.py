#####################################
# FlaskAPITemplate.py
#####################################
# Description:
# * Abstract base class that defines
# common functionality interface to convert
# groups of Flask classes into xml templates
# that can be used to create Flask python applications.

from abc import ABC, abstractmethod, abstractproperty
from bs4 import BeautifulSoup as Soup

class FlaskAPITemplate(ABC):
    """
    * Abstract base class that defines
    common functionality interface to convert
    groups of Flask classes into xml templates
    that can be used to create Flask python applications.
    """
    def __init__(self):
        pass

