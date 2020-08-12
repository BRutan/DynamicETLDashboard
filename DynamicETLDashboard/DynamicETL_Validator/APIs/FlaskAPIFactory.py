#####################################
# FlaskAPIFactory.py
#####################################
# Description:
# * Validate flask startup arguments, 
# register all controllers function and
# blueprints, manage dependency injection
# and run Flask application dynamically.

# https://stackoverflow.com/questions/19261833/what-is-an-endpoint-in-flask
# https://flask.palletsprojects.com/en/1.1.x/api/
# https://flask.palletsprojects.com/en/1.1.x/config/
# https://www.digitalocean.com/community/tutorials/how-to-make-a-web-application-using-flask-in-python-3
# https://flask.palletsprojects.com/en/1.1.x/api/#url-route-registrations
# https://stackoverflow.com/questions/3523028/valid-characters-of-a-hostname

from flask import Blueprint, Flask, url_for
from flask_injector import FlaskInjector
import json
import re
import socket
import sys

class FlaskAPIFactory(object):
    """
    * Validate flask startup arguments, 
    register all controllers function and
    blueprints, manage dependency injection
    and run Flask application dynamically.
    """
    __app = None
    __defaultTemplate = {}
    __invalidNames = set(['flask'])
    __invalidNamesREs = [re.compile(name, re.IGNORECASE) for name in __invalidNames]
    __urlPattern = r'http://.+'
    __urlRE = re.compile(__urlPattern)
    def __init__(self, appname, hostname = '127.0.0.1', port = 5000, debug = True, injector = None):
        """
        * Create new factory object where controllers will later
        be added.
        """
        FlaskAPIFactory.__Validate(appname, hostname, port, debug, injector)
        self.__SetProperties(appname, hostname, port, debug, injector)

    ####################
    # Properties:
    ####################
    @property
    def BaseURL(self):
        return self.__baseurl
    @property
    def BlueprintNames(self):
        return self.__blueprints
    @property
    def Debug(self):
        return self.__debug
    @property
    def Name(self):
        return self.__name
    @property
    def URLs(self):
        return self.__urls

    ####################
    # Interface Methods:
    ####################
    def Run(self):
        """
        * Set up dependency injector if used, and 
        run the Flask app with all configured endpoints/blueprints.
        """
        # Set injector if used:
        kwargs = { 'host' : self.__hostname, 'port' : self.__port, 'debug' : self.__debug }
        #kwargs = { 'port' : self.__port, 'debug' : self.__debug }
        FlaskAPIFactory.__app.run(**kwargs)

    def AddEndpoint(self, func, endpoint, route, injection = None, handler = None, **options):
        """
        * Add endpoint to Flask app.
        Inputs:
        * func: callable that will be used as endpoint in Flask app.
        * route: 
        """
        errs = []
        if not callable(func):
            errs.append('func must be callable.')
        if not isinstance(endpoint, str):
            errs.append('endpoint must be a string.')
        if not isinstance(route, str):
            errs.append('route must be a string.')
        elif not route.startswith('/'):
            errs.append('route must start with forward slash.')
        elif self.__HasURL(endpoint, route):
            errs.append('route has already been used.')
        if not injection is None and not (not isinstance(injection, str) and hasattr('__iter__', injection)):
            errs.append('injection must be None or an iterable of injector.inject objects.')
        if not handler is None and not callable(handler):
            errs.append('handler must be callable if provided.')
        if errs:
            raise ValueError('\n'.join(errs))
        kwargs = { 'rule' : route, 'endpoint' : endpoint, 'view_func' : func }
        #if not handler is None:
        #    kwargs['handler'] = EndpointAction(handler)

        FlaskAPIFactory.__app.add_url_rule(**kwargs, **options)
        self.__UpdateUrls(endpoint, route)

    def RegisterBlueprint(self, bp, prefix = None, url_defaults = None):
        """
        * Register blueprint to Flask app.
        """
        errs = []
        if not isinstance(bp, Blueprint):
            errs.append('bp must be a Flask Blueprint object.')
        if not prefix is None and not isinstance(prefix, str):
            errs.append('prefix must be a string if provided.')
        if not url_defaults  is None:
            pass
        if errs:
            raise ValueError('\n'.join(errs))
        kwargs = { 'blueprint' : bp }
        kwargs['url_prefix'] = prefix

        FlaskAPIFactory.__app.register_blueprint(**kwargs)

    ####################
    # Static Helpers:
    ####################
    @classmethod
    def BuildFromTemplate(cls, templatepath):
        """
        * Build Flask API dynamically using predefined
        template.
        Inputs:
        * templatepath: string path to template.
        """
        pass

    @classmethod
    def ConvertModuleToTemplate(cls):
        """
        * Convert python module where
        Flask application has been defined
        into a json template that can be read
        and/or changed.
        """
        pass

    @classmethod
    def DefaultTemplate(cls):
        """
        * Output default template object that can
        be printed to file and filled in.
        """
        pass

    @classmethod
    def IsValidAppName(cls, appname, errs):
        """
        * Test to see if passed appname can be used to 
        identify Flask app.
        Inputs:
        * appname: string name of application.
        Optional:
        * errs: list to store error messages or None.
        """
        if not errs is None and not isinstance(errs, list):
            raise ValueError('errs must be a list or None.')
        if not isinstance(appname, str):
            if not errs is None:
                errs.append('appname must be a string.')
            return False
        elif any([not pat.match(appname) is None for pat in FlaskAPIFactory.__invalidNamesREs]):
            if not errs is None:
                errs.append('appname cannot be one of %s.' % ','.join(FlaskAPIFactory. __invalidNames))
            return False
        return True   

    @classmethod
    def IsValidHostName(cls, hostname, errs = None):
        """
        * Checks to see that hostname is valid and 
        can be used with Flask.
        """
        pass
    
    @classmethod
    def IsValidUrl(cls, url, errs = None):
        """
        * Test to see if passed url is valid.
        Inputs:
        * url: string url.
        Optional:
        * errs: list to store error messages or None.
        """
        if not errs is None and not isinstance(errs, list):
            raise ValueError('errs must be a list or None.')
        if not isinstance(url, str):
            if not errs is None:
                errs.append('url must be a string.')
            return False
        elif not FlaskAPIFactory.__urlRE.match(url):
            if not errs is None:
                errs.append('url must match %s.' % FlaskAPIFactory.__urlPattern)
            return False
        return True

    ####################
    # Private Helpers:
    ####################
    def __SetProperties(self, appname, hostname, port, debug, injector):
        """
        * Set object properties using constructor parameters.
        """
        self.__appname = appname
        self.__hostname = hostname
        self.__baseurl = 'http://%s/' % self.__hostname
        self.__debug = debug
        self.__injector = injector
        self.__port = port
        self.__urls = set()
        # Start Flask app:
        FlaskAPIFactory.__app = Flask(appname)
        
    def __HasURL(self, endpoint, route):
        """
        * Check that route has not already been added.
        """
        url = r'%s%s%s' % (self.__baseurl, endpoint, route)
        return url in self.__urls

    def __UpdateUrls(self, endpoint, route):
        """
        * Add url to stored URLs to keep
        track of which urls the Flask application can use.
        """
        self.__urls.add(r'%s\%s%s' % (self.__baseurl, endpoint, route))

    @staticmethod
    def __Validate(appname, hostname, port, debug, injector):
        """
        * Validate all constructor parameters.
        """
        errs = []
        if not FlaskAPIFactory.IsValidAppName(appname, errs):
            pass
        #if not FlaskAPIFactory.IsValidUrl(hostname, errs):
        #    pass
        if not isinstance(debug, bool):
            errs.append('debug must be boolean.')
        if not injector is None and not callable(injector):
            errs.append('injector must be callable if provided.')
        if errs:
            raise ValueError('\n'.join(errs))