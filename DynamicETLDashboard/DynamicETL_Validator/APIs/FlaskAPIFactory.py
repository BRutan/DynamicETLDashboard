#####################################
# FlaskAPIFactory.py
#####################################
# Description:
# * Validate flask startup arguments, 
# register all controllers function and
# blueprints, manage dependency injection
# and run Flask application dynamically.

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
    __moduleType = type(sys.modules['sys'])
    __urlPattern = r'http://.+'
    __urlRE = re.compile(__urlPattern)
    __validMethods = set(['get', 'post', 'put'])
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
    def EndpointNames(self):
        return self.__endpointnames
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
        * Run the Flask app with all configured endpoints/blueprints,
        and setup FlaskInjector if used.
        """
        # Set injector if used:
        if self.__shouldinject:
            injector = FlaskInjection(**self.__injection).init_app(FlaskAPIFactory.__app)
        kwargs = { 'host' : self.__hostname, 'port' : self.__port, 'debug' : self.__debug }
        # Run Flask application:
        FlaskAPIFactory.__app.run(**kwargs)

    def AddEndpoint(self, func, name, route, methods, inject = False, handler = None, **options):
        """
        * Add endpoint to Flask app.
        Inputs:
        * func: callable that will be used as endpoint in Flask app.
        * name: string name of endpoint, just for identification purposes. Cannot
        have already been used.
        * route: string route to endpoint. Must not have already been used.
        * methods: REST methods to use with endpoint. Must satisfy IsValidRESTMethod().
        Optional:
        * inject: must be a boolean indicating that function should be injected
        using the FlaskInjector.
        * handler: Error handler. Must be callable if provided.
        * **options: General options for use with add_url_rule().
        """
        errs = []
        if not callable(func):
            errs.append('func must be callable.')
        if not isinstance(name, str):
            errs.append('name must be a string.')
        elif name in self.EndpointNames:
            errs.append('endpoint with name has already been added.')
        if not isinstance(route, str):
            errs.append('route must be a string.')
        elif not route.startswith('/'):
            errs.append('route must start with forward slash.')
        elif self.__HasURL(route, errs):
            errs.append('route has already been used.')
        if not FlaskAPIFactory.IsValidRESTMethod(methods, errs):
            pass
        if not isinstance(inject, bool):
            errs.append('inject must be a boolean.')
        if not handler is None and not callable(handler):
            errs.append('handler must be callable if provided.')
        if errs:
            raise ValueError('\n'.join(errs))
        # Strip out parameter names in route before using as name:
        kwargs = { 'rule' : route, 'endpoint' : name, 'view_func' : func, 'methods' : methods }
        #if not handler is None:
        #    kwargs['handler'] = EndpointAction(handler)
        # Add the endpoint to the API:
        FlaskAPIFactory.__app.add_url_rule(**kwargs, **options)
        with FlaskAPIFactory.__app.test_request_context():
            self.__UpdateEndpoints(url_for(name), name)
        if inject:
            self.__UpdateInjection(func)

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
        * Output default application json object that can
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
    def IsValidRESTMethod(cls, method, errs = None):
        """
        * Verify that passed method(s) are valid
        to be used with endpoint.
        Inputs:
        * method: string method or list of string methods.
        """
        if not errs is None and not isinstance(errs, list):
            raise ValueError('errs must be a list if provided.')
        if isinstance(method, str):
            method = [method]
        if any([not isinstance(met, str) for met in method]):
            raise ValueError('method must only consist of strings.')
        # Ensure that all provided methods are valid:
        method = [met.lower() for met in method]
        invalid = set(method) - FlaskAPIFactory.__validMethods
        if invalid:
            if not errs is None:
                errs.append('The following methods are invalid: %s.' % ','.join(invalid))
            return False
        return True
    
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
        injector = [] if injector is None else injector
        self.__appname = appname
        self.__hostname = hostname
        self.__baseurl = 'http://%s/' % self.__hostname
        self.__debug = debug
        self.__endpointnames = set()
        self.__injectors = [injector] if callable(injector) else list(injector)
        # 'modules' contains the functions that take (bind) as parameter:
        self.__injection = { 'views' : [], 'modules' : self.__injectors }
        self.__port = port
        self.__shouldinject = False
        self.__urls = set()
        # Start Flask app:
        FlaskAPIFactory.__app = Flask(appname)

    def __UpdateInjection(self, view):
        """
        * Update stored injection parameters to
        be fed into the FlaskInjector.
        """
        self.__injection['views'].append(view)
        self.__shouldinject = True 
        
    def __HasURL(self, route, errs = None):
        """
        * Check that route has not already been added.
        """
        if not errs is None and not isinstance(errs, list):
            raise ValueError('errs must be a list if provided.')
        url = r'%s%s' % (self.__baseurl, route.lstrip('/'))
        result = url in self.__urls
        if result and not errs is None:
            errs.append('%s has already been used.' % url)
        return result

    def __UpdateEndpoints(self, route, name):
        """
        * Update used endpoint names and full URLs 
        to keep track of which urls are currently being used 
        and the names of the endpoints.
        """
        self.__urls.add('%s%s' % (self.__baseurl, route.lstrip('/')))
        self.__endpointnames.add(name)

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
        if not injector is None:
            if not callable(injector) and not (not isinstance(injector, str) and hasattr(injector, '__iter__')):
                errs.append('injector must be callable or iterable of callables if provided.')
            elif not (not isinstance(injector, str) and hasattr(injector, '__iter__')) and any([not callable(at) for at in injector]):
                errs.append('injector must only contain callables if an iterable.')
        if errs:
            raise ValueError('\n'.join(errs))