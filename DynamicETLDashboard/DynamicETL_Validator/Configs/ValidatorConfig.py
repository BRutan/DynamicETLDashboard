#####################################
# ValidatorConfig.py
#####################################
# Description:
# * Register one config object for each
# API. Note that all configs class members must be public.

from Configs.AppConfig import AppConfig
from Configs.ETLSummaryReportConfig import ETLSummaryReportConfig
from DynamicETL_Dashboard.Utilities.TypeConstructor import TypeConstructor
import json
import os

class ValidatorConfig:
    """
    * Convert config json file into APIConfigs to be
    injected into Flask controllers.
    """
    __attributeMap = { "apis" : ({ "etlsummaryreport" : (ETLSummaryReportConfig, "etlsummaryreportconfig") }),
                       "appconfig" : (AppConfig, "appconfig") }
    def __init__(self, path):
        """
        * Convert all target sections in json file
        at path into associated APIConfig objects.
        Inputs:
        * path: String path to .json file.
        """
        ValidatorConfig.__Validate(path)
        self.__SetProperties(path)

    ####################
    # Properties:
    ####################
    @property
    def AppConfig(self):
        return self.appconfig
    @property
    def ETLSummaryReportConfig(self):
        return self.etlsummaryreportconfig
    
    ####################
    # Private Helpers:
    ####################
    @staticmethod
    def __Validate(path):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(path, str):
            errs.append('path must be a string.')
        else:
            folder, file = os.path.split(path)
            if not os.path.exists(folder):
                errs.append('path points to folder that does not exist.')
            if not file.endswith('.json'):
                errs.append('path must point to json file.')
            if not errs:
                try:
                    config = json.load(open(path, 'rb'))
                    config = { key.lower() : config[key] for key in config }
                    ValidatorConfig.__ValidateSections(config, errs)
                except Exception as ex:
                    errs.append('Failed to load %s. Reason: %s.' % (file, str(ex)))    
        if errs:
            raise Exception('\n'.join(errs))

    @staticmethod
    def __ValidateSections(config, errs):
        """
        * Validate individual sections.
        """
        missing = set(ValidatorConfig.__attributeMap) - set(config)
        if missing:
            errs.append('json is missing the following sections: %s.' % ','.join(missing))
            return            
        for section in config:
            attrs = ValidatorConfig.__attributeMap[section]
            attrs = attrs[0] if not isinstance(attrs, dict) else attrs
            if not isinstance(attrs, type) and not isinstance(attrs, type(config[section])):
                errs.append('json::%s is not a %s type.' % (section, type(attrs)))
            elif isinstance(attrs, dict):
                # Ensure all attribute keys are present in dictionary:
                config[section] = { key.lower() : config[section][key] for key in config[section] }
                missing = set(attrs) - set(config[section])
                if missing:
                    errs.append('json::%s is missing the following attributes: %s.' % (section, ','.join(missing)))

    def __SetProperties(self, path):
        """
        * Set object properties using passed json file.
        """
        config = json.load(open(path, 'rb'))
        # Normalize keys in lowercase:
        config = { key.lower() : config[key] for key in config }
        errs = []
        for section in config:
            attrs = ValidatorConfig.__attributeMap[section]
            if isinstance(attrs, dict):
                config[section] = { key.lower() : config[section][key] for key in config[section] }
                for attr in attrs:
                    kwargs = config[section][attr]
                    sectionType = attrs[attr][0]
                    targetAttr = attrs[attr][1]
                    try:
                        # Convert json section into object:
                        obj = TypeConstructor.ConstructTypeKwargs(sectionType, **kwargs)
                        setattr(self, targetAttr, obj)
                    except Exception as ex:
                        errs.append('Failed to generate %s. Reason: %s.' % (key, str(ex)))
            elif isinstance(attrs, tuple):
                kwargs = { key.lower() : config[section][key] for key in config[section] }
                sectionType = attrs[0]
                targetAttr = attrs[1]
                try:
                    # Convert json section into object:
                    obj = TypeConstructor.ConstructTypeKwargs(sectionType, **kwargs)
                    setattr(self, targetAttr, obj)
                except Exception as ex:
                    errs.append('Failed to generate %s. Reason: %s.' % (section, str(ex)))
        if errs:
            raise Exception('\n'.join(errs))