#####################################
# ValidatorConfig.py
#####################################
# Description:
# * Convert config json file into APIConfigs to be
# injected into Flask controllers.

from DynamicETL_Validator.Configs.ControllerConfig import ControllerConfig
from DynamicETL_Validator.Configs.ETLSummaryReportConfig import ETLSummaryReportConfig
import json
import os

class ValidatorConfig:
    """
    * Convert config json file into APIConfigs to be
    injected into Flask controllers.
    """
    __attributeMap = { "apis" : (dict, { "etlsummaryreport" : ETLSummaryReportConfig }, "__apis"),
                       "controllerconfig" : (dict, ControllerConfig, "__controllerconfig") }
    __validateMap = { "apis" : (dict, { "etlsummaryreport" : ETLSummaryReportConfig }, "__apis"),
                       "controllerconfig" : (dict, ControllerConfig, "__controllerconfig") }
    def __init__(self, path):
        """
        * Convert all target sections in json file
        at path into associated APIConfig objects.
        Inputs:
        * path: String path to .json file.
        """
        APIConfigReader.__Validate(path)
        self.__SetProperties(path)

    ####################
    # Properties:
    ####################
    @property
    def APIs(self):
        return self.__apis
    @property
    def ControllerConfig(self):
        return self.__controllerconfig

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
            folder, file = os.split(path)
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
        missing = set(APIConfigReader.__attributeMap) - set(config)
        if missing:
            errs.append('json is missing the following sections: %s.' % ','.join(missing))
            return            
        for section in config:
            _type = ValidatorConfig.__attributeMap[section][0]
            attrs = ValidatorConfig.__attributeMap[section][1]
            if not _type is None and not isinstance(config[section], _type):
                errs.append('json::%s is not a %s type.' % (section, type(config[section])))
            elif not _type is None and _type == dict:
                missing = set(attrs) - set(config)
                if missing:
                    errs.append('json::%s is missing the following attributes: %s.' % (section, ','.join(missing)))

    def __SetProperties(self, path):
        """
        * Set object properties using passed json file.
        """
        config = json.load(open(path, 'rb'))
        # Normalize into lowercase:
        config = { key.lower() : config[key] for key in config }
        config['apis'] = { key.lower() : config['apis'][key] for key in config['apis'] }
        errs = []
        for section in config:
            _type = ValidatorConfig.__attributeMap[section][0]
            attrs = ValidatorConfig.__attributeMap[section][1]
            if _type == dict:
                objs = config[section]
                for obj in objs:
                    config['apis'] = { key.lower() : config['apis'][key] for key in config['apis'] }
                    self.__apis = {}
                    for api in config['apis']:
                        kwargs = config['apis'][api]
                        sectionType = APIConfigReader.__apiMap[api]
                        try:
                            obj = TypeConstructor.ConstructTypeKwargs(sectionType, **kwargs)
                            self.__apis[api] = obj
                        except Exception as ex:
                            errs.append('Failed to generate %s. Reason: %s.' % (key, str(ex)))
            else:
                self.__baseurl = config['baseurl']
        if errs:
            raise Exception('\n'.join(errs))

if __name__ == '__main__':
    path = r"C:\Users\berutan\Desktop\Projects\DynamicETLDashboard\DynamicETL_Validator\ScriptArgs\DynamicETL_Validator.json"
    reader = ValidatorConfig(path)