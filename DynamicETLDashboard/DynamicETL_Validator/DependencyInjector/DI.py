#####################################
# DI.py
#####################################
# Description:
# * Inject dependencies into flask apis.

from DynamicETL_Dashboard.Utilities.TypeConstructor import TypeConstructor
from injector import singleton
import sys

def inject_api_dependencies(binder, parentcfg, allconfigs, *objs, **kwargs):
    """
    * Inject dependencies into flask apis using FlaskInjector 
    in dynamic fashion.
    """
    errs = []
    if not isinstance(parentcfg, type):
        errs.append('parentcfg must be a type.')
    if not isinstance(allconfigs, (str, type(sys.modules['sys']))):
        errs.append('allconfigs must be a string module name or a module object.')
    elif not TypeConstructor.IsImported(allconfigs):
        errs.append('allconfigs module has not been imported.')
    elif isinstance(allconfigs, str):
        allconfigs = sys.modules[allconfigs]
    if errs:
        raise ValueError('\n'.join(errs))
    # Convert .json file into parent configuration object:
    parent = TypeConstructor.ConstructTypeKwargs(parentcfg, **kwargs)
    # Extract each member from the parent object for dependency injection:
    configs = TypeConstructor.GetDefinedClasses(allconfigs)
    for cfg in configs:
        cfgName = configs[cfg][1]
        cfgType = configs[cfg][2]
        binder.bind(getattr(parent, cfgName), to=cfgType, scope=singleton)
    # Bind additional objects:
    for obj in objs:
        binder.bind(obj, to=type(obj), scope=singleton)