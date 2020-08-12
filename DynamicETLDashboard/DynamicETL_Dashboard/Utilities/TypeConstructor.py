################################
# TypeConstructor.py
################################
# Description:
# * Singleton class that generates
# new instances from type objects.

import inspect
import os
import re
import sys

class TypeConstructor:
    """
    * Singleton class that generates
    new instances from type objects.
    """
    __replacements = re.compile('|'.join(['class', '<', '>',"'", ' +']))
    __moduleType = type(sys.modules['re'])
    __modPathRE = re.compile("from '.+'")
    __modPathStrip = re.compile('[from ]')
    __modPathStrip = lambda x, pat = __modPathStrip : pat.sub('', x).strip(' ') 
    ################
    # Interface Methods:
    ################
    @classmethod
    def ConstructType(cls, typeObj):
        """
        * Generate instance of type without arguments
        Inputs:
        * typeObj: type object to construct.
        """
        errs = []
        if not isinstance(typeObj, type):
            errs.append('typeObj must be a type.')
        if errs:
            raise ValueError('\n'.join(errs))
        module, classname = TypeConstructor.SplitType(typeObj)
        return TypeConstructor.__CreateClassObj(module, classname)

    @classmethod
    def ConstructTypeArgs(cls, typeObj, *args):
        """
        * Generate instance of type using 
        *args style arguments.
        Inputs:
        * typeObj: type object to construct.
        * args: variadic list containing objects to be fed to constructor. 
        """
        errs = []
        if not isinstance(typeObj, type):
            errs.append('typeObj must be a type.')
        if errs:
            raise ValueError('\n'.join(errs))
        module, classname = TypeConstructor.SplitType(typeObj)
        return TypeConstructor.__CreateClassObj(module, classname, args)

    @classmethod
    def ConstructTypeKwargs(cls, typeObj, **kwargs):
        """
        * Generate instance of type using 
        **kwargs style arguments.
        Inputs:
        * typeObj: type object to construct.
        * kwargs: dictionary containing objects to be fed to constructor. 
        """
        errs = []
        if not isinstance(typeObj, type):
            errs.append('typeObj must be a type.')
        if errs:
            raise ValueError('\n'.join(errs))
        module, classname = TypeConstructor.SplitType(typeObj)
        return TypeConstructor.__CreateClassObj(module, classname, kwargs)

    @classmethod
    def GetDefinedClasses(cls, modules):
        """
        * Get all classes defined in module(s) 
        at path, or in module objects.
        Inputs:
        * modules: string name of module (assuming has been imported) or module object,
        or iterable of string names or module objects.
        An exception will be thrown if any named modules have not been imported.
        Output:
        * Dictionary with { FullClassName -> (ModuleName, ClassName, ClassTypeObj) }. 
        """
        if isinstance(modules, (str, TypeConstructor.__moduleType)):
            modules = [modules]
        elif hasattr(modules, __iter__) and any([not isinstance(mod, str) and not isinstance(mod, TypeConstructor.__moduleType) for mod in modules]):
            raise Exception('modulepath can only contain module name strings or module objects if an iterable.')
        out = {}
        errs = []
        for num, mod in enumerate(modules):
            if isinstance(mod, str):
                if not TypeConstructor.IsImported:
                    errs.append(mod)
                    continue
                else:
                    mod = sys.modules[mod]
            classes = inspect.getmembers(mod, inspect.isclass)
            for item in classes:
                modname, classname = TypeConstructor.SplitType(item[1])
                fullname = '%s.%s' % (modname, classname)
                out[fullname] = (modname, classname, item[1])
        if errs:
            raise Exception('The following modules were not imported before call: %s.' % ','.join(errs))
        return out

    @classmethod
    def GetUsedModules(cls, modules):
        """
        * Get all modules that were imported in module
        at path(s).
        Inputs:
        * modules: Can either be 
        1. Single string path to module or module object.
        2. Iterable of string paths to module or module objects.
        Returns set of all module objects imported by passed modules.
        """
        if not isinstance(modules, (str, TypeConstructor.__moduleType)) and not (not isinstance(modules, str) and hasattr(modules, '__iter__')): 
            raise ValueError('modules must be a single string path/module object or iterable of string paths/module objects.')
        elif isinstance(modules, (str, TypeConstructor.__moduleType)):
            modules = [modules]
        if hasattr(modulepath, '__iter__') and any([not isinstance(pth, str) and not isinstance(pth, TypeConstructor.__moduleType) for pth in modulepath]):
            raise ValueError('modulepath can only contain strings or modules if an iterable.')
        out = set()
        errs = []
        for num, obj in enumerate(modulepath):
            if isinstance(obj, str) and not os.path.exists(obj):
                errs.append(num)
            if isinstance(obj, TypeConstructor.__moduleType):
                vals = [attr for attr in dir(obj) if not attr.startswith('_') and not callable(attr)]
            else:
                f = open(path, 'r')
                content = f.read()
                instructions = dis.get_instructions(content)
                vals = [inst.argval for inst in instructions if 'IMPORT_NAME' == inst.opname]
                f.close()
            out.update(vals)
        if errs:
            raise ValueError('The following module paths do not exist: %s.' % ','.join(errs))
        return out

    @classmethod
    def SplitType(cls, typeObj):
        """
        * Get the entity name from the type.
        Returns (module, classname).
        Inputs:
        * typeObj: type object.
        """
        if not isinstance(typeObj, type):
            raise ValueError('typeObj must be a type.')
        name = str(typeObj)
        module = TypeConstructor.__StripChars(name[0:name.rfind('.')])
        classname = TypeConstructor.__StripChars(name[name.rfind('.') + 1:len(name)])
        return module, classname

    @classmethod
    def IsImported(cls, module):
        """
        * Check that module has been imported.
        Inputs:
        * module: string module name.
        """
        if not isinstance(module, str):
            raise ValueError('module must be a string.')
        return module in sys.modules

    ################
    # Private Helpers:
    ################
    @staticmethod
    def __ExtractModulePaths(modules):
        """
        * Extract module paths from passed
        iterable of modules so they can be
        read by various libraries (like inspect).
        Inputs:
        * modules: Expecting iterable of module objects.
        """
        paths = set()
        for mod in modules:
            subbed = TypeConstructor.__modPathRE.find(mod)
            paths.add(TypeConstructor.__modPathStrip(mod))
        return paths

    @staticmethod
    def __FindModules(module):
        """
        * Search for all modules at all depths from
        which to perform class construction.
        """
        modules = []
        while module != '':
            if module in sys.modules:
                modules.append(module)
            if '.' not in module:
                break
            module = module[0:module.rfind('.')]
        return set(modules)

    @staticmethod
    def __CreateClassObj(module, classname, args = None):
        """
        * Create class object using one of the 
        modules at depth.
        """
        modules = TypeConstructor.__FindModules(module)
        # Attempt to construct class from one of the modules:
        for mod in modules:
            try:
                if isinstance(args, dict):
                    return getattr(sys.modules[mod], classname)(**args)
                elif isinstance(args, tuple):
                    return getattr(sys.modules[mod], classname)(*args)
                else:
                    return getattr(sys.modules[mod], classname)()
            except Exception as ex:
                if isinstance(ex, ValueError) or not "'module' object is not callable" in str(ex):
                    raise Exception('Argument Error: %s.' % str(ex))
        raise Exception('Could not construct class %s at any module depth in %s.' % (classname, module))

    @staticmethod
    def __StripChars(val):
        """
        * Strip characters from module/class name.
        """
        return TypeConstructor.__replacements.sub('', val)
