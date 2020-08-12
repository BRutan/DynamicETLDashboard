################################
# TypeConstructor.py
################################
# Description:
# * Singleton class that generates
# new instances from type objects.

import re
import sys

class TypeConstructor:
    """
    * Singleton class that generates
    new instances from type objects.
    """
    __replacements = re.compile('|'.join(['class', '<', '>',"'", ' +']))
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
    def GetUsedModules(cls, modulepath):
        """
        * Get all modules that were imported in module
        at path(s).
        Inputs:
        * modulepath: Can either be 
        1. Single string path to module or module object.
        2. Iterable of string paths to module or module objects.
        """
        if isinstance(modulepath, (str, sys.module)):
            modulepath = [modulepath]
        elif hasattr(modulepath, __iter__) and any([not isinstance(pth, str) and not isinstance(pth, sys.modules) for pth in modulepath]):
            raise Exception('modulepath can only contain strings or modules if an iterable.')
        if hasattr(modulepath, __iter__):
            # Get for all paths:
            out = set()
            errs = []
            for num, obj in enumerate(modulepath):
                if not errs:
                    if isinstance(obj, sys.module):
                        vals = [attr for attr in dir(obj) if not attr.startswith('_') and not callable(attr)]
                    elif os.path.exists(obj):
                        f = open(path, 'r')
                        content = f.read()
                        instructions = dis.get_instructions(content)
                        imports = [__ for __ in instructions if 'IMPORT' in __.opname]
                        grouped = {}
                        for instr in imports:
                            grouped[instr.opname].append(instr.argval)
                        f.close()
                        out = grouped['IMPORT_NAME']
                    else:
                        errs.append('%s does not exist.' % num)
                        continue
                out.update(vals)
            if errs:
                raise ValueError('\n'.join(errs))
        else:
            raise ValueError('modulepath must be a string path hor an iterable of string paths.')
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
