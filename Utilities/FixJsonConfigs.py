#####################################
# FixJsonConfigs.py
#####################################
# Description:
# * Update json configs if new files have been
# passed.

import json
import re
import os

def FixJsonConfigs():
    """
    * Fix all json config files in AppSettingsFiles.
    """
    files = ['%s\\AppsettingsFiles\\FileWatcher_AppSettings-template.json', '%s\\AppsettingsFiles\\Service_Appsettings.json']
    errs = []
    for num in range(0, len(files)):
        files[num] = files[num] % os.getcwd()
        if not os.path.exists(files[num]):
            errs.append(files[num])
        else:
            FixJsonConfig(files[num])
    if errs:
        errs.insert(0, 'The following required json files are missing:')
        raise Exception('\n'.join(errs))

def FixJsonConfig(jsonfilepath):
    """
    * Fix json file at path so can use with python json library.
    Inputs:
    * jsonfilepath: Path to json file that needs correcting.
    If nothing wrong with file then will do nothing.
    """
    if not isinstance(jsonfilepath, str):
        raise Exception('jsonfilepath must be a string.')
    elif not jsonfilepath.endswith('json'):
        raise Exception('jsonfilepath must point to json file.')
    elif not os.path.exists(jsonfilepath):
        raise Exception('File at jsonfilepath does not exist.')
    try:
        result = json.load(open(jsonfilepath, 'rb'))
        return
    except:
        pass
    # Convert non-string environment variables to strings:
    #varPattern = re.compile('((?<!")|{.+}')
    varPattern = re.compile('[\s\[\n{]{.+}[\s\]\n}]')
    lines = []
    with open(jsonfilepath, 'r') as target:
        for line in target:
            matches = [match[0].strip(' []\n') for match in varPattern.finditer(line)]
            if matches:
                matches = set(matches)
                for match in matches:
                    line = line.replace(match, '"%s"' % match)
            lines.append(line)
    # Write replacement lines to file:
    with open(jsonfilepath, 'w') as target:
        target.writelines(lines)
