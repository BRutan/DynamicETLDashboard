#####################################
# FileConverter.py
#####################################
# Description:
# * Convert all files matching regular expression to path.

from argparse import ArgumentParser
from cmd2 import with_argparser
import shutil
import os
from Utilities.Helpers import IsRegex

class FileConverter:
    """
    * Empty singleton-like class.
    """
    def __init__(self):
        """
        * 
        """
        pass
    @classmethod
    def GetAllFilePaths(cls, folderpath, filereg, subdirs = False):
        """
        * Get all full paths for files matching regular expression
        at folderpath.
        Inputs:
        * folderpath: String path to folder.
        * filereg: Regular expression for files.
        Optional:
        * subdirs: Search all subdirectories in folder for files. 
        Output:
        * Returns { FileName -> Path }.
        """
        errs = []
        if not isinstance(str, folderpath):
            errs.append('folderpath must be a string.')
        elif not os.path.isdir(folderpath):
            errs.append('folderpath must point to a folder.')
        elif not os.path.exists(folderpath):
            errs.append('folderpath does not exist.')
        if not IsRegex(filereg):
            errs.append('filereg must be a valid regular expression string or object.')
        if not subdirs is None and not isinstance(subdirs, bool):
            errs.append('subdirs must be a boolean.')
        if errs:
            raise Exception('\n'.join(errs))
        filePaths = {}
        if fileExp:
            filePaths = { file : os.path.join(path, file) for file in os.listdir(path) if os.path.isfile(os.path.join(path, file)) and filereg.match(file)}
        else:
            filePaths = { file : os.path.join(path, file) for file in os.listdir(path) if os.path.isfile(os.path.join(path, file))}
        return filePaths
    @classmethod
    def ConvertAllFilePaths(cls, outfolder, extension, folderpath = None, filereg = None, paths = None, subdirs = False):
        """
        * Convert all files listed in folderpath/paths to files with 
        extension in outfolder.
        Inputs:
        * outfolder: string output folder for converted files. Must exist.
        * extension: extension to convert files to.
        Mutually Exclusive Inputs:
        * folderpath, filereg: put string folder containing files matching filereg regular expression.
        * paths: list of filepaths to convert.
        Optional Inputs:
        * subdirs: Search all subdirectories of folderpath for matching files.
        """
        errs = []
        if not isinstance(outfolder, str):
            errs.append('outfolder must be a string.')
        elif not os.path.isdir(outfolder):
            errs.append('outfolder must be point to a folder.')
        elif not os.path.exists(outfolder):
            errs.append('outfolder does not exist.')
        if not isinstance(extension, str):
            errs.append('extension must be a string.')
        elif '.' not in extension:
            errs.append('extension is invalid.')
        if folderpath is None and filereg is None and paths is None:
            errs.append('One of folderpath, filereg and paths must be passed.')
        elif not (folderpath is None and filereg is None) ^ (paths is None):
            errs.append('folderpath AND filereg or (exclusive) paths must be passed.')
        elif not folderpath is None:
            if not isinstance(folderpath, str):
                errs.append('folderpath must be a string.')
            elif not os.path.isdir(folderpath):
                errs.append('folderpath must point to a folder.')
            elif os.path.exists(folderpath):
                errs.append('folderpath does not exist.')
            if not IsRegex(filereg):
                errs.append('filereg must be a valid regular expression.')
        elif not paths is None:
            if not hasattr(paths, '__iter__'):
                errs.append('paths must be an iterable.')
        if errs:
            raise Exception('\n'.join(errs))

        if folderpath:
            paths = list(FileConverter.GetAllFilePaths(folderpath, filereg, subdirs).values())
        
        for filepath in paths:
            filename, file_extension = os.path.splitext(filepath)
            oldfile = "%s//%s_%s" % (outfolder, filename, file_extension)
            newfile = "%s//%s_%s" % (outfolder, filename, extension)
            shutil.copyfile(filepath, outfolder)
            os.rename(oldfile, newfile)