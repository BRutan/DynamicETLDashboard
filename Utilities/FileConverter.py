#####################################
# FileConverter.py
#####################################
# Description:
# * Convert all files matching regular expression to path.

from argparse import ArgumentParser
import os
import re
import shutil
from Utilities.Helpers import IsRegex

class FileConverter:
    """
    * Empty singleton-like class that pulls in files (GetAllFilePaths())
    or converts large number of files to particular extension.
    """
    __reObj = type(re.compile(''))
    def __init__(self):
        """
        * Initialize singleton object.
        """
        pass

    @classmethod
    def GetAllFolderPaths(cls, headpath, folderreg):
        """
        * Get all folders matching folderreg located
        in headpath.
        Inputs:
        * headpath: String path containing folders want to search.
        * folderreg: regex string or regex object used to find
        folders.
        """
        errs = []
        if not isinstance(headpath, str):
            errs.append('headpath must be a string.')
        elif not os.path.isdir(headpath):
            errs.append('headpath does not point to valid directory.')
        if not isinstance(folderreg, (str, FileConverter.__reObj)):
            errs.append('folderreg must be a string or regular expression.')
        elif isinstance(folderreg, str):
            if not IsRegex(folderreg):
                errs.append('folderreg must be a valid regular expression.')
            else:
                folderreg = re.compile(folderreg)
        if errs:
            raise Exception('\n'.join(errs))
        # Get all matching folders:
        folders = []
        for folder in os.listdir(headpath):
            fullpath = os.path.join(headpath, folder)
            if os.path.isdir(fullpath) and folderreg.match(folder):
                folders.append(fullpath)
        return folders

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
        if not isinstance(folderpath, str):
            errs.append('folderpath must be a string.')
        elif not os.path.isdir(folderpath):
            errs.append('folderpath must point to a folder.')
        elif not os.path.exists(folderpath):
            errs.append('folderpath does not exist.')
        if isinstance(filereg, str) and not IsRegex(filereg):
            errs.append('filereg must be a valid regular expression string or object.')
        elif isinstance(filereg, str):
            filereg = re.compile(filereg)
        if not subdirs is None and not isinstance(subdirs, bool):
            errs.append('subdirs must be a boolean.')
        if errs:
            raise Exception('\n'.join(errs))
        filePaths = {}
        if filereg:
            filePaths = { file : os.path.join(folderpath, file) for file in os.listdir(folderpath) if os.path.isfile(os.path.join(folderpath, file)) and filereg.match(file)}
        else:
            filePaths = { file : os.path.join(folderpath, file) for file in os.listdir(folderpath) if os.path.isfile(os.path.join(folderpath, file))}
        return filePaths

    @classmethod
    def ConvertAllFilePaths(cls, outfolder, new_ext, folderpath = None, filereg = None, paths = None, subdirs = False):
        """
        * Convert all files listed in folderpath/paths to files with 
        extension in outfolder.
        Inputs:
        * outfolder: string output folder for converted files. Must exist.
        * new_ext: extension to convert files to.
        Mutually Exclusive Inputs:
        * folderpath, filereg: put string folder containing files matching filereg regular expression.
        * paths: list of filepaths to convert.
        Optional Inputs:
        * subdirs: Search all subdirectories of folderpath for matching files.
        Outputs:
        * Dictionary mapping { convertedfilename -> path }.
        """
        errs = []
        if not isinstance(outfolder, str):
            errs.append('outfolder must be a string.')
        elif not os.path.isdir(outfolder):
            errs.append('outfolder must be point to a folder.')
        elif not os.path.exists(outfolder):
            errs.append('outfolder does not exist.')
        if not isinstance(new_ext, str):
            errs.append('extension must be a string.')
        elif '.' not in new_ext:
            errs.append('extension is invalid.')
        if folderpath is None and filereg is None and paths is None:
            errs.append('One of folderpath, filereg and paths must be passed.')
        elif not (not folderpath is None and not filereg is None) ^ (not paths is None):
            errs.append('folderpath AND filereg or (exclusive) paths must be passed.')
        elif not folderpath is None:
            if not isinstance(folderpath, str):
                errs.append('folderpath must be a string.')
            elif not os.path.isdir(folderpath):
                errs.append('folderpath must point to a folder.')
            elif not os.path.exists(folderpath):
                errs.append('folderpath does not exist.')
            if not IsRegex(filereg):
                errs.append('filereg must be a valid regular expression.')
        elif not paths is None:
            if not hasattr(paths, '__iter__'):
                errs.append('paths must be an iterable.')
        if errs:
            raise Exception('\n'.join(errs))

        # Convert local folders to current working directory for use with
        # shutil:
        if ":" not in outfolder and outfolder[0] != "\\":
            outfolder = "%s\\%s" % (os.getcwd(), outfolder)
        if ":" not in folderpath and folderpath[0] != "\\":
            outfolder = "%s\\%s" % (os.getcwd(), folderpath)
        # Get all full file paths if not provided:
        if folderpath:
            paths = list(FileConverter.GetAllFilePaths(folderpath, filereg, subdirs).values())
        if not outfolder[len(outfolder)-1:len(outfolder)] == "\\":
            outfolder += "\\"
        
        convertedpaths = {}
        for filepath in paths:
            filename = os.path.basename(filepath)
            newfilename = "%s%s" % (filename[0:filename.find('.')],new_ext)
            convertedpath = "%s%s" % (outfolder, newfilename)
            if not os.path.exists(convertedpath):
                shutil.copyfile(filepath, convertedpath)
            convertedpaths[newfilename] = convertedpath

        return convertedpaths