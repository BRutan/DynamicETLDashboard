#####################################
# DateFormatFinder.py
#####################################
# Description:
# * Given a possible datetime string,
# determine a regular expression/datetime 
# pattern that works for the string.
# Credit goes to: https://stackoverflow.com/questions/53892450/get-the-format-in-dateutil-parse

from collections import defaultdict, Counter
from dateutil.parser import parse
from itertools import product
import re
from Utilities.Helpers import StringIsDT

class DateFormatFinder:
    """
    * Find appropriate date format string
    from potential datetime string.
    """
    COMMON_SPECIFIERS = [
    '%a', '%A', '%d', '%b', '%B', '%m',
    '%Y', '%H', '%p', '%M', '%S', '%Z']
    def __init__(self, valid_specifiers=COMMON_SPECIFIERS,date_element=r'([\w]+)',delimiter_element=r'([\W]+)',ignore_case=False):
        """
        * Create new object that can determine appropriate
        datetime, regex and yyyymmdd type patterns from
        strings.
        Inputs:
        * valid_specifiers: iterable containing all common 
        dt specifiers.
        * date_element: regex pattern string used to group
        date elements together.
        * delimiter_element:
        * ignore_case: put True if want to ignore case-sensitivity
        in string.
        """
        DateFormatFinder.__Validate(valid_specifiers, date_element, delimiter_element, ignore_case)
        self.specifiers = valid_specifiers
        joined = (r'' + date_element + r"|" + delimiter_element)
        self.pattern = re.compile(joined)
        self.ignore_case = ignore_case
    
    ######################
    # Interface Methods:
    ######################
    def find_candidate_patterns(self, date_string):
        """
        * Return list of candidate %Y-%m-%d-type patterns that could
        work for the passed datetime string.
        Inputs:
        * date_string: String potentially containing
        value convertible to datetime object.
        """
        # Validate parameters:
        if not isinstance(date_string, str):
            raise Exception('date_string must be a string.')
        elif not StringIsDT(date_string):
            raise Exception('date_string cannot be converted to a datetime object.')
        # Determine %Y-%m-%d-type appropriate date format string from string:
        date = parse(date_string)
        tokens = self.pattern.findall(date_string)

        candidate_specifiers = defaultdict(list)

        for specifier in self.specifiers:
            token = date.strftime(specifier)
            candidate_specifiers[token].append(specifier)
            if self.ignore_case:
                candidate_specifiers[token.
                                     upper()] = candidate_specifiers[token]
                candidate_specifiers[token.
                                     lower()] = candidate_specifiers[token]

        options_for_each_element = []
        for (token, delimiter) in tokens:
            if token:
                if token not in candidate_specifiers:
                    options_for_each_element.append([token])
                else:
                    options_for_each_element.append(
                        candidate_specifiers[token])
            else:
                options_for_each_element.append([delimiter])

        # Determine all candidates:
        for parts in product(*options_for_each_element):
            counts = Counter(parts)
            max_count = max(counts[specifier] for specifier in self.specifiers)
            if max_count > 1:
                # this is a candidate with the same item used more than once
                continue
            yield "".join(parts)

    def find_regex_candidate_patterns(self, date_string):
        """
        * Return all regular expression patterns
        ready for use in "ConfigVal" attribute in various applications.
        Inputs:
        * date_string: string containing numeric
        values and/or punctuation that could 
        be a datetime object.
        """
        regexPatterns = []
        candidatePatterns = find_candidate_patterns(date_string)
        for candidate in candidatePatterns:
            regexPatterns.append(DateFormatFinder.ConvertDTFormatToRegex(candidate))
        return regexPatterns

    def find_YYYYMMDD_candidate_patterns(self, date_string):
        """
        * Return all YYYYMMDD expression patterns
        ready for use in "ConfigVal" attribute in various applications.
        Inputs:
        * date_string: string containing numeric
        values and/or punctuation that could 
        be a datetime object.
        """
        output = []
        candidatePatterns = find_candidate_patterns(date_string)
        for candidate in candidatePatterns:
            output.append(DateFormatFinder.ConvertDTFormatToRegex(candidate))
        return regexPatterns

    @staticmethod
    def ConvertDTFormatToRegex(dtPattern):
        """
        * Convert datetime format string to regex.
        Inputs:
        * dtPattern: %Y-%m-%d-type datetime pattern.
        """
        pass

    @staticmethod
    def ConvertDTFormatToYYYYMMDD(dtPattern):
        """
        * Convert datetime format string to
        YYYYMMDD type string.
        Inputs:
        * dtPattern: %Y-%m-%d-type datetime pattern.
        """
        pass

    #################
    # Private Helpers:
    #################
    @staticmethod
    def __Validate(valid_specifiers, date_element, delimiter_element, ignore_case):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not hasattr(valid_specifiers, '__iter__'):
            errs.append('valid_specifiers must be an iterable.')
        if not isinstance(date_element, str):
            errs.append('date_element must be a string.')
        if not isinstance(delimiter_element, str):
            errs.append('delimiter_element must be a string.')
        if not isinstance(ignore_case, bool):
            errs.append('ignore_case must be a boolean.')
        if errs:
            raise Exception('\n'.join(errs))