# -*- coding: utf-8 -*-

import logging
from os.path import exists
from os import makedirs
import ConfigParser

# ------ Init Config ------

config_path = "./config.ini"
config_section = "crawler"
logging_section = "logging"

config = ConfigParser.SafeConfigParser()
config.read(config_path)

def config_section_map(section):
    result = {}
    options = config.options(section)
    for option in options:
        try:
            result[option] = config.get(section, option)
        except:
            result[option] = None
    return result

# ------ Init Config ------

# ------ Logging Config ------

log_path = config_section_map(logging_section)['log_path']

complete_path = config_section_map(logging_section)['complete_path']
debug_path = config_section_map(logging_section)['debug_path']
general_path = config_section_map(logging_section)['general_path']

general_format = config_section_map(logging_section)['general_format']
specific_format = config_section_map(logging_section)['specific_format']

if not exists(log_path):
    makedirs(log_path)

class SingleLevelFilter(logging.Filter):
    def __init__(self, pass_level, reject):
        self.pass_level = pass_level
        self.reject = reject

    def filter(self, record):
        if self.reject:
            return (record.levelno != self.pass_level)
        else:
            return (record.levelno == self.pass_level)

logger = logging.getLogger('crawler.py')
logger.setLevel(logging.DEBUG)

general_formatter = logging.Formatter(general_format)
specific_formatter = logging.Formatter(specific_format)

complete_handler = logging.FileHandler(complete_path)
complete_handler.setLevel(logging.DEBUG)
complete_handler.setFormatter(general_formatter)
logger.addHandler(complete_handler)

debug_handler = logging.FileHandler(debug_path)
debug_handler.addFilter(SingleLevelFilter(logging.DEBUG, False))
debug_handler.setFormatter(specific_formatter)
logger.addHandler(debug_handler)

general_handler = logging.FileHandler(general_path)
general_handler.setLevel(logging.INFO)
general_handler.setFormatter(general_formatter)
logger.addHandler(general_handler)

# ------ Logging Config ------

class MyConfigReader():

    def __init__(self):
        

    def config_section_map(self, section):
        result = {}
        options = config.options(section)
        for option in options:
            try:
                result[option] = config.get(section, option)
            except:
                result[option] = None
        return result

    config_path = "./config.ini"
    config_section = "crawler"
    logging_section = "logging"

    config = ConfigParser.SafeConfigParser()
    config.read(config_path)
