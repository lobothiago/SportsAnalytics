# -*- coding: utf-8 -*-

import logging
from os.path import exists
from os import makedirs

class SingleLevelFilter(logging.Filter):
    def __init__(self, pass_level, reject):
        self.pass_level = pass_level
        self.reject = reject

    def filter(self, record):
        if self.reject:
            return (record.levelno != self.pass_level)
        else:
            return (record.levelno == self.pass_level)

class MyLogger():

    def __init__(self, 
                 name,
                 log_path = "./logs", 
                 general_format = "[%(levelname)s] [%(name)s] [%(asctime)s]: %(message)s", 
                 specific_format = "[%(name)s] [%(asctime)s]: %(message)s"):
        self.name = name
        self.log_path = log_path
        self.complete_path = log_path + "/complete.log"
        self.general_path = log_path + "/general.log"
        self.debug_path = log_path + "/debug.log"
        self.general_format = general_format
        self.specific_format = specific_format
        
        if not exists(log_path):
            makedirs(log_path)

        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        general_formatter = logging.Formatter(general_format)
        specific_formatter = logging.Formatter(specific_format)

        complete_handler = logging.FileHandler(self.complete_path)
        complete_handler.setLevel(logging.DEBUG)
        complete_handler.setFormatter(general_formatter)
        self.logger.addHandler(complete_handler)

        debug_handler = logging.FileHandler(self.debug_path)
        debug_handler.addFilter(SingleLevelFilter(logging.DEBUG, False))
        debug_handler.setFormatter(specific_formatter)
        self.logger.addHandler(debug_handler)

        general_handler = logging.FileHandler(self.general_path)
        general_handler.setLevel(logging.INFO)
        general_handler.setFormatter(general_formatter)
        self.logger.addHandler(general_handler)

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def critical(self, message):
        self.logger.critical(message)
