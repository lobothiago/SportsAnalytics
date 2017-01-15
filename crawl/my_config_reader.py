# -*- coding: utf-8 -*-

import ConfigParser

class MyConfigReader():

    def __init__(self, config_path="./config.ini"):
        self.config_path = config_path
        self.config = ConfigParser.SafeConfigParser()        
        self.data = {}

        try:
            self.config.read(self.config_path)
            sections = self.config.sections()
            for section in sections:
                section_options = {}
                options = self.config.options(section)
                for option in options:
                    section_options[option] = self.config.get(section, option)
                self.data[section] = section_options
        except Exception as e:
            print "Couldn't read config file at {}: {}".format(self.config_path, e.message)

    def get(self, section, option):
        if section in self.data and option in self.data[section]:
            return self.data[section][option]
        else:
            return None
