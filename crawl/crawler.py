# -*- coding: utf-8 -*-

import urllib2
from bs4 import BeautifulSoup
import json
import urllib
from datetime import datetime
from pprint import pprint
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

bets_api_url = config_section_map(config_section)['bets_api_url']
bets_params = config_section_map(config_section)['bets_params']

data_url = config_section_map(config_section)['data_url']
team_list_url = config_section_map(config_section)['team_list_url']

database = {}

def parse_html(url):
    stop = False
    logger.info("Attempting to retrieve HTML from URL: {}".format(url))
    while not stop:
        try:
            response = urllib2.urlopen(url)
            stop = True
        except Exception as e:
            logger.error("Couldn't retrieve HTML. Exception: {}".format(e.message))
            stop = False
    html = response.read()
    return BeautifulSoup(html, 'html.parser')

def parse_json(url):
    stop = False
    logger.info("Attempting to retrieve JSON from URL: {}".format(url))
    while not stop:
        try:
            response = urllib2.urlopen(url)
            stop = True
        except Exception as e:
            logger.error("Couldn't retrieve JSON. Exception: {}".format(e.message))
            stop = False
    return json.load(response)

def crawl_bets():
    logger.info("Starting bet crawling procedure")

    data = parse_json(bets_api_url + urllib.quote(bets_params.format(datetime.now().year, datetime.now().month, datetime.now().day)))    

    for bet in data:
        print bet['camp_nome']

    with open('output.json', 'w') as f:
        f.write(json.dumps(data, indent=4, sort_keys=True))
        f.close()

def build_team_database():
    logger.info("Starting team database update procedure")
    
    parsed_html = parse_html(team_list_url)
    country_ids = [x.get('data-area_id') for x in parsed_html.body.find('ul', attrs={'class':'areas'}).find_all('li', attrs={'class':'expandable'})]
    
    logger.info("{} country ids retrieved".format(len(country_ids)))
    
    for country_id in country_ids:
        payload_country = dict(
            block_id=urllib.quote('page_teams_1_block_teams_index_club_teams_2'),
            callback_params=urllib.quote('{"level":1}'),
            action=urllib.quote('expandItem'),
            params=urllib.quote('{{"area_id":"{0}","level":2,"item_key":"area_id"}}'.format(country_id))
        )

        comps_request_url = data_url + '/a/block_teams_index_club_teams?block_id={block_id}&callback_params={callback_params}&action={action}&params={params}'.format(**payload_country)
        
        logger.info("Fetching competitions for country #{}".format(country_id))
        
        comps_data = parse_json(comps_request_url)
        comps_html = BeautifulSoup(comps_data['commands'][0]['parameters']['content'].rstrip('\n'), 'html.parser')
        comp_ids = [x.get('data-competition_id') for x in comps_html.find_all('li')]
        
        logger.info("{} competition ids retrieved".format(len(comp_ids)))
        
        for comp_id in comp_ids:
            payload_comp = dict(
                block_id=urllib.quote('page_teams_1_block_teams_index_club_teams_2'),
                callback_params=urllib.quote('{"level":"3"}'),
                action=urllib.quote('expandItem'),
                params=urllib.quote('{{"competition_id":"{0}","level":3,"item_key":"competition_id"}}'.format(comp_id))
            )

            teams_request_url = data_url + '/a/block_teams_index_club_teams?block_id={block_id}&callback_params={callback_params}&action={action}&params={params}'.format(**payload_comp)
            
            logger.info("Fetching teams for competition #{}".format(comp_id))
            
            teams_data = parse_json(teams_request_url)
            teams_html = BeautifulSoup(teams_data['commands'][0]['parameters']['content'].rstrip('\n'), 'html.parser')
            links = teams_html.find_all('a')
            team_urls = [data_url + x.get('href') for x in links if 'women' not in x.get('href')]
            team_names = [x.string for x in links if 'women' not in x.get('href')]
            
            logger.info("{} teams retrieved".format(len(team_names)))
            
            for index, url in enumerate(team_urls):
                if url not in database:
                    database[url] = [team_names[index]]
                else:
                    database[url].append(team_names[index])

        pprint(database)

# build_team_database()
# crawl_bets()

# Team Page:
# -> Title: div id='subheading'
# -> id: from URL
# -> Matches: URL/matches

# Testes:
# distancia na tabela
# ultimos 5 jogos (considerando casa/campeonato)
# quantidade de gols dentro/fora (mesmo tempo)
# vitorias/derrotas
# ganha muito em casa e perde muito fora é um indicativo importante (comparar isso)

# h2h (no mesmo ano?)
