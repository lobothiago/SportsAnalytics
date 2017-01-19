# -*- coding: utf-8 -*-

import urllib2
from bs4 import BeautifulSoup
import json
import urllib
from datetime import datetime
from my_logger import MyLogger
from my_config_reader import MyConfigReader
from my_db import SQLDb
import re
from string_similarity import list_similarity
from prettytable import PrettyTable
from pprint import pprint 

class Crawler():
    
    crawler_section = "crawler"
    logging_section = "logging"
    db_section = "db"

    config = MyConfigReader()
    logger = MyLogger("crawler.py")

    bets_api_url = config.get(crawler_section, "bets_api_url")
    bets_params = config.get(crawler_section, "bets_params")
    data_url = config.get(crawler_section, "data_url")
    team_list_url = config.get(crawler_section, "team_list_url")
    bet_rate_threshold = float(config.get(crawler_section, "bet_rate_threshold"))
    
    db_name = config.get(db_section, "db_name")
    teams_table_name = config.get(db_section, "teams_table_name")

    def __init__(self):
        # self.build_team_database()
        self

    # HELPER METHODS

    def parse_html(self, url):
        stop = False
        self.logger.debug("Attempting to retrieve HTML from URL: {}".format(url))
        while not stop:
            try:
                response = urllib2.urlopen(url)
                stop = True
            except Exception as e:
                self.logger.error("Couldn't retrieve HTML. Exception: {}".format(e.message))
                stop = False
        html = response.read()
        return BeautifulSoup(html, 'html.parser')

    def parse_json(self, url):
        stop = False
        self.logger.debug("Attempting to retrieve JSON from URL: {}".format(url))
        while not stop:
            try:
                response = urllib2.urlopen(url)
                stop = True
            except Exception as e:
                self.logger.error("Couldn't retrieve JSON. Exception: {}".format(e.message))
                stop = False
        return json.load(response)

    def filter_team_name(self, name):
        # Default return values
        age_group = -1
        new_key = name
        
        # Regex expressions
        age_tag = re.compile(r"\s*([-|/|\s.]\s*)(sub|u|U)\s*[-|/]*[0-9]+", re.IGNORECASE)
        number_tag = re.compile(r"[0-9]+", re.IGNORECASE)

        # Try name match
        name_match = age_tag.search(name)
        
        if name_match:
            new_key = new_key[:name_match.start()].strip()
            age_string = name_match.group()
            number_match = number_tag.search(age_string)            
            if number_match:
                age_group = int(number_match.group())

        return new_key.replace("'", ""), age_group

    # ESPORTENET CRAWLER METHODS

    def crawl_bets(self):
        self.logger.info("Starting bet crawling procedure")

        data = self.parse_json(self.bets_api_url + urllib.quote(self.bets_params.format(datetime.now().year, datetime.now().month, datetime.now().day)))
        
        teams = self.retrieve_teams()

        t = PrettyTable(['Full Name == Name', 'Full Name', 'Name', 'Age Group'])

        for bet in data:
            # date = datetime.strptime(bet["dt_hr_ini"], '%Y-%m-%dT%H:%M:00')
            if "basquete" not in bet["camp_nome"].lower():
                # Just process teams if the bet rate is interesting
                if abs(bet["taxa_c"] - bet["taxa_f"]) > self.bet_rate_threshold:
                    self.logger.info(u"Interesting bet rate for '{}' x '{}': {} x {} - delta: {} (thr: {})".format(bet["casa_time"], bet["visit_time"], bet["taxa_c"], bet["taxa_f"], abs(bet["taxa_c"] - bet["taxa_f"]), self.bet_rate_threshold))
                    
                    team_h, age_h = self.filter_team_name(bet["casa_time"])
                    team_v, age_v = self.filter_team_name(bet["visit_time"])
                    
                    t.add_row([bet["casa_time"] == team_h, u"'{}'".format(bet["casa_time"]), u"'{}'".format(team_h), age_h])
                    t.add_row([bet["visit_time"] == team_v, u"'{}'".format(bet["visit_time"]), u"'{}'".format(team_v), age_v])
                    
                    if age_h != age_v:
                        self.logger.warning(u"Different age groups retrieved for '{}' vs '{}': {} and {}".format(bet["casa_time"], bet["visit_time"], age_h, age_v))
        
                    sub_group = [x for x in teams if x[2] == age_v]
                    option_names = [x[0].lower() for x in sub_group]

                    print u"'{}'' x '{}'".format(bet["casa_time"], bet["visit_time"])

                    pprint([(sub_group[y], x) for (x, y) in list_similarity(team_h.lower(), option_names, 20)])
                    print "\n"
                    pprint([(sub_group[y], x) for (x, y) in list_similarity(team_v.lower(), option_names, 20)])
                    print "\n"

        self.logger.info(u"Team name decoding result:\n{}".format(t))

    # SOCCERWAY CRAWLER METHODS

    def crawl_country_data(self, country_id):
        payload_country = dict(
            block_id=urllib.quote('page_teams_1_block_teams_index_club_teams_2'),
            callback_params=urllib.quote('{"level":1}'),
            action=urllib.quote('expandItem'),
            params=urllib.quote('{{"area_id":"{0}","level":2,"item_key":"area_id"}}'.format(country_id))
        )

        comps_request_url = self.data_url + '/a/block_teams_index_club_teams?block_id={block_id}&callback_params={callback_params}&action={action}&params={params}'.format(**payload_country)
        
        comps_data = self.parse_json(comps_request_url)
        comps_html = BeautifulSoup(comps_data['commands'][0]['parameters']['content'].rstrip('\n'), 'html.parser')
        
        return comps_html

    def crawl_comp_data(self, comp_id):
        payload_comp = dict(
            block_id=urllib.quote('page_teams_1_block_teams_index_club_teams_2'),
            callback_params=urllib.quote('{"level":"3"}'),
            action=urllib.quote('expandItem'),
            params=urllib.quote('{{"competition_id":"{0}","level":3,"item_key":"competition_id"}}'.format(comp_id))
        )

        comp_data_request_url = self.data_url + '/a/block_teams_index_club_teams?block_id={block_id}&callback_params={callback_params}&action={action}&params={params}'.format(**payload_comp)

        comp_data = self.parse_json(comp_data_request_url)
        comp_html = BeautifulSoup(comp_data['commands'][0]['parameters']['content'].rstrip('\n'), 'html.parser')

        return comp_html
        
    def extract_teams(self, html_data):        
        hrefs = html_data.find_all('a')
        
        result = [(x.string, self.data_url + x.get('href')) for x in hrefs if 'women' not in x.get('href')]

        return result

    def store_teams(self, dcto):
        db = SQLDb(self.db_name)
        
        for k, v in dcto.iteritems():
            m = 1 # No female teams for now

            new_key, age_group = self.filter_team_name(k)

            self.logger.debug(u"Trying to store team: {}, {}, {}, {}".format(k, v, age_group, m))

            # if not db.row_exists(self.teams_table_name, u"name='{}' AND sub={}".format(new_key, age_group)):
            if not db.row_exists(self.teams_table_name, u"url='{}'".format(v)):
                db.execute(u""" 
                    INSERT INTO {} (name, url, sub, m)
                    VALUES ('{}', '{}', {}, {});
                """.format(self.teams_table_name, 
                           new_key, 
                           v,
                           age_group,
                           m))

                self.logger.info(u"New team stored: {}, {}, {}, {}".format(k, v, age_group, m))
                
    def retrieve_teams_by_age(self, sub):
        db = SQLDb(self.db_name)

        matches = db.execute_group(""" 
            SELECT name, url from {}
            WHERE sub={}
        """.format(self.teams_table_name, sub))

        return matches

    def retrieve_teams(self):
        db = SQLDb(self.db_name)

        matches = db.execute_group("""
            SELECT name, url, sub from {}
        """.format(self.teams_table_name))

        return matches

    def build_team_database(self):
        self.logger.info("Starting team database update procedure")
        self.logger.info("Initializing database: '{}'".format(self.db_name))

        db = SQLDb(self.db_name)
        database = {}
        
        self.logger.info("Creating table: '{}'".format(self.teams_table_name))
        if not db.table_exists(self.teams_table_name):
            db.execute(""" 
                CREATE TABLE {} 
                ( 
                    name varchar(64),
                    url varchar(256) PRIMARY KEY,
                    sub INTEGER,
                    m BOOLEAN
                );
            """.format(self.teams_table_name))
        else:
            self.logger.info("Table already existent")

        parsed_html = self.parse_html(self.team_list_url)
        country_ids = [x.get('data-area_id') for x in parsed_html.body.find('ul', attrs={'class':'areas'}).find_all('li', attrs={'class':'expandable'})]
        
        self.logger.info("{} country ids retrieved".format(len(country_ids)))

        for country_id in country_ids:
            country_data = self.crawl_country_data(country_id)
            teams = []
            
            if country_data.find('ul', attrs={'class':'competitions'}) != None:
                self.logger.debug("No direct teams found for country #{}. Will look for competitions".format(country_id))
                comp_ids = [x.get('data-competition_id') for x in country_data.find_all('li')]
                self.logger.info("{} competition ids retrieved for country #{}".format(len(comp_ids), country_id))
                for comp_id in comp_ids:
                    comp_data = self.crawl_comp_data(comp_id)
                    new_teams = self.extract_teams(comp_data)
                    teams.extend(new_teams)
                    self.logger.info("{} teams retrieved for comp #{}".format(len(new_teams), comp_id))
            else:
                self.logger.debug("Direct teams found for country #{}".format(country_id))
                new_teams = self.extract_teams(country_data)                
                teams.extend(new_teams)
                self.logger.info("{} teams retrieved for comp #{}".format(len(new_teams), country_id))
            
            for team in teams:
                if not team[0] in database:
                    database[team[0]] = team[1]            

        self.store_teams(database)
                        


if __name__ == '__main__':
    crawler = Crawler()
    crawler.build_team_database()
    # crawler.retrieve_teams()
    # crawler.crawl_bets()
    
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
