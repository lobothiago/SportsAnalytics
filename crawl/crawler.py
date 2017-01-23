# -*- coding: utf-8 -*-

import urllib2
from bs4 import BeautifulSoup
import json
import urllib
from datetime import datetime, timedelta
from my_logger import MyLogger
from my_config_reader import MyConfigReader
from my_db import SQLDb
import re
from string_similarity import list_similarity
from prettytable import PrettyTable
from pprint import pprint 
from os.path import exists
from os import makedirs

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
    bet_search_depth = int(config.get(crawler_section, "bet_search_depth"))
    match_day_window = int(config.get(crawler_section, "match_day_window"))
    match_hour_threshold = int(config.get(crawler_section, "match_hour_threshold"))

    db_name = config.get(db_section, "db_name")
    teams_table_name = config.get(db_section, "teams_table_name")
    date_storage_format = config.get(db_section, "date_storage_format")
    time_storage_format = config.get(db_section, "time_storage_format")
    
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
        filtered_name = name.strip()
        
        # Regex expressions
        age_tag = re.compile(r"\s*([-|/|\s.]\s*)(sub|u|U|UNDER|under)\s*[-|/]*[0-9]+", re.IGNORECASE)
        number_tag = re.compile(r"[0-9]+", re.IGNORECASE)

        # Try name match
        name_match = age_tag.search(name)
        
        if name_match:
            filtered_name = filtered_name[:name_match.start()].strip()
            age_string = name_match.group()
            number_match = number_tag.search(age_string)            
            if number_match:
                age_group = int(number_match.group())

        filtered_name = filtered_name.replace("'", "")

        if "-" in filtered_name:
            filtered_name = filtered_name[:filtered_name.index("-")]

        if "/" in filtered_name:
            filtered_name = filtered_name[:filtered_name.index("/")]

        return filtered_name.strip(), age_group

    def team_name_from_url(self, url):
        name, age = self.filter_team_name(url.split("/")[5].replace("-", " "))

        return name, age

    # ESPORTENET CRAWLER METHODS

    def crawl_bets(self):
        self.logger.info("Starting bet crawling procedure")

        db = SQLDb(self.db_name)

        data = self.parse_json(self.bets_api_url + urllib.quote(self.bets_params.format(datetime.now().year, datetime.now().month, datetime.now().day)))
        
        t = PrettyTable(['Full Name == Name', 'Full Name', 'Name', 'Age Group'])

        for bet in data:
            if "basquete" not in bet["camp_nome"].lower():
                # Just process teams if the bet rate is interesting
                if abs(bet["taxa_c"] - bet["taxa_f"]) > self.bet_rate_threshold:
                    self.logger.info(u"Interesting bet rate for '{}' x '{}': {} x {} - delta: {} (thr: {})".format(bet["casa_time"], bet["visit_time"], bet["taxa_c"], bet["taxa_f"], abs(bet["taxa_c"] - bet["taxa_f"]), self.bet_rate_threshold))
                    match_dt = datetime.strptime(bet["dt_hr_ini"], '%Y-%m-%dT%H:%M:00')
                    match_date = match_dt.strftime(self.date_storage_format)
                                    
                    if not db.table_exists(match_date):
                        # self.logger.info("")

                    team_h, age_h = self.filter_team_name(bet["casa_time"])
                    team_v, age_v = self.filter_team_name(bet["visit_time"])
                    
                    t.add_row([bet["casa_time"] == team_h, u"'{}'".format(bet["casa_time"]), u"'{}'".format(team_h), age_h])
                    t.add_row([bet["visit_time"] == team_v, u"'{}'".format(bet["visit_time"]), u"'{}'".format(team_v), age_v])

                    if age_h != age_v:
                        self.logger.warning(u"Different age groups retrieved for '{}' vs '{}': {} and {}".format(bet["casa_time"], bet["visit_time"], age_h, age_v))

        self.logger.info(u"Team name decoding result:\n{}".format(t))

    # SOCCERWAY CRAWLER METHODS
    def store_match(self, match):
        date = match.find('td', attrs={'class':'date'}).string.strip()
        dt = datetime.strptime(date, '%d/%m/%y')
        date = dt.strftime(self.date_storage_format)

        today_dt = datetime(year=datetime.now().year, month=datetime.now().month, day=datetime.now().day)

        time = match.find('td', attrs={'class':'status'}).a.span.string.replace(" ", "").strip()
        
        try:
            match_url = self.data_url + match.find('td', attrs={'class':'info-button'}).a.get("href")
        except Exception as e:
            self.logger.error("Couldn't retrieve match_url: {}".format(e.message))
            return

        if dt < today_dt:
            self.logger.debug("Match {} in past. Skipping it".format(match_url))
            return

        if (dt - today_dt).total_seconds() > (self.match_day_window * 24 * 60 * 60):
            self.logger.debug("Match {} outside of time window. Skipping it".format(match_url))
            return

        h_url = self.data_url + match.find('td', attrs={'class':'team-a'}).a.get('href')
        v_url = self.data_url + match.find('td', attrs={'class':'team-b'}).a.get('href')
        h_name, h_age = self.filter_team_name(match.find('td', attrs={'class':'team-a'}).a.string.strip())
        v_name, v_age = self.filter_team_name(match.find('td', attrs={'class':'team-b'}).a.string.strip())

        if u"…" in h_name:
            self.logger.debug(u"h_name '{}' is too big. Using url to determine it".format(h_name))
            h_name, h_age = self.team_name_from_url(h_url)
            self.logger.debug(u"New h_name '{}'".format(h_name))

        if u"…" in v_name:
            self.logger.debug(u"v_name '{}' is too big. Using url to determine it".format(v_name))
            v_name, v_age = self.team_name_from_url(v_url)
            self.logger.debug(u"New v_name '{}'".format(v_name))

        if v_age != h_age:
            self.logger.warning(u"h_age ({}) != v_age ({}) for match: {}".format(h_age, v_age, match_url))

        db = SQLDb(self.db_name)
        
        if not db.table_exists(date):
            self.logger.info(u"Table for day {} does not exist. Creating now".format(date))
            db.execute(u"""
                CREATE TABLE '{}'
                (
                    match_url varchar(256) PRIMARY KEY,
                    a_name varchar(64),
                    b_name varchar(64),
                    a_url varchar(256),
                    b_url varchar(256),
                    hour varchar(32)
                );
            """.format(date))

        if not db.row_exists(date, "match_url='{}'".format(match_url)):
            db.execute(u""" 
                INSERT INTO '{}' (match_url, a_name, b_name, a_url, b_url, hour)
                VALUES ('{}', '{}', '{}', '{}', '{}', '{}');
            """.format(date, match_url, h_name, v_name, h_url, v_url, time))

            self.logger.debug(u"New match stored at {}: {}".format(date, match_url))

    def crawl_matches_by_day(self, dt):
        day_string_url = dt.strftime("%Y/%m/%d")

        url = self.data_url + "/matches/{}/".format(dt.strftime("%Y/%m/%d"))
        self.logger.info("URL for day {} is: {}".format(day_string_url, url))
        
        parsed_html = self.parse_html(url)

        competition_links = parsed_html.find_all('th', attrs={'class':'competition-link'})
        competition_urls = [self.data_url + x.a.get('href') for x in competition_links]

        self.logger.info("Found {} competitions found for day {}".format(len(competition_urls), day_string_url))

        for competition_url in competition_urls:
            competition_html = self.parse_html(competition_url)
            
            matches = competition_html.find_all("tr", attrs={'class':'no-date-repetition', 'class':'match'})
            self.logger.info("Found {} matches".format(len(matches)))
            
            for match in matches:                
                # If future match -> class status
                # else -> class score
                future_match = match.find_all("td", attrs={'class':'status'})

                # If contains span, not cancelled or postponed. Contains time instead
                if len(future_match) > 0 and future_match[0].a.span:
                    self.store_match(match)

    def clear_old_match_tables(self):
        self.logger.info(u"Cleaning old match tables")
        today_dt = datetime(year=datetime.now().year, month=datetime.now().month, day=datetime.now().day)
        
        db = SQLDb(self.db_name)

        table_names = [x[0] for x in db.execute_group("SELECT name FROM sqlite_master")]

        t = PrettyTable(["Table Name"])

        for table_name in table_names:
            t.add_row([table_name])

        self.logger.info("Found the following tables in db:\n{}".format(t))

        for table_name in table_names:
            if 'sqlite' in table_name:
                continue
            table_dt = datetime.strptime(table_name, self.date_storage_format)
            if table_dt < today_dt:
                self.logger.info("Table '{}' in past. Dropping it".format(table_name))
                db.execute("DROP TABLE '{}'".format(table_name))
                
    def crawl_matches(self):
        self.clear_old_match_tables()
        today_dt = datetime(year=datetime.now().year, month=datetime.now().month, day=datetime.now().day)
        self.logger.info(u"Starting match crawling procedure. today_dt = {}".format(today_dt))

        for i in range(self.match_day_window):
            search_dt = today_dt + timedelta(days=i)
            self.crawl_matches_by_day(search_dt)

    def crawl_team_data(self, url, name_only=False):
        result = {}
        parsed_html = self.parse_html(url)
        
        result["name"], dummy = self.filter_team_name(parsed_html.find('div', attrs={'id':'subheading'}).h1.string)
        
        if name_only:
            return result

        return result

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
                
    def store_team(self, team):
        db = SQLDb(self.db_name)
        
        name = team[0]
        url = team[1]
        
        m = 1 # No female teams for now
        name, age_group = self.filter_team_name(name)
        self.logger.debug(u"Trying to store team: {}, {}, {}, {}".format(name, url, age_group, m))

        if not db.row_exists(self.teams_table_name, u"url='{}'".format(url)):
            db.execute(u""" 
                INSERT INTO '{}' (name, url, sub, m)
                VALUES ('{}', '{}', {}, {});
            """.format(self.teams_table_name, 
                       name,
                       url,
                       age_group,
                       m))

            self.logger.info(u"New team stored: {}, {}, {}, {}".format(name, url, age_group, m))

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
        
        self.logger.info("Creating table: '{}'".format(self.teams_table_name))
        if not db.table_exists(self.teams_table_name):
            db.execute(""" 
                CREATE TABLE '{}'
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
                self.logger.info("{} teams retrieved for country #{}".format(len(new_teams), country_id))
            
            for team in teams:
                self.store_team(team)

if __name__ == '__main__':
    crawler = Crawler()
    # crawler.build_team_database()
    crawler.crawl_bets()
    # crawler.crawl_matches()
    # db = SQLDb(crawler.db_name)
    # data = db.execute_group("SELECT match_url FROM '17/01/23'")
    # pprint(data)

        
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
