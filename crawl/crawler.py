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
    bet_search_depth = int(config.get(crawler_section, "bet_search_depth"))
    match_day_window = int(config.get(crawler_section, "match_day_window"))
    match_hour_threshold = float(config.get(crawler_section, "match_hour_threshold"))
    delta_hours = int(config.get(crawler_section, "delta_hours"))
    old_match_tolerance = int(config.get(crawler_section, "old_match_tolerance"))
    bets_file_name = config.get(crawler_section, "bets_file_name")
    discard_matches_days = float(config.get(crawler_section, "discard_matches_days"))
    
    db_name = config.get(db_section, "db_name")
    teams_table_name = config.get(db_section, "teams_table_name")
    matches_table_name = config.get(db_section, "matches_table_name")
    date_storage_format = config.get(db_section, "date_storage_format")
    time_storage_format = config.get(db_section, "time_storage_format")
    analyses_table_name = config.get(db_section, "analyses_table_name")
    bets_table_name = config.get(db_section, "bets_table_name")
    
    def __init__(self):
        db = SQLDb(self.db_name)
        
        self.logger.info(u"Initializing crawler")

        if not db.table_exists(self.matches_table_name):
            self.logger.info(u"Table '{}' does not exist. Creating it now".format(self.matches_table_name))
            db.execute(u"""
                CREATE TABLE '{}'
                (
                    match_url varchar(256) PRIMARY KEY,
                    a_name varchar(64),
                    b_name varchar(64),
                    a_url varchar(256),
                    b_url varchar(256),
                    hour varchar(32),
                    day varchar(16)
                );
            """.format(self.matches_table_name))

        if not db.table_exists(self.bets_table_name):
            self.logger.info(u"Table '{}' does not exist. Creating it now".format(self.bets_table_name))
            db.execute(u"""
                CREATE TABLE '{}'
                (
                    id INTEGER PRIMARY KEY,
                    home_name varchar(64),
                    home_rate REAL,
                    visit_name varchar(64),
                    visit_rate REAL,
                    delta_rate REAL,
                    draw_rate REAL,
                    hour varchar(32),
                    day varchar(16),
                    match_url varchar(256),
                    fit_score REAL
                );
            """.format(self.bets_table_name))

        if not db.table_exists(self.analyses_table_name):
            self.logger.info(u"Table '{}' does not exist. Creating it now".format(self.analyses_table_name))
            db.execute(u"""
                CREATE TABLE '{}'
                (
                    match_url varchar(256) PRIMARY KEY,
                    
                    h_pos INTEGER,
                    v_pos INTEGER,
                    d_pos INTEGER,
                    two_tables INTEGER,

                    hgh INTEGER,
                    hth INTEGER,
                    hmh INTEGER,
                    hwh INTEGER,
                    hlh INTEGER,
                    hdh INTEGER,

                    hgv INTEGER,
                    htv INTEGER,
                    hmv INTEGER,
                    hwv INTEGER,
                    hlv INTEGER,
                    hdv INTEGER,

                    vgh INTEGER,
                    vth INTEGER,
                    vmh INTEGER,
                    vwh INTEGER,
                    vlh INTEGER,
                    vdh INTEGER,

                    vgv INTEGER,
                    vtv INTEGER,
                    vmv INTEGER,
                    vwv INTEGER,
                    vlv INTEGER,
                    vdv INTEGER
                );
            """.format(self.analyses_table_name))

    # HELPER METHODS

    def parse_html(self, url):
        self.logger.debug("Attempting to retrieve HTML from URL: {}".format(url))
        trials = 0
        while trials < 1024:
            try:
                response = urllib2.urlopen(url)
                trials = 1024
            except Exception as e:
                self.logger.error("Trial {}/1024 couldn't retrieve HTML. Exception: {}".format(trials + 1, e.message))
                trials += 1
        html = response.read()
        return BeautifulSoup(html, 'html.parser')

    def parse_json(self, url):
        self.logger.debug("Attempting to retrieve JSON from URL: {}".format(url))
        trials = 0
        while trials < 1024:
            try:
                response = urllib2.urlopen(url)
                trials = 1024
            except Exception as e:
                self.logger.error("Trial {}/1024 couldn't retrieve JSON. Exception: {}".format(trials + 1, e.message))
                trials += 1
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
            next_chars = filtered_name[filtered_name.index("-") + 1:].replace(" ", "")
            if len(next_chars) > 2:
                filtered_name = " ".join(filtered_name.replace("-", " ").split())
            else:
                filtered_name = filtered_name[:filtered_name.index("-")]

        if "/" in filtered_name:
            filtered_name = filtered_name[:filtered_name.index("/")]

        return filtered_name.strip(), age_group

    def team_name_from_url(self, url):
        name, age = self.filter_team_name(url.split("/")[5].replace("-", " "))

        return name, age

    def team_id_from_url(self, url):
        tag = re.compile(r"[/][0-9]+[/]", re.IGNORECASE)
        id = -1
        
        match = tag.search(url)

        if match:
            id = int(match.group().replace("/", ""))

        return id

    # SOCCERWAY CRAWLER METHODS
    def store_match(self, match, dt):
        date = dt.strftime(self.date_storage_format)

        time = match.find('td', attrs={'class':'status'}).a.span.string.replace(" ", "").strip()
        
        match_dt = datetime.strptime("{} {}".format(date, time), "{} {}".format(self.date_storage_format, self.time_storage_format))
        match_dt = match_dt - timedelta(hours = self.delta_hours)

        date = match_dt.strftime(self.date_storage_format)
        time = match_dt.strftime(self.time_storage_format)
        
        try:
            match_url = self.data_url + match.find('td', attrs={'class':'info-button'}).a.get("href")
        except Exception as e:
            self.logger.error("Couldn't retrieve match_url: {}".format(e.message))
            return

        if match_dt < datetime.now():
            self.logger.debug("Match {} in past. Skipping it".format(match_url))
            return

        h_url = self.data_url + match.find('td', attrs={'class':'team-a'}).a.get('href')
        v_url = self.data_url + match.find('td', attrs={'class':'team-b'}).a.get('href')
        h_name, h_age = self.filter_team_name(match.find('td', attrs={'class':'team-a'}).a.string.strip())
        v_name, v_age = self.filter_team_name(match.find('td', attrs={'class':'team-b'}).a.string.strip())

        if u"…" in h_name or len(h_name) <= 3:
            self.logger.debug(u"h_name '{}' is too big or too small. Using url to determine it".format(h_name))
            h_name, h_age = self.team_name_from_url(h_url)
            self.logger.debug(u"New h_name '{}'".format(h_name))

        if u"…" in v_name or len(v_name) <= 3:
            self.logger.debug(u"v_name '{}' is too big or too small. Using url to determine it".format(v_name))
            v_name, v_age = self.team_name_from_url(v_url)
            self.logger.debug(u"New v_name '{}'".format(v_name))

        # h_name, h_age = self.team_name_from_url(h_url)
        # v_name, v_age = self.team_name_from_url(v_url)

        db = SQLDb(self.db_name)

        if db.row_exists(self.matches_table_name, u"match_url = '{}'".format(match_url)):
            db.execute(u"""
                DELETE FROM '{}'
                WHERE match_url = '{}';
            """.format(self.matches_table_name, match_url))

        db.execute(u"""
            INSERT INTO '{}' (match_url, a_name, b_name, a_url, b_url, hour, day)
            VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}');
        """.format(self.matches_table_name, match_url, h_name, v_name, h_url, v_url, time, date))

        self.logger.debug(u"New match stored at {} {}: {}".format(date, time, match_url))

    def crawl_match_data(self, stage_value, competition_id, dt):
        payload = dict(
            block_id=urllib.quote('page_matches_1_block_date_matches_1'),
            callback_params=urllib.quote("""{{"bookmaker_urls":{{"13":[{{"link":"http://www.bet365.com/home/?affiliate=365_371546","name":"Bet 365"}}]}},
                                             "block_service_id":"matches_index_block_datematches",
                                             "date":"{}",
                                             "stage-value":"{}"}}""".format(dt.strftime("%Y-%m-%d"), stage_value)),
            action=urllib.quote('showMatches'),
            params=urllib.quote('{{"competition_id":{}}}'.format(competition_id))
        )

        request_url = self.data_url + '/a/block_date_matches?block_id={block_id}&callback_params={callback_params}&action={action}&params={params}'.format(**payload)
        
        data = self.parse_json(request_url)
        html = BeautifulSoup(data['commands'][0]['parameters']['content'].rstrip('\n'), 'html.parser')
        
        return html

    def crawl_match_score(self, match_url):
        parsed_html = self.parse_html(match_url)

        result = dict(
            status="future",
            score_h=-1,
            score_v=-1
        )

        score_header = parsed_html.find("h3", attrs={'class':'scoretime'})
        
        if score_header.string:
            score_class = score_header.get("class")

            if "score-orange" in score_class:
                result["status"] = "ongoing"
            else:
                result["status"] = "past"

            scores = [x.strip() for x in score_header.string.split("-")]

            result["score_h"] = int(scores[0])
            result["score_v"] = int(scores[1])

        return result

    def crawl_matches_by_day(self, dt):
        date_url = dt.strftime("%Y/%m/%d")
        # date_db = dt.strftime(self.date_storage_format)

        url = self.data_url + "/matches/{}/".format(dt.strftime("%Y/%m/%d"))
        self.logger.info("URL for day {} is: {}".format(date_url, url))
        
        parsed_html = self.parse_html(url)

        group_heads = parsed_html.find_all("tr", attrs={'class':'group-head'})

        self.logger.info("Found {} group heads".format(len(group_heads)))

        for group_head in group_heads:
            stage_value = group_head.get("stage-value")
            competition_id = group_head.get("id").replace("date_matches-", "") 
            
            if "-" in competition_id:
                competition_id = competition_id[:competition_id.index("-")]
            
            self.logger.debug("Retrieving matches for 'stage_value:{}' 'competition_id:{}'".format(stage_value, competition_id))

            matches_html = self.crawl_match_data(stage_value, competition_id, dt)

            # self.logger.debug("HTML content: \n{}".format(matches_html.prettify().encode("utf-8")))

            matches = matches_html.find_all("tr", attrs={'class':'no-date-repetition', 'class':'match'})
            
            self.logger.debug("{} matches retrieved".format(len(matches)))
            
            for match in matches:
                # If future match -> class status
                # else -> class score
                future_match = match.find_all("td", attrs={'class':'status'})

                # If contains span, not cancelled or postponed and contains time instead
                if len(future_match) > 0 and future_match[0].a.span:
                    self.store_match(match, dt)

    def clear_old_bets(self):
        self.logger.info(u"Cleaning old bets stored in table '{}'".format(self.bets_table_name))

        db = SQLDb(self.db_name)

        stored_bets = db.execute_group(u"SELECT id, match_url, day, hour FROM '{}'".format(self.bets_table_name))

        now_dt = datetime.now()

        for stored_bet in stored_bets:
            dt = datetime.strptime(u"{} {}".format(stored_bet[2], stored_bet[3]), u"{} {}".format(self.date_storage_format, self.time_storage_format))
            if dt < now_dt and (now_dt - dt).total_seconds() > self.discard_matches_days * 24 * 60 * 60:
                self.logger.info(u"Bet '{}' in past. Discarding it".format(stored_bet[0]))
                
                self.logger.info(u"Discarding corresponding match")
                db.execute(u"""
                    DELETE FROM '{}'
                    WHERE match_url = '{}';
                """.format(self.matches_table_name, stored_bet[1]))
                
                self.logger.info(u"Discarding corresponding analysis")
                db.execute(u"""
                    DELETE FROM '{}'
                    WHERE match_url = '{}';
                """.format(self.analyses_table_name, stored_bet[1]))
                
                db.execute(u"""
                    DELETE FROM '{}'
                    WHERE id = {};
                """.format(self.bets_table_name, stored_bet[0]))

    def clear_old_matches(self):
        self.logger.info(u"Cleaning old matches stored in table '{}'".format(self.matches_table_name))

        db = SQLDb(self.db_name)

        stored_matches = db.execute_group(u"SELECT match_url, day, hour FROM '{}'".format(self.matches_table_name))

        stored_bets_urls = db.execute_group(u"SELECT match_url FROM '{}'".format(self.bets_table_name))

        unreferenced_matches = [x for x in stored_matches if x[0] not in [y[0] for y in stored_bets_urls]]

        self.logger.info(u"Found {} matches and {} bets yielding {} unreferenced matches".format(len(stored_matches), len(stored_bets_urls), len(unreferenced_matches)))

        now_dt = datetime.now()

        for stored_match in unreferenced_matches:
            dt = datetime.strptime(u"{} {}".format(stored_match[1], stored_match[2]), u"{} {}".format(self.date_storage_format, self.time_storage_format))
            if dt < now_dt and (now_dt - dt).total_seconds() > self.discard_matches_days * 24 * 60 * 60:
                self.logger.info(u"Match '{}' in past and unreferenced. Discarding it".format(stored_match[0]))
                db.execute(u"""
                    DELETE FROM '{}'
                    WHERE match_url = '{}';
                """.format(self.matches_table_name, stored_match[0]))
                
    def crawl_matches(self):
        self.clear_old_matches()
        today_dt = datetime(year=datetime.now().year, month=datetime.now().month, day=datetime.now().day)
        self.logger.info(u"Starting match crawling procedure. today_dt = {}".format(today_dt))

        for i in range(self.match_day_window):
            search_dt = today_dt + timedelta(days=i)
            self.crawl_matches_by_day(search_dt)

    def crawl_team_matches(self, team_id, home=True):
        filter_param = "home"
        
        if not home:
            filter_param = "away"

        payload = dict(
            block_id=urllib.quote('page_team_1_block_team_matches_5'),
            callback_params=urllib.quote("""{{"page":"0", "bookmaker_urls":{{"13":[{{"link":"http://www.bet365.com/home/?affiliate=365_371546","name":"Bet 365"}}]}},
                                              "block_service_id":"team_matches_block_teammatches",
                                              "team_id":{},
                                              "competition_id":"0",
                                              "filter":"home"}}""".format(team_id)),
            action=urllib.quote('filterMatches'),
            params=urllib.quote('{{"filter":"{}"}}'.format(filter_param))
        )

        request_url = self.data_url + '/a/block_team_matches?block_id={block_id}&callback_params={callback_params}&action={action}&params={params}'.format(**payload)

        data = self.parse_json(request_url)
        html = BeautifulSoup(data['commands'][0]['parameters']['content'].rstrip('\n'), 'html.parser')

        return html

    def crunch_matches_data(self, matches_data, away=False):
        result = dict(
            goals = 0,
            taken = 0,
            matches = 0,
            wins = 0,
            loses = 0,
            draws = 0
        )

        for match_data in matches_data:
            match_dt = datetime.strptime(match_data.parent.parent.find("td", attrs={'class':'full-date'}).string, '%d/%m/%y')
            
            # Three months, for now
            if (datetime.now() - match_dt).total_seconds() > self.old_match_tolerance:
                continue

            result["matches"] += 1

            if "win" in match_data.get("class")[0]:
                result["wins"] += 1
            elif "draw" in match_data.get("class")[0]:
                result["draws"] += 1
            else:
                result["loses"] += 1

            if match_data.span:
                goals = match_data.span.next_sibling.replace("-", "").split()                
            else:
                goals = match_data.string.replace("-", "").split()

            if away:
                result["goals"] += int(re.sub(r"[^0-9]", "", goals[1]))
                result["taken"] += int(re.sub(r"[^0-9]", "", goals[0]))
            else:
                result["goals"] += int(re.sub(r"[^0-9]", "", goals[0]))
                result["taken"] += int(re.sub(r"[^0-9]", "", goals[1]))

        return result

    def analyse_match(self, match_url):
        self.logger.info("Initializing analysis of match {}".format(match_url))

        db = SQLDb(self.db_name)

        match = db.execute(u"""
                            SELECT * FROM '{}'
                            WHERE match_url = '{}'
                           """.format(self.matches_table_name,
                                      match_url))
        
        home_url = match[3]
        visit_url = match[4]

        match_html = self.parse_html(match[0])
        home_id = self.team_id_from_url(match[3])
        visit_id = self.team_id_from_url(match[4])

        analysis_result = dict(
            home_pos = -1,
            visit_pos = -1,
            delta_pos = -1,
            two_tables = 0,
            
            home_goals_home = 0,
            home_taken_home = 0,
            home_matches_home = 0,
            home_wins_home = 0,
            home_loses_home = 0,
            home_draws_home = 0,
            
            home_goals_visit = 0,
            home_taken_visit = 0,
            home_matches_visit = 0,
            home_wins_visit = 0,
            home_loses_visit = 0,
            home_draws_visit = 0,
            
            visit_goals_home = 0,
            visit_taken_home = 0,
            visit_matches_home = 0,
            visit_wins_home = 0,
            visit_loses_home = 0,
            visit_draws_home = 0,
            
            visit_goals_visit = 0,
            visit_taken_visit = 0,
            visit_matches_visit = 0,
            visit_wins_visit = 0,
            visit_loses_visit = 0,
            visit_draws_visit = 0
        )

        try:
            team_ranks = match_html.find_all("tr", attrs={'class':'highlight'})

            # Ranking stuff
            if len(team_ranks) == 2:
                tables = match_html.find_all("table", attrs={'class':'leaguetable'})

                if len(tables) == 2:
                    table_urls = match_html.find_all("div", attrs={'class':'block_team_table-wrapper'})
                    table_urls = [x.h2.a.get("href") for x in table_urls]
                    if table_urls[0] != table_urls[1]:
                        analysis_result["two_tables"] = 1
                
                for team_rank in team_ranks:
                    href = team_rank.find("a").get("href")
                    rank = int(team_rank.find("td", attrs={'class':'rank'}).string)
                    
                    if href in home_url:
                        analysis_result["home_pos"] = rank

                    if href in visit_url:
                        analysis_result["visit_pos"] = rank
                
                analysis_result["delta_pos"] = abs(analysis_result["home_pos"] - analysis_result["visit_pos"])
            
            # Recent matches stuff
            home_matches_home = self.crawl_team_matches(home_id).find_all("a", attrs={'class': re.compile(r"result-(win|loss|draw)")})
            home_matches_away = self.crawl_team_matches(home_id, False).find_all("a", attrs={'class': re.compile(r"result-(win|loss|draw)")})
            visit_matches_home = self.crawl_team_matches(visit_id).find_all("a", attrs={'class': re.compile(r"result-(win|loss|draw)")})
            visit_matches_away = self.crawl_team_matches(visit_id, False).find_all("a", attrs={'class': re.compile(r"result-(win|loss|draw)")})
            
            result = self.crunch_matches_data(home_matches_home)
            analysis_result["home_goals_home"] = result["goals"]
            analysis_result["home_taken_home"] = result["taken"]
            analysis_result["home_matches_home"] = result["matches"]
            analysis_result["home_wins_home"] = result["wins"]
            analysis_result["home_loses_home"] = result["loses"]
            analysis_result["home_draws_home"] = result["draws"]

            result = self.crunch_matches_data(home_matches_away, away=True)
            analysis_result["home_goals_visit"] = result["goals"]
            analysis_result["home_taken_visit"] = result["taken"]
            analysis_result["home_matches_visit"] = result["matches"]
            analysis_result["home_wins_visit"] = result["wins"]
            analysis_result["home_loses_visit"] = result["loses"]
            analysis_result["home_draws_visit"] = result["draws"]

            result = self.crunch_matches_data(visit_matches_home)
            analysis_result["visit_goals_home"] = result["goals"]
            analysis_result["visit_taken_home"] = result["taken"]
            analysis_result["visit_matches_home"] = result["matches"]
            analysis_result["visit_wins_home"] = result["wins"]
            analysis_result["visit_loses_home"] = result["loses"]
            analysis_result["visit_draws_home"] = result["draws"]

            result = self.crunch_matches_data(visit_matches_away, away=True)
            analysis_result["visit_goals_visit"] = result["goals"]
            analysis_result["visit_taken_visit"] = result["taken"]
            analysis_result["visit_matches_visit"] = result["matches"]
            analysis_result["visit_wins_visit"] = result["wins"]
            analysis_result["visit_loses_visit"] = result["loses"]
            analysis_result["visit_draws_visit"] = result["draws"]
            
            self.logger.info("Analysis finished. Storing it".format(match_url))

            if db.row_exists(self.analyses_table_name, u"match_url = '{}'".format(match_url)):
                db.execute(u"""
                    DELETE FROM '{}'
                    WHERE match_url = '{}';
                """.format(self.analyses_table_name, match_url))

            db.execute(u"""
                INSERT INTO '{}' (match_url,                    
                                  h_pos, v_pos, d_pos, two_tables,
                                  hgh, hth, hmh, hwh, hlh, hdh,
                                  hgv, htv, hmv, hwv, hlv, hdv,
                                  vgh, vth, vmh, vwh, vlh, vdh,
                                  vgv, vtv, vmv, vwv, vlv, vdv)
                VALUES ('{}', {home_pos}, {visit_pos}, {delta_pos}, {two_tables},
                              {home_goals_home}, {home_taken_home}, {home_matches_home}, {home_wins_home}, {home_loses_home}, {home_draws_home},
                              {home_goals_visit}, {home_taken_visit}, {home_matches_visit}, {home_wins_visit}, {home_loses_visit}, {home_draws_visit},            
                              {visit_goals_home}, {visit_taken_home}, {visit_matches_home}, {visit_wins_home}, {visit_loses_home}, {visit_draws_home},
                              {visit_goals_visit}, {visit_taken_visit}, {visit_matches_visit}, {visit_wins_visit}, {visit_loses_visit}, {visit_draws_visit});
            """.format(self.analyses_table_name, match_url, **analysis_result))
        except Exception as e:
            self.logger.error(u"Couldn't analyse match. Error: {}".format(e.message))

    # ESPORTENET CRAWLER METHODS
    def crawl_bets(self):
        self.logger.info("Starting bet crawling procedure")

        self.clear_old_bets()

        db = SQLDb(self.db_name)

        data = self.parse_json(self.bets_api_url + urllib.quote(self.bets_params.format(datetime.now().year, datetime.now().month, datetime.now().day)))

        self.logger.info("Found {} bets. Analysing them now".format(len(data)))

        for bet in data:
            try:
                if "basquete" not in bet["camp_nome"].lower():                
                    bet_dt = datetime.strptime(bet["dt_hr_ini"], '%Y-%m-%dT%H:%M:00')

                    bet_hour = bet_dt.strftime(self.time_storage_format)

                    if bet_hour == "23:59":
                        bet_dt = bet_dt + timedelta(minutes=1)

                    date_matches = db.execute_group(u"""SELECT match_url, a_name, b_name, a_url, b_url, hour, day FROM '{}'
                                                        WHERE day = '{}'
                                                    """.format(self.matches_table_name,
                                                               bet_dt.strftime(self.date_storage_format)))

                    if not len(date_matches) > 0:
                        self.logger.debug(u"No matches found for bet at dt: '{}'. Skipping it".format(bet_dt.strftime("{} {}".format(self.date_storage_format, self.time_storage_format))))
                        continue

                    match_dts = [datetime.strptime("{} {}".format(x[6], x[5]), "{} {}".format(self.date_storage_format, self.time_storage_format)) for x in date_matches]

                    close_matches = []

                    for index, match_dt in enumerate(match_dts):
                        if abs((bet_dt - match_dt).total_seconds()) <= self.match_hour_threshold * 60 * 60:
                            close_matches.append(date_matches[index])                    

                    if not len(close_matches) > 0:
                        self.logger.debug(u"No matches within time tolerance for bet at date {}".format(bet_dt))
                        continue
                                    
                    bet_id = bet["camp_jog_id"]

                    bet_delta_rate = abs(bet["taxa_c"] - bet["taxa_f"])

                    team_h, age_h = self.filter_team_name(bet["casa_time"])
                    team_v, age_v = self.filter_team_name(bet["visit_time"])

                    close_matches_team_h = [x[1].lower() for x in close_matches]
                    close_matches_team_v = [x[2].lower() for x in close_matches]
                    scores_h = list_similarity(team_h.lower(), close_matches_team_h)
                    scores_v = list_similarity(team_v.lower(), close_matches_team_v)

                    # This line and the next for loop are supposed to merge scores_h and scores_v
                    scores = list(scores_h)
                    
                    for score_v in scores_v:
                        index = [i for i, x in enumerate(scores) if x[1] == score_v[1]]
                        if len(index) > 0:
                            scores[index[0]] = (scores[index[0]][0] + score_v[0], score_v[1])
                        else:
                            scores.append(score_v)

                    # Most likely match is the one with maximum score
                    likely_match = [(close_matches[x[1]], x[0] / 2) for x in sorted(scores, reverse=True)][0]
                    
                    # Store bet in database (and update if repeated)
                    self.logger.info(u"Storing bet #{}".format(bet_id))
                    
                    if db.row_exists(self.bets_table_name, u"id = '{}'".format(bet_id)):
                        db.execute(u"""
                            DELETE FROM '{}'
                            WHERE id = {};
                        """.format(self.bets_table_name, bet_id))
                    
                    db.execute(u"""
                        INSERT INTO '{}' (id,
                                          home_name, 
                                          home_rate, 
                                          visit_name, 
                                          visit_rate, 
                                          delta_rate, 
                                          draw_rate, 
                                          hour,
                                          day,
                                          match_url,
                                          fit_score)
                        VALUES ({}, '{}', {}, '{}', {}, {}, {}, '{}', '{}', '{}', {});
                    """.format(self.bets_table_name, 
                               bet_id, 
                               bet["casa_time"].replace("'", ""), 
                               bet["taxa_c"], 
                               bet["visit_time"].replace("'", ""), 
                               bet["taxa_f"], 
                               bet_delta_rate, 
                               bet["taxa_e"],
                               "{}".format(bet_dt.strftime(self.time_storage_format)),
                               "{}".format(bet_dt.strftime(self.date_storage_format)),
                               likely_match[0][0],
                               likely_match[1]))

                    self.analyse_match(likely_match[0][0])
            except Exception as e:
                self.logger.error(u"Couldn't crawl bet #{}. {}. Skipping it".format(bet["camp_jog_id"], e.message))
                continue

if __name__ == '__main__':
    crawler = Crawler()

    # r = crawler.crawl_match_score("http://br.soccerway.com/matches/2017/02/12/chile/primera-b/provincial-curico-unido/club-de-desportes-cobreloa/2381691/?ICID=HP_MS_64_03")
    # pprint(r)

    # db = SQLDb(crawler.db_name)
    
    # stored_matches = db.execute_group(u"SELECT match_url, day, hour FROM '{}'".format(crawler.matches_table_name))
    # stored_bets_urls = db.execute_group(u"SELECT match_url FROM '{}'".format(crawler.bets_table_name))
    # unreferenced_matches = [x for x in stored_matches if x[0] not in [y[0] for y in stored_bets_urls]]

    # crawler.crawl_matches()
    # crawler.crawl_bets()
        
# Testes:
# distancia na tabela
# ultimos 5 jogos (considerando casa/campeonato)
# quantidade de gols dentro/fora (mesmo tempo)
# vitorias/derrotas
# ganha muito em casa e perde muito fora é um indicativo importante (comparar isso)
# h2h (no mesmo ano?)
