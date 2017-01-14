# -*- coding: utf-8 -*-

import urllib2
from bs4 import BeautifulSoup
import json
import urllib
from datetime import datetime
from pprint import pprint
import logging

# bet_url = "http://www.esportenet.net/"
bet_url = "http://www.esportenet.net/futebolapi/api/CampJogos?"
bet_params = "$filter=status eq 0 and ativo eq 1 and cancelado ne 1 and camp_ativo eq 1 and esporte_ativo eq 1 and placar_c eq null and placar_f eq null and qtd_odds gt 0 and qtd_main_odds gt 0 and (taxa_c gt 0 or taxa_f gt 0) and esporte_id eq 1 and dt_hr_ini le datetime'{0}-{1}-{2}T23:59:59'&$orderby=camp_nome,dt_hr_ini,camp_jog_id".format(datetime.now().year, datetime.now().month, datetime.now().day)

data_url = "http://br.soccerway.com"
team_params = "/teams/club-teams/"

database = {}

def parse_html(url):
	stop = False
	while not stop:
		try:
			response = urllib2.urlopen(url)
			stop = True
		except Exception:
			stop = False
	html = response.read()
	return BeautifulSoup(html, 'html.parser')

def parse_json(url):
	stop = False
	while not stop:
		try:
			response = urllib2.urlopen(url)
			stop = True
		except Exception:
			stop = False
	return json.load(response)

def crawl_bets():
	data = parse_json(bet_url + urllib.quote_plus(bet_params))

	for bet in data:
		print bet['camp_nome']

	with open('output.json', 'w') as f:
		f.write(json.dumps(data, indent=4, sort_keys=True))
		f.close()

def build_team_database():
	parsed_html = parse_html(data_url + team_params)

	# For each country
	country_ids = [x.get('data-area_id') for x in parsed_html.body.find('ul', attrs={'class':'areas'}).find_all('li', attrs={'class':'expandable'})]
	for country_id in country_ids:
		# Request comps by means of country_id
		payload_country = dict(
			block_id=urllib.quote('page_teams_1_block_teams_index_club_teams_2'),
			callback_params=urllib.quote('{"level":1}'),
			action=urllib.quote('expandItem'),
			params=urllib.quote('{{"area_id":"{0}","level":2,"item_key":"area_id"}}'.format(country_id))
		)
		comps_request_url = data_url + '/a/block_teams_index_club_teams?block_id={block_id}&callback_params={callback_params}&action={action}&params={params}'.format(**payload_country)
		comps_data = parse_json(comps_request_url)		
		comps_html = BeautifulSoup(comps_data['commands'][0]['parameters']['content'].rstrip('\n'), 'html.parser')
		# Find all comp ids
		comp_ids = [x.get('data-competition_id') for x in comps_html.find_all('li')]
		# For each comp_id
		for comp_id in comp_ids:
			# Request teams by means of comp_id
			payload_comp = dict(
				block_id=urllib.quote('page_teams_1_block_teams_index_club_teams_2'),
				callback_params=urllib.quote('{"level":"3"}'),
				action=urllib.quote('expandItem'),
				params=urllib.quote('{{"competition_id":"{0}","level":3,"item_key":"competition_id"}}'.format(comp_id))
			)
			teams_request_url = data_url + '/a/block_teams_index_club_teams?block_id={block_id}&callback_params={callback_params}&action={action}&params={params}'.format(**payload_comp)
			teams_data = parse_json(teams_request_url)
			teams_html = BeautifulSoup(teams_data['commands'][0]['parameters']['content'].rstrip('\n'), 'html.parser')
			# Find all team URLs
			links = teams_html.find_all('a')
			team_urls = [data_url + x.get('href') for x in links if not 'women' in x.get('href')]
			team_names = [x.string for x in links if not 'women' in x.get('href')]
			
			for index, url in enumerate(team_urls):
				if not url in database:
					database[url] = [team_names[index]]
				else:
					database[url].append(team_names[index])

		pprint(database)

		break

def crawl_team(url):
	return 0

def crawl_matches(url):
	return 0

def crawl_match(url):
	return 0

build_team_database()

# Team Page:
# 
# -> Title: div id='subheading'
# -> id: from URL
# -> Matches: URL/matches
# 
#

# Testes:
# distancia na tabela
# ultimos 5 jogos (considerando casa/campeonato)
## quantidade de gols dentro/fora (mesmo tempo)
## vitorias/derrotas
## ganha muito em casa e perde muito fora é um indicativo importante (comparar isso)

# h2h (no mesmo ano?)