# -*- coding: utf-8 -*-

import urllib2
from BeautifulSoup import BeautifulSoup
import json
import urllib
from datetime import datetime

# bet_url = "http://www.esportenet.net/"
bet_url = "http://www.esportenet.net/futebolapi/api/CampJogos?"
bet_params = "$filter=status eq 0 and ativo eq 1 and cancelado ne 1 and camp_ativo eq 1 and esporte_ativo eq 1 and placar_c eq null and placar_f eq null and qtd_odds gt 0 and qtd_main_odds gt 0 and (taxa_c gt 0 or taxa_f gt 0) and esporte_id eq 1 and dt_hr_ini le datetime'{0}-{1}-{2}T23:59:59'&$orderby=camp_nome,dt_hr_ini,camp_jog_id".format(datetime.now().year, datetime.now().month, datetime.now().day)

data_url = "http://br.soccerway.com"
team_params = "/teams/club-teams/"

def parse_html(url):
	response = urllib2.urlopen(url)
	html = response.read()
	return BeautifulSoup(html)

def parse_json(url):
	response = urllib2.urlopen(url)
	return json.load(response)

def crawl_bets():
	data = parse_json(bet_url + urllib.quote_plus(bet_params))

	for bet in data:
		print bet['camp_nome']

def build_team_database():
	parsed_html = parse_html(data_url + team_params)
	
	ul = parsed_html.body.find('ul', attrs={'class':'areas'})
	for content in ul.contents:
		if content == '\n':
			continue
		print content
		print len(content.contents)
	
	# print ul
	# li = ul.find_all('li')
	# print len(li)
	# print li
	# print len(ul)

	# print ul.find('li')

def crawl_team(url):
	return 0

def crawl_matches(url):
	return 0

def crawl_match(url):
	return 0

build_team_database()

# for bet in data:
# 	print bet['camp_nome']

# with open('output.json', 'w') as f:
# 	f.write(json.dumps(data, indent=4, sort_keys=True))
# 	f.close()

# crawl_bets()

# response = urllib2.urlopen("http://int.soccerway.com/teams/england/chelsea-football-club/661/")
# html = response.read()

# parsed_html = BeautifulSoup(html)
# print parsed_html.body.find('div', attrs={'id':'subheading'}).h1.string

# with open("output.html", "w") as f:
	# f.write(html)
	# f.close()

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