import urllib2
from BeautifulSoup import BeautifulSoup

def crawl_team(url):
	return 0

def crawl_matches(url):
	return 0

def crawl_match(url):
	return 0

response = urllib2.urlopen("http://int.soccerway.com/teams/england/chelsea-football-club/661/")
html = response.read()

parsed_html = BeautifulSoup(html)
print parsed_html.body.find('div', attrs={'id':'subheading'}).h1.string

with open("output.html", "w") as f:
	f.write(html)
	f.close()

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