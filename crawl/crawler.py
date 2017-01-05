import urllib2
from BeautifulSoup import BeautifulSoup

response = urllib2.urlopen("http://int.soccerway.com/teams/england/chelsea-football-club/661/")
html = response.read()

parsed_html = BeautifulSoup(html)
print parsed_html.body.find('div', attrs={'id':'subheading'}).h1.string

