# -*- coding: utf-8 -*-

from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, Job
from telegram import ParseMode
from datetime import datetime, timedelta
from my_logger import MyLogger
from my_config_reader import MyConfigReader
from my_db import SQLDb
from crawler import Crawler
import pickle

telegram_section = "telegram"
logging_section = "logging"
crawler_section = "crawler"
db_section = "db"

config = MyConfigReader()
logger = MyLogger("esportenet_bot.py")

token_file = config.get(telegram_section, "token_file")
subscription_password = config.get(telegram_section, "subscription_password")
digest_schedule_hour = config.get(telegram_section, "digest_schedule_hour")
match_crawl_hour = config.get(telegram_section, "match_crawl_hour")
bets_crawl_hour = config.get(telegram_section, "bets_crawl_hour")

db_name = config.get(db_section, "db_name")
subscribers_table_name = config.get(db_section, "subscribers_table_name")

bet_rate_threshold = float(config.get(crawler_section, "bet_rate_threshold"))

crawler = Crawler()
bets_data = None

# Helper ---------------------------------

def read_token():
	logger.info("Attempting to read bot token")
	
	try:
		with open(token_file, "r") as f:
			bot_token = [x.rstrip('\n') for x in f.readlines()][0]
			return bot_token
	except Exception as e:
		logger.critical("Couldn't read bot token: {}".format(e.message))

	return None

def add_subscription(sub_id, sub_name):
	db = SQLDb(db_name)

	if db.row_exists(subscribers_table_name, "id={}".format(sub_id)):
			return u"Assinatura já existente.\n"

	db.execute(""" 
		INSERT INTO {} (id, username)
		VALUES ({}, '{}');
	""".format(subscribers_table_name, sub_id, sub_name))

	return u"Assinatura adicionada com sucesso.\n"

def build_digest_message():
	global bets_data

	msg = u"A diferença de taxas mínima atual é: {}\n\n".format(bet_rate_threshold)
	msg += u"Encontrei as seguintes {} apostas desde minha última atualização:\n\n".format(len(bets_data))
	
	for bet in bets_data:
		msg += u"{}: {} ({}) x {} ({}) - delta: {}\n".format(bet["id"], bet["home_name"], bet["home_rate"], bet["visit_name"], bet["visit_rate"], bet["delta_rate"])
	
	msg += u"\n Envie '/expand <id>' para saber mais sobre uma das apostas.\n"

	return msg

def build_bet_expand_message(bet_id):
	global bets_data

	bet = [x for x in bets_data if x["id"] == bet_id][0]

	msg = u"{}:\n*{} x {}*\n{} x {} - delta = {}\n\n".format(bet["id"], bet["home_name"], bet["visit_name"], bet["home_rate"], bet["visit_rate"], bet["delta_rate"])

	matched_match = bet["matches"][0]
	analysis_result = matched_match["analysis_result"]

	msg += u"Partida do soccerway.com mais provável ({0:.2f}% de probabilidade):\n{1}\n\n".format(matched_match["match_score"] * 100.0, matched_match["match_url"])
		
	if analysis_result["delta_pos"] != -1:
		msg += u"*Posição na tabela*:\n{}: {}\n{}: {}\nDelta: {}\n\n".format(bet["home_name"], analysis_result["home_pos"], bet["visit_name"], analysis_result["visit_pos"], analysis_result["delta_pos"])

	msg += u"*Análise das últimas partidas:*\n"
	msg += u"*{} - em casa*\n".format(bet["home_name"])
	msg += u"Gols feitos em casa: {}\n".format(analysis_result["home_goals_home"])
	msg += u"Gols sofridos em casa: {}\n".format(analysis_result["home_taken_home"])
	try:
		ratio = float(analysis_result["home_goals_home"]) / float(analysis_result["home_taken_home"])
	except Exception:
		ratio = float('Inf')
	msg += u"Razão em casa: {0:.2f}\n\n".format(ratio)
	msg += u"Gols feitos fora: {}\n".format(analysis_result["home_goals_visit"])
	msg += u"Gols sofridos fora: {}\n".format(analysis_result["home_taken_visit"])
	try:
		ratio = float(analysis_result["home_goals_visit"]) / float(analysis_result["home_taken_visit"])
	except Exception:
		ratio = float('Inf')
	msg += u"Razão fora: {0:.2f}\n\n".format(ratio)

	msg += u"Vitórias/derrotas em casa: {}/{}\n".format(analysis_result["home_wins_home"], (analysis_result["home_matches_home"] - analysis_result["home_wins_home"]))
	msg += u"Vitórias/derrotas fora: {}/{}\n\n".format(analysis_result["home_wins_visit"], (analysis_result["home_matches_visit"] - analysis_result["home_wins_visit"]))

	msg += u"*{} - fora*\n".format(bet["visit_name"])
	msg += u"Gols feitos em casa: {}\n".format(analysis_result["visit_goals_home"])
	msg += u"Gols sofridos em casa: {}\n".format(analysis_result["visit_taken_home"])
	try:
		ratio = float(analysis_result["visit_goals_home"]) / float(analysis_result["visit_taken_home"])
	except Exception:
		ratio = float('Inf')
	msg += u"Razão em casa: {0:.2f}\n\n".format(ratio)
	msg += u"Gols feitos fora: {}\n".format(analysis_result["visit_goals_visit"])
	msg += u"Gols sofridos fora: {}\n".format(analysis_result["visit_taken_visit"])
	try:
		ratio = float(analysis_result["visit_goals_visit"]) / float(analysis_result["visit_taken_visit"])
	except Exception:
		ratio = float('Inf')
	msg += u"Razão fora: {0:.2f}\n\n".format(ratio)

	msg += u"Vitórias/derrotas em casa: {}/{}\n".format(analysis_result["visit_wins_home"], (analysis_result["visit_matches_home"] - analysis_result["visit_wins_home"]))
	msg += u"Vitórias/derrotas fora: {}/{}\n\n".format(analysis_result["visit_wins_visit"], (analysis_result["visit_matches_visit"] - analysis_result["visit_wins_visit"]))
	
	return msg

# Callbacks ---------------------------------

def callback_digest(bot, job):
	logger.info("Sending digest message")
	
	db = SQLDb(db_name)
	
	chat_ids = [row[0] for row in db.execute_group("SELECT id FROM {};".format(subscribers_table_name))]
	
	msg = build_digest_message()

	for chat_id in chat_ids:
		logger.debug(u"Sending digest message to id: {}".format(chat_id))

		bot.send_message(chat_id=chat_id,
						 text=msg)

def callback_crawl_matches(bot, job):
	logger.info("Attempting to crawl matches data")
	
	try:
		crawler.crawl_matches()
	except Exception as e:
		logger.error("Couldn't crawl matches. Error: {}".format(e.message))

def callback_crawl_bets(bot, job):
	global bets_data
	logger.info("Attempting to crawl bets data")

	try:
		bets_data = crawler.crawl_bets()
	except Exception as e:
		logger.error("Couldn't crawl bets. Error: {}".format(e.message))

# Commands ---------------------------------

def show(bot, update):
	db = SQLDb(db_name)

	chat_id = str(update.message.chat_id)
	
	if db.row_exists(subscribers_table_name, u"id={}".format(chat_id)):	
		msg = build_digest_message()

		logger.debug(u"Sending show message to id: {}".format(chat_id))

		bot.send_message(chat_id=update.message.chat_id,
						 text=msg)
	else:
		bot.send_message(chat_id=update.message.chat_id,
						 text="Esse recurso só é disponível para assinantes.\n")

def subscribe(bot, update, args):
	sub_id = str(update.message.chat_id)
	sub_name = None
	
	if update.message.chat.type == 'private':
		sub_name = update.message.from_user.username
	else:
		sub_name = update.message.chat.title

	logger.info("Subscription attempt by: {} (id: {})".format(sub_name, sub_id))

	if len(args) > 0:
		logger.debug("Password given: '{}'. Expected '{}'".format(args[0], subscription_password))
	else:
		logger.debug("No password given. Expected '{}'".format(subscription_password))

	if len(args) > 0 and args[0] == subscription_password:
		result = add_subscription(sub_id, sub_name)
		bot.send_message(chat_id=update.message.chat_id,
	                 	 text=result)
		logger.info("Subscription attempt succeeded")
	else:
		bot.send_message(chat_id=update.message.chat_id,
	                 	 text=u"Senha incorreta.\n")
		logger.info("Subscription attempt failed")
	
def expand_bet(bot, update, args):
	global bets_data
	db = SQLDb(db_name)

	chat_id = str(update.message.chat_id)
	
	if db.row_exists(subscribers_table_name, u"id={}".format(chat_id)):	
		if len(args) < 1:
			bot.send_message(chat_id=update.message.chat_id,
		                 	 text=u"id não informado.")
			return

		try:
			bet_id = int(args[0])
		except Exception:
			bot.send_message(chat_id=update.message.chat_id,
		                 	 text=u"O id deve ser um valor numérico.")
			return

		if bet_id < 1 or bet_id > len(bets_data):
			ids = [x["id"] for x in bets_data]
			bot.send_message(chat_id=update.message.chat_id,
		                 	 text=u"O id deve estar entre {} e {}.".format(min(ids), max(ids)))
			return

		bot.send_message(chat_id=update.message.chat_id,
		                 text=build_bet_expand_message(bet_id),
		                 parse_mode=ParseMode.MARKDOWN)
	else:
		bot.send_message(chat_id=update.message.chat_id,
						 text="Esse recurso só é disponível para assinantes.\n")
	
def start(bot, update):
	me = bot.get_me()

	msg = u"Oi!\n"
	msg += u"Sou o {0}.\n".format(me.first_name)
	msg += u"O que você quer fazer?\n\n"
	msg += u"/start - Exibe essa mensagem\n"
	msg += u"/subscribe <senha> - Inscreve você na lista de receptores de apostas\n"
	msg += u"/show - Mostra as apostas analisadas\n\n"
	
	bot.send_message(chat_id=update.message.chat_id,
	                 text=msg)

def test(bot, update):
	global bets_data
	db = SQLDb(db_name)

	chat_ids = [row[0] for row in db.execute_group("SELECT id FROM {};".format(subscribers_table_name))]
	
	logger.debug("Found {} chat ids in database".format(len(chat_ids)))
		
	msg = build_digest_message()

	for chat_id in chat_ids:
		logger.debug("Sending test message to id: {}".format(chat_id))

		bot.send_message(chat_id=chat_id,
						 text=msg)

def unknown(bot, update):
	msg = u"Desculpe, esse comando não parece existir."
	bot.send_message(chat_id=update.message.chat_id,
	                 text=msg)
	start(bot, update)

# Inits ---------------------------------

def bot_init():
	global bets_data
	logger.info("Initializing Telegram Bot")
	# Connecting to Telegram API
	# Updater retrieves information and dispatcher connects commands 
	updater = Updater(token=read_token())
	dispatcher = updater.dispatcher
	job_q = updater.job_queue
	
	logger.info("Crawling matches for bot startup...")
	crawler.crawl_matches()

	logger.info("Crawling bets for bot startup...")
	bets_data = crawler.crawl_bets()

	# with open("bets.txt", "rb") as f:
	# 	bets_data = pickle.load(f)

	# Determine remaining seconds until next digest message (1 per day)
	current_day = datetime(year=datetime.today().year,
					 	   month=datetime.today().month,
					 	   day=datetime.today().day)
	logger.debug("Current day: {}".format(current_day))

	# #########
	target = current_day + timedelta(hours=int(digest_schedule_hour))
	logger.debug("Base digest target: {}".format(target))
	
	if target < datetime.today():
		target = target + timedelta(days=1)
		logger.debug("Target too early. Postpone one day: {}".format(target))
	
	deltaseconds = (target - datetime.today()).total_seconds()
	logger.info("Next digest: {} - deltaseconds: {}".format(target, deltaseconds))

	# Add new Job to the dispatcher's job queue.
	# Will happen every 24 hours seconds, starting from deltaseconds
	job_q.put(Job(callback_digest, (24 * 60 * 60)), next_t=deltaseconds)
	
	# #########
	target = current_day + timedelta(hours=int(match_crawl_hour))
	logger.debug("Base match crawl target: {}".format(target))
	
	if target < datetime.today():
		target = target + timedelta(days=1)
		logger.debug("Target too early. Postpone one day: {}".format(target))
	
	deltaseconds = (target - datetime.today()).total_seconds()
	logger.info("Next match crawl: {} - deltaseconds: {}".format(target, deltaseconds))

	# Add new Job to the dispatcher's job queue.
	# Will happen every 24 hours seconds, starting from deltaseconds
	job_q.put(Job(callback_crawl_matches, (24 * 60 * 60)), next_t=deltaseconds)
	
	# #########
	target = current_day + timedelta(hours=int(bets_crawl_hour))
	logger.debug("Base bets crawl target: {}".format(target))
	
	if target < datetime.today():
		target = target + timedelta(days=1)
		logger.debug("Target too early. Postpone one day: {}".format(target))
	
	deltaseconds = (target - datetime.today()).total_seconds()
	logger.info("Next bets crawl: {} - deltaseconds: {}".format(target, deltaseconds))

	# Add new Job to the dispatcher's job queue.
	# Will happen every 24 hours seconds, starting from deltaseconds
	job_q.put(Job(callback_crawl_bets, (24 * 60 * 60)), next_t=deltaseconds)

	start_handler = CommandHandler('start', start)
	dispatcher.add_handler(start_handler)

	subscribe_handler = CommandHandler('subscribe', subscribe, pass_args=True)
	dispatcher.add_handler(subscribe_handler)

	expand_handler = CommandHandler('expand', expand_bet, pass_args=True)
	dispatcher.add_handler(expand_handler)

	test_handler = CommandHandler('test', test)
	dispatcher.add_handler(test_handler)

	show_handler = CommandHandler('show', show)
	dispatcher.add_handler(show_handler)

	unknown_handler = MessageHandler(Filters.command, unknown)
	dispatcher.add_handler(unknown_handler)

	logger.info("Bot will now start polling")
	updater.start_polling()
	updater.idle()

def db_init():
	logger.info("Initializing database: '{}'".format(db_name))

	db = SQLDb(db_name)
	
	logger.info("Creating table: '{}'".format(subscribers_table_name))
	if not db.table_exists(subscribers_table_name):
		db.execute(""" 
			CREATE TABLE {} 
			( 
				id INTEGER PRIMARY KEY,
				username VARCHAR(25)
			);
		""".format(subscribers_table_name))
	else:
		logger.info("Table already existent")

def init():
	db_init()
	bot_init()

if __name__ == '__main__':
	try:
		init()
	except Exception as e:
		logger.critical("Couldn't initialize bot: {}".format(e.message))