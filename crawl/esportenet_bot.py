# -*- coding: utf-8 -*-

from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, Job
from telegram import ParseMode
from datetime import datetime, timedelta
from my_logger import MyLogger
from my_config_reader import MyConfigReader
from my_db import SQLDb
from crawler import Crawler

db_section = "db"
logging_section = "logging"
crawler_section = "crawler"
telegram_section = "telegram"

config = MyConfigReader()
logger = MyLogger("esportenet_bot.py")

token_file = config.get(telegram_section, "token_file")
subscription_password = config.get(telegram_section, "subscription_password")
digest_schedule_hour = config.get(telegram_section, "digest_schedule_hour")
match_crawl_hour = config.get(telegram_section, "match_crawl_hour")
bets_crawl_hour = config.get(telegram_section, "bets_crawl_hour")
default_threshold = float(config.get(telegram_section, "default_threshold"))
list_page_size = int(config.get(telegram_section, "list_page_size"))

db_name = config.get(db_section, "db_name")
subscribers_table_name = config.get(db_section, "subscribers_table_name")
teams_table_name = config.get(db_section, "teams_table_name")
bets_table_name = config.get(db_section, "bets_table_name")
matches_table_name = config.get(db_section, "matches_table_name")
analyses_table_name = config.get(db_section, "analyses_table_name")
date_storage_format = config.get(db_section, "date_storage_format")
time_storage_format = config.get(db_section, "time_storage_format")

match_day_window = int(config.get(crawler_section, "match_day_window"))
old_match_tolerance = int(config.get(crawler_section, "old_match_tolerance"))

crawler = Crawler()

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

def build_digest_message(days_to_show=match_day_window, delta_threshold=default_threshold):
	db = SQLDb(db_name)

	msgs = []
	valid_dates = []
	dt = datetime.now()
	
	for i in range(days_to_show):
		dt_new = dt + timedelta(days=i)
		valid_dates.append(dt_new.strftime(date_storage_format))

	all_bets = db.execute_group(u"""SELECT id, home_name, home_rate, 
											   visit_name, visit_rate,
											   delta_rate, draw_rate,
											   hour, day, match_url,
											   fit_score FROM '{}'""".format(bets_table_name))

	valid_bets = [x for x in all_bets if x[8] in valid_dates 
									  and x[5] > delta_threshold 
									  and datetime.strptime(u"{} {}".format(x[8], x[7]), u"{} {}".format(date_storage_format, time_storage_format)) > datetime.now()]

	bet_dates = [[y for y in valid_bets if y[8] == x] for x in valid_dates]

	msgs.append(u"Mostrando apostas com delta > {} para o(s) próximo(s) {} dia(s).".format(delta_threshold, days_to_show))

	for index, bet_date in enumerate(bet_dates):
		msgs.append(u"{} apostas para o dia: '{}'".format(len(bet_date), valid_dates[index]))
		msg = u""

		sorted_bets = sorted(bet_date, key=lambda x: x[5], reverse=True)

		for counter, bet in enumerate(sorted_bets):
			msg += u"{} - {}: {} x {}\n".format(bet[0], bet[5], bet[1], bet[3])

			if counter % 10 == 0 and counter != 0:
				if len(msg) != 0:
					msgs.append(msg)
				msg = u""

		if len(msg) != 0:
			msgs.append(msg)
	
	msgs.append(u"Envie '/expand número_da_aposta' para saber mais sobre uma das apostas.\n")
	return msgs

def build_bet_expand_message(bet_id):
	db = SQLDb(db_name)

	bet = db.execute(u"""
						SELECT id, home_name, home_rate, 
							    visit_name, visit_rate,
							    delta_rate, draw_rate,
							    hour, day, match_url,
							    fit_score 
					 	FROM '{}'
					    WHERE id={}
					  """.format(bets_table_name, bet_id))

	analysis = db.execute(u"""
			                SELECT match_url,                    
			                       h_pos, v_pos, d_pos, two_tables,
			                       hgh, hth, hmh, hwh, hlh, hdh,
			                       hgv, htv, hmv, hwv, hlv, hdv,
			                       vgh, vth, vmh, vwh, vlh, vdh,
			                       vgv, vtv, vmv, vwv, vlv, vdv
			                FROM '{}'
			                WHERE match_url='{}'
			               """.format(analyses_table_name, bet[9]))

	msg = u"{}:\n*{} x {}*\nC: {} E: {} V: {}\nDelta: {}\n{} {}\n\n".format(bet[0], bet[1], bet[3], bet[2], bet[6], bet[4], bet[5], bet[8], bet[7])

	msg += u"Partida do soccerway.com mais provável ({0:.2f}% de probabilidade):\n{1}\n\n".format(bet[10] * 100.0, bet[9])
		
	if analysis[3] != -1:
		msg += u"*Posição na tabela:*\n"
		if analysis[4]:
			msg += u"Times em capeonatos diferentes!\n"
		msg += u"{}: {}\n".format(bet[1], analysis[1])
		msg += u"{}: {}\n".format(bet[3], analysis[2])
		msg += u"Delta: {}\n\n".format(analysis[3])

	msg += u"*Análise das últimas partidas (últimos {} meses):*\n".format(old_match_tolerance / (24 * 60 * 60 * 30))
	msg += u"*{} - casa*\n".format(bet[1])
	msg += u"Gols feitos em casa: {}\n".format(analysis[5])
	msg += u"Gols sofridos em casa: {}\n".format(analysis[6])
	
	try:
		ratio = float(analysis[5]) / float(analysis[6])
	except Exception:
		ratio = float('Inf')

	msg += u"Razão em casa: {0:.2f}\n\n".format(ratio)
	msg += u"Gols feitos fora: {}\n".format(analysis[11])
	msg += u"Gols sofridos fora: {}\n".format(analysis[12])
	
	try:
		ratio = float(analysis[11]) / float(analysis[12])
	except Exception:
		ratio = float('Inf')

	msg += u"Razão fora: {0:.2f}\n\n".format(ratio)

	msg += u"Vitórias/empates/derrotas em casa: {}/{}/{}\n".format(analysis[8], analysis[10], analysis[9])
	msg += u"Vitórias/empates/derrotas fora: {}/{}/{}\n\n".format(analysis[14], analysis[16], analysis[15])

	msg += u"*{} - fora*\n".format(bet[3])
	msg += u"Gols feitos em casa: {}\n".format(analysis[17])
	msg += u"Gols sofridos em casa: {}\n".format(analysis[18])

	try:
		ratio = float(analysis[17]) / float(analysis[18])
	except Exception:
		ratio = float('Inf')

	msg += u"Razão em casa: {0:.2f}\n\n".format(ratio)
	msg += u"Gols feitos fora: {}\n".format(analysis[23])
	msg += u"Gols sofridos fora: {}\n".format(analysis[24])
	
	try:
		ratio = float(analysis[23]) / float(analysis[24])
	except Exception:
		ratio = float('Inf')

	msg += u"Razão fora: {0:.2f}\n\n".format(ratio)

	msg += u"Vitórias/empates/derrotas em casa: {}/{}/{}\n".format(analysis[20], analysis[22], analysis[21])
	msg += u"Vitórias/empates/derrotas fora: {}/{}/{}\n\n".format(analysis[26], analysis[28], analysis[27])
	
	return msg

# Callbacks ---------------------------------

def callback_digest(bot, job):
	logger.info("Sending digest message")
	
	db = SQLDb(db_name)
	
	chat_ids = [row[0] for row in db.execute_group("SELECT id FROM {};".format(subscribers_table_name))]
	
	for chat_id in chat_ids:
		logger.debug(u"Sending digest message to id: {}".format(chat_id))

		bot.send_message(chat_id=chat_id,
						 text=u"oi")

def callback_crawl_matches(bot, job):
	logger.info("Attempting to crawl matches data")
	
	try:
		crawler.crawl_matches()
	except Exception as e:
		logger.error("Couldn't crawl matches. Error: {}".format(e.message))

def callback_crawl_bets(bot, job):
	logger.info("Attempting to crawl bets data")

	try:
		crawler.crawl_bets()
	except Exception as e:
		logger.error("Couldn't crawl bets. Error: {}".format(e.message))

# Commands ---------------------------------

def show(bot, update, args):
	db = SQLDb(db_name)

	chat_id = str(update.message.chat_id)
	
	if db.row_exists(subscribers_table_name, u"id={}".format(chat_id)):	
		days_to_show = match_day_window
		delta_threshold = default_threshold

		if len(args) != 0:
			if len(args) == 2 or len(args) == 4:
				i = 0
				while i < len(args):
					if args[i] == "t":						
						i += 1
						try:
							delta_threshold = max(float(args[i]), 0)
						except Exception:
							bot.send_message(chat_id=update.message.chat_id,
								 		 	 text=u"Não foi possível encontrar um número após 't'")
							return	
					elif args[i] == "d":
						i += 1
						try:
							days_to_show = max(min(int(args[i]), match_day_window), 1)
						except Exception:
							bot.send_message(chat_id=update.message.chat_id,
								 		 	 text=u"Não foi possível encontrar um número após 'd'")
							return	
					else:
						bot.send_message(chat_id=update.message.chat_id,
								 		 text=u"Argumentos inválidos.")
						return
					i += 1
			else:
				bot.send_message(chat_id=update.message.chat_id,
								 text=u"Número de argumentos inválido.")
				return

		try:
			messages = build_digest_message(days_to_show, delta_threshold)

			logger.debug(u"Sending show message to id: {}".format(chat_id))

			for msg in messages:
				bot.send_message(chat_id=update.message.chat_id,
								 text=msg)
		except Exception as e:
			bot.send_message(chat_id=update.message.chat_id,
							 text=u"Erro: {}".format(e.message))
	else:
		bot.send_message(chat_id=update.message.chat_id,
						 text=u"Esse recurso só é disponível para assinantes.\n")

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

		if not db.row_exists(bets_table_name, u"id={}".format(bet_id)):
			bot.send_message(chat_id=update.message.chat_id,
		                 	 text=u"id inexistente.")
			return
			
		bot.send_message(chat_id=update.message.chat_id,
		                 text=build_bet_expand_message(bet_id),
		                 parse_mode=ParseMode.MARKDOWN)
	else:
		bot.send_message(chat_id=update.message.chat_id,
						 text=u"Esse recurso só é disponível para assinantes.\n")
	
def start(bot, update):
	me = bot.get_me()

	msg = u"Oi!\n"
	msg += u"Sou o {0}.\n".format(me.first_name)
	msg += u"O que você quer fazer?\n\n"
	msg += u"/start - Exibe essa mensagem\n"
	msg += u"/subscribe <senha> - Inscreve você na lista de receptores de apostas\n"
	msg += u"/show d numero_de_dias t taxa_minima - Mostra as apostas analisadas\n\n"
	
	bot.send_message(chat_id=update.message.chat_id,
	                 text=msg)

def test(bot, update):
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
	logger.info("Initializing Telegram Bot")
	# Connecting to Telegram API
	# Updater retrieves information and dispatcher connects commands 
	updater = Updater(token=read_token())
	dispatcher = updater.dispatcher
	job_q = updater.job_queue
	
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
	# Will happen every 12 hours seconds, starting from deltaseconds
	# job_q.put(Job(callback_digest, (12 * 60 * 60)), next_t=deltaseconds)
	
	logger.info("Crawling matches for bot startup...")
	job_q.put(Job(callback_crawl_matches, (8 * 60 * 60)), next_t=0)
	
	logger.info("Crawling bets for bot startup...")
	job_q.put(Job(callback_crawl_bets, (4 * 60 * 60)), next_t=0)

	start_handler = CommandHandler('start', start)
	dispatcher.add_handler(start_handler)

	subscribe_handler = CommandHandler('subscribe', subscribe, pass_args=True)
	dispatcher.add_handler(subscribe_handler)

	expand_handler = CommandHandler('expand', expand_bet, pass_args=True)
	dispatcher.add_handler(expand_handler)

	test_handler = CommandHandler('test', test)
	dispatcher.add_handler(test_handler)

	show_handler = CommandHandler('show', show, pass_args=True)
	dispatcher.add_handler(show_handler)

	unknown_handler = MessageHandler(Filters.command, unknown)
	dispatcher.add_handler(unknown_handler)

	logger.info("Bot will now start polling")
	updater.start_polling()
	updater.idle()

def db_init():
	logger.info("Initializing database: '{}'".format(db_name))

	db = SQLDb(db_name)
	
	if not db.table_exists(subscribers_table_name):
		logger.info(u"Table '{}' does not exist. Creating it now".format(subscribers_table_name))
		db.execute(""" 
			CREATE TABLE {} 
			( 
				id INTEGER PRIMARY KEY,
				username VARCHAR(25)
			);
		""".format(subscribers_table_name))

def init():
	db_init()
	bot_init()

if __name__ == '__main__':
	try:
		init()
	except Exception as e:
		logger.critical("Couldn't initialize bot: {}".format(e.message))