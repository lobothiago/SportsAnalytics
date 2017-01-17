# -*- coding: utf-8 -*-

from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, Job
from datetime import datetime, timedelta
from my_logger import MyLogger
from my_config_reader import MyConfigReader
from my_db import SQLDb

telegram_section = "telegram"
logging_section = "logging"
db_section = "db"

config = MyConfigReader()
logger = MyLogger("esportenet_bot.py")

token_file = config.get(telegram_section, "token_file")
subscription_password = config.get(telegram_section, "subscription_password")
digest_schedule_hour = config.get(telegram_section, "digest_schedule_hour")

db_name = config.get(db_section, "db_name")
subscribers_table_name = config.get(db_section, "subscribers_table_name")

def read_token():
	logger.info("Attempting to read bot token")
	
	try:
		with open(token_file, "r") as f:
			bot_token = [x.rstrip('\n') for x in f.readlines()][0]
			return bot_token
	except Exception as e:
		logger.critical("Couldn't read bot token: {}".format(e.message))

	return None

def callback_digest(bot, job):
	logger.info("Sending digest message")
	
	db = SQLDb(db_name)
	
	chat_ids = [row[0] for row in db.execute_group("SELECT id FROM {};".format(subscribers_table_name))]
	
	for chat_id in chat_ids:
		bot.send_message(chat_id=chat_id,
						 text=u"Essa mensagem chega a cada dia às 08:00!\n")

def add_subscription(sub_id, sub_name):
	db = SQLDb(db_name)

	if db.row_exists(subscribers_table_name, "id={}".format(sub_id)):
			return u"Assinatura já existente.\n"

	db.execute(""" 
		INSERT INTO {} (id, username)
		VALUES ({}, '{}');
	""".format(subscribers_table_name, sub_id, sub_name))

	return u"Assinatura adicionada com sucesso.\n"

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
	
def start(bot, update):
	me = bot.get_me()

	msg = u"Oi!\n"
	msg += u"Sou o {0}.\n".format(me.first_name)
	msg += u"O que você quer fazer?\n\n"
	msg += u"/start - Exibe essa mensagem\n"
	msg += u"/subscribe <senha> - Inscreve você na lista de receptores de apostas\n\n"
	
	bot.send_message(chat_id=update.message.chat_id,
	                 text=msg)

def test(bot, update):
	db = SQLDb(db_name)

	chat_ids = [row[0] for row in db.execute_group("SELECT id FROM {};".format(subscribers_table_name))]
	
	logger.debug("Found {} chat ids in database".format(len(chat_ids)))
	
	for chat_id in chat_ids:
		logger.debug("Sending test message to id: {}".format(chat_id))
		bot.send_message(chat_id=chat_id,
						 text=u"Mensagem de teste.\n")

def unknown(bot, update):
	msg = u"Desculpe, esse comando não parece existir."
	bot.send_message(chat_id=update.message.chat_id,
	                 text=msg)
	start(bot, update)

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

	start_handler = CommandHandler('start', start)
	dispatcher.add_handler(start_handler)

	subscribe_handler = CommandHandler('subscribe', subscribe, pass_args=True)
	dispatcher.add_handler(subscribe_handler)

	test_handler = CommandHandler('test', test)
	dispatcher.add_handler(test_handler)

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