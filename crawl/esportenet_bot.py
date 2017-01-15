# -*- coding: utf-8 -*-

from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, Job
from os.path import exists
from datetime import datetime, timedelta
from my_logger import MyLogger
from my_config_reader import MyConfigReader
from my_db import SQLDb

telegram_section = "telegram"
logging_section = "logging"
db_section = "db"

config = MyConfigReader()
logger = MyLogger("esportenet_bot.py", config.get(logging_section, "log_path"))

subscribers_file = config.get(telegram_section, "subscribers_file")

# cursor.execute("""
# 	INSERT INTO subscribers (id, username)
#     VALUES (1, "William");
# """)

# cursor.execute(""" 
# 	INSERT INTO subscribers (id, username)
#     VALUES (2, "Shakespeare");
# """)

# cursor.execute("SELECT * FROM subscribers WHERE id=1 LIMIT 1;")
# print cursor.fetchone()

def read_token():
	logger.info("Attempting to read bot token")
	
	try:
		with open(config.get(telegram_section, "token_file"), "r") as f:
			bot_token = [x.rstrip('\n') for x in f.readlines()][0]
			return bot_token
	except Exception as e:
		logger.critical("Couldn't read bot token: {}".format(e.message))

	return None

def callback_digest(bot, job):
	logger.info("Sending digest message")

	if exists(subscribers_file):
		with open(subscribers_file, "r") as f:
			lines = [x.rstrip('\n') for x in f.readlines()]
			for chat_id in lines:
				bot.send_message(chat_id=chat_id,
								 text=u"Essa mensagem chega a cada dia às 08:00!\n")

def add_subscription(chat_id):
	if exists(subscribers_file):
		with open(subscribers_file, "r") as f:
			lines = [x.rstrip('\n') for x in f.readlines()]
			if str(chat_id) in lines:
				return u"Assinatura já existente.\n"

	if not exists(subscribers_file):
		with open(subscribers_file, "w") as f:
			f.write(str(chat_id))
			f.write("\n")
			f.close()
	else:
		with open(subscribers_file, "a") as f:
			f.write(str(chat_id))
			f.write("\n")
			f.close()

	return u"Assinatura adicionada com sucesso.\n"
	
def start(bot, update):
	me = bot.get_me()

	msg = u"Oi!\n"
	msg += u"Sou o {0}.\n".format(me.first_name)
	msg += u"O que você quer fazer?\n\n"
	msg += u"/start - Exibe essa mensagem\n"
	msg += u"/subscribe <senha> - Inscreve você na lista de receptores de apostas\n\n"
	
	bot.send_message(chat_id=update.message.chat_id,
	                 text=msg)

def subscribe(bot, update, args):
	if len(args) > 0 and args[0] == config.get(telegram_section, "subscription_password"):
		result = add_subscription(update.message.chat_id)
		bot.send_message(chat_id=update.message.chat_id,
	                 	 text=result)
		logger.info("Subscription attempt by: {} (id: {}) - succeeded".format(update.message.from_user.username, str(update.message.from_user.id)))
	else:
		bot.send_message(chat_id=update.message.chat_id,
	                 	 text=u"Senha incorreta.\n")
		logger.info("Subscription attempt by: {} (id: {}) - failed".format(update.message.from_user.username, str(update.message.from_user.id)))

def test(bot, update):
	with open(subscribers_file, "r") as f:
		lines = [x.rstrip('\n') for x in f.readlines()]
		for chat_id in lines:
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
	today = datetime(year=datetime.today().year,
					 month=datetime.today().month,
					 day=datetime.today().day)
	target = today + timedelta(days=1, hours=int(config.get(telegram_section, "digest_schedule_hour")))
	deltaseconds = (target - datetime.now()).total_seconds()
	logger.info("Next digest: {} - deltaseconds: {}".format(target, deltaseconds))

	# Add new Job to the dispatcher's job queue.
	# Will happen every deltaseconds seconds, starting from now
	job_q.put(Job(callback_digest, (24 * 60 * 60)), next_t=deltaseconds)

	start_handler = CommandHandler('start', start)
	dispatcher.add_handler(start_handler)

	subscribe_handler = CommandHandler('subscribe', subscribe, pass_args=True)
	dispatcher.add_handler(subscribe_handler)

	test_handler = CommandHandler('test', test)
	dispatcher.add_handler(test_handler)

	unknown_handler = MessageHandler([Filters.command], unknown)
	dispatcher.add_handler(unknown_handler)

	logger.info("Bot will now start polling")
	updater.start_polling()
	updater.idle()

def db_init():
	logger.info("Initializing database")
	db = SQLDb(config.get(db_section, "db_name"))

	logger.info("Creating table {}".format(config.get(db_section, "subscribers_table_name")))
	if not db.table_exists(config.get(db_section, "subscribers_table_name")):
		db.execute(""" 
			CREATE TABLE {} 
			( 
				id INTEGER PRIMARY KEY,
				username VARCHAR(40)
			);
		""".format(config.get(db_section, "subscribers_table_name")))
	else:
		logger.info("Table already exists")

def init():
	db_init()
	bot_init()

init()

# Output of update after message
# {'message': {'migrate_to_chat_id': 0, 'delete_chat_photo': False, 'new_chat_photo': [], 'entities': [{'length': 10, 'type': u'bot_command', 'offset': 0}], 'text': u'/subscribe lala', 'migrate_from_chat_id': 0, 'channel_chat_created': False, 'from': {'username': u'thiago_lobo', 'first_name': u'Thiago', 'last_name': u'Lobo', 'type': '', 'id': 58880229}, 'supergroup_chat_created': False, 'chat': {'username': u'thiago_lobo', 'first_name': u'Thiago', 'all_members_are_admins': False, 'title': '', 'last_name': u'Lobo', 'type': u'private', 'id': 58880229}, 'photo': [], 'date': 1484511254, 'group_chat_created': False, 'caption': '', 'message_id': 323, 'new_chat_title': ''}, 'update_id': 503174214}