# -*- coding: utf-8 -*-

from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, Job
from os.path import exists
from datetime import datetime, timedelta

subscription_password = "batata"
subscribers_file = "subscribers"
token_file = "token"
bot_token = ""
digest_hour = 8

with open(token_file, "r") as f:
	bot_token = [x.rstrip('\n') for x in f.readlines()][0]

print bot_token

def callback_minute(bot, job):
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
	if len(args) > 0 and args[0] == subscription_password:
		result = add_subscription(update.message.chat_id)
		bot.send_message(chat_id=update.message.chat_id,
	                 	 text=result)
	else:
		bot.send_message(chat_id=update.message.chat_id,
	                 	 text=u"Senha incorreta.\n")

def test(bot, update):
	with open(subscribers_file, "r") as f:
		lines = [x.rstrip('\n') for x in f.readlines()]
		for chat_id in lines:
			bot.send_message(chat_id=chat_id,
							 text=u"ação à décima potência\n")

def unknown(bot, update):
	msg = u"Desculpe, esse comando não parece existir."
	bot.send_message(chat_id=update.message.chat_id,
	                 text=msg)
	start(bot, update)

####################### STARTUP #######################
# Connecting to Telegram API
# Updater retrieves information and dispatcher connects commands
updater = Updater(token=bot_token)
dispatcher = updater.dispatcher
job_q = updater.job_queue

# Determine remaining seconds until next digest message (1 per day)
today = datetime(year=datetime.today().year,
				 month=datetime.today().month,
				 day=datetime.today().day)
target = today + timedelta(days=1, hours=digest_hour)
deltaseconds = (target - datetime.now()).total_seconds()

# Add new Job to the dispatcher's job queue.
# Will happen every deltaseconds seconds, starting from now
job_q.put(Job(callback_minute, (24 * 60 * 60)), next_t=deltaseconds)

start_handler = CommandHandler('start', start)
dispatcher.add_handler(start_handler)

subscribe_handler = CommandHandler('subscribe', subscribe, pass_args=True)
dispatcher.add_handler(subscribe_handler)

test_handler = CommandHandler('test', test)
dispatcher.add_handler(test_handler)

unknown_handler = MessageHandler([Filters.command], unknown)
dispatcher.add_handler(unknown_handler)

updater.start_polling()
updater.idle()