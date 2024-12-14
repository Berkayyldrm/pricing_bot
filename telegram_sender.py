import pika
import telebot
import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

bot = telebot.TeleBot(TELEGRAM_TOKEN)

def send_message(message):
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
        print(f"Message sent: {message}")
    except Exception as e:
        print(f"Failed to send message: {e}")

def callback(ch, method, properties, body): # Burayı güncelle
    "RabbitMQ'dan gelen mesajı işler ve Telegram'a gönderir."
    message = body.decode('utf-8')
    send_message(message)

def consume_messages():
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', credentials))
    channel = connection.channel()

    channel.queue_declare(queue='telegram')
    channel.basic_consume(queue='telegram', on_message_callback=callback, auto_ack=True)
    print("Waiting for messages. To exit, press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    consume_messages()