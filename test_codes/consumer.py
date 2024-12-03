import pika
import json

def consume_messages():
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', credentials))
    channel = connection.channel()

    channel.queue_declare(queue='test_queue')

    def callback(ch, method, properties, body):
        message_dict = json.loads(body)  # Gelen mesajı sözlüğe dönüştür
        print(f"Alınan mesaj: {message_dict["date"]}")

    channel.basic_consume(queue='test_queue', on_message_callback=callback, auto_ack=True)
    print("Mesajlar dinleniyor...")
    channel.start_consuming()

consume_messages()