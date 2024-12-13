import pika
import json
from datetime import datetime

def publish_message(name, links, prices):
    # RabbitMQ bağlantı bilgileri
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', credentials))
    channel = connection.channel()

    # Kuyruğu tanımlama
    channel.queue_declare(queue='test_queue')

    # Mesaj oluşturma
    link_price = dict(zip(links, prices))
    data = {
        "time": datetime.now().isoformat(),
        "name": name,
        "link_price": link_price
    }
    message_json = json.dumps(data)

    # Mesajı kuyruğa gönderme
    channel.basic_publish(exchange='', routing_key='test_queue', body=message_json)

    print(f"Message sent: {message_json}")
    connection.close()

# Örnek çağrı
links = ["link1", "link2", "link3"]
prices = [500, 200, 300]
publish_message("SampleName", links, prices)
