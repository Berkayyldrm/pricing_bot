import pika

def publish_message():
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', credentials))
    channel = connection.channel()

    # Test queue oluştur
    channel.queue_declare(queue='test_queue')

    # Mesaj gönder
    message = "Hello, RabbitMQ!"
    channel.basic_publish(exchange='', routing_key='test_queue', body=message)

    print(f"Sent: {message}")
    connection.close()

if __name__ == "__main__":
    publish_message()
