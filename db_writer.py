import pika
import psycopg2
import json

# Veritabanı bağlantısı ve tablo yönetimi
def create_or_connect_table(name):
    connection = psycopg2.connect(
        dbname="mydatabase",
        user="myuser",
        password="mypassword",
        host="localhost",
        port="5432"
    )
    cursor = connection.cursor()
    
    # Tabloyu oluşturma (eğer yoksa)
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {name} (
        id TEXT PRIMARY KEY,
        time TIMESTAMP,
        current_price FLOAT,
        control_price_hourly FLOAT,
        control_price_daily FLOAT
    );
    """
    cursor.execute(create_table_query)
    connection.commit()
    cursor.close()
    connection.close()

def insert_data(name, time, link_price):
    connection = psycopg2.connect(
        dbname="mydatabase",
        user="myuser",
        password="mypassword",
        host="localhost",
        port="5432"
    )
    cursor = connection.cursor()

    # Her key-value çiftini tabloya ekleme
    for link_id, current_price in link_price.items():
        ### BURAYA FİYAT KONTROL KODU EKLENECEK.
        insert_query = f"""
        INSERT INTO {name} (id, time, current_price)
        VALUES (%s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET current_price = EXCLUDED.current_price, time = EXCLUDED.time;
        """
        cursor.execute(insert_query, (link_id, time, current_price))

    connection.commit()
    cursor.close()
    connection.close()

# RabbitMQ mesaj dinleyici
def callback(ch, method, properties, body):
    print(f"Received message: {body}")
    data = json.loads(body)

    name = data["name"]
    time = data["time"]
    link_price = data["link_price"]

    # Veritabanında tablo oluştur ve veri ekle
    create_or_connect_table(name)
    insert_data(name, time, link_price)
    print(f"Data written to {name} table: {link_price}")

def consume_messages():
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', credentials))
    channel = connection.channel()

    # Kuyruğu tanımlama
    channel.queue_declare(queue='test_queue')

    # Mesaj dinleme
    channel.basic_consume(queue='test_queue', on_message_callback=callback, auto_ack=True)

    print("Waiting for messages. To exit, press CTRL+C")
    channel.start_consuming()

# Mesajları dinle
consume_messages()
