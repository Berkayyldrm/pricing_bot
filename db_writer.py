import pika
import psycopg2
import json
from datetime import datetime

# Database Configuration
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "dbname": "mydatabase",
    "user": "myuser",
    "password": "mypassword",
}

# RabbitMQ Configuration
RABBITMQ_CONFIG = {
    "host": "localhost",
    "port": 5672,
    "queue": "price_queue",
    "credentials": pika.PlainCredentials("user", "password"),
}

# Publish a message to RabbitMQ
def publish_message(message, queue="telegram"):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            RABBITMQ_CONFIG["host"], RABBITMQ_CONFIG["port"], "/", RABBITMQ_CONFIG["credentials"]
        )
    )
    channel = connection.channel()
    channel.queue_declare(queue=queue)
    channel.basic_publish(exchange="", routing_key=queue, body=message)
    connection.close()

# Connect to PostgreSQL
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# Create or connect to a table
def create_or_connect_table(name):
    query = f"""
    CREATE TABLE IF NOT EXISTS {name} (
        id TEXT PRIMARY KEY,
        time TIMESTAMP,
        current_price FLOAT,
        prev_current_price FLOAT,
        control_price_daily FLOAT
    );
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)

# Insert or update data in a table
def insert_data(name, time, link_price):
    query = f"""
    INSERT INTO {name} (id, time, current_price)
    VALUES (%s, %s, %s)
    ON CONFLICT (id) DO UPDATE
    SET current_price = EXCLUDED.current_price, time = EXCLUDED.time;
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            for link_id, current_price in link_price.items():
                cursor.execute(query, (link_id, time, current_price))

# Update previous price columns
def update_tables(table_name):
    query = """
    UPDATE {table_name}
    SET prev_current_price = current_price
    WHERE prev_current_price IS DISTINCT FROM current_price;
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute(query.format(table_name=table_name))
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"Failed to update table {table_name}: {e}")

# Compare columns and publish messages
def compare_columns(table_name):
    query_check_columns = """
    SELECT column_name FROM information_schema.columns
    WHERE table_name = %s AND column_name IN ('current_price', 'prev_current_price');
    """
    query_compare = """
    SELECT id, current_price, prev_current_price, control_price_daily
    FROM {table_name}
    WHERE current_price IS DISTINCT FROM prev_current_price;
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query_check_columns, (table_name,))
            if len(cursor.fetchall()) == 2:
                cursor.execute(query_compare.format(table_name=table_name))
                rows = cursor.fetchall()
                if rows:
                    for id, current_price, prev_current_price, control_price_daily in rows:
                        if current_price and prev_current_price:
                            if current_price < prev_current_price:
                                change = abs(current_price - prev_current_price) / prev_current_price * 100
                                message = (
                                    f"Url: {id}, Anlık Fiyat: {current_price}, Bir Önceki Fiyat: {prev_current_price},"
                                    f" Gece Fiyatı: {control_price_daily} \n Anlık Değişim Oranı: {change:.2f}%\n"
                                )
                                print(f"Change for {table_name}, Message: {message}")
                                publish_message(message)
                        else:
                            print(f"Some None Values -> current_price: {current_price} , prev_current_price: {prev_current_price}")
                else:
                    print(f"There is no change for {table_name}")

# Process incoming RabbitMQ messages
def callback(ch, method, properties, body):
    print("----------------------------------------------------------------")
    print(datetime.now()) 
    data = json.loads(body)
    name, time, link_price = data["name"], data["time"], data["link_price"]
    create_or_connect_table(name)
    update_tables(table_name=name)
    insert_data(name, time, link_price)
    compare_columns(table_name=name)

# Start consuming messages from RabbitMQ
def consume_messages():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            RABBITMQ_CONFIG["host"], RABBITMQ_CONFIG["port"], "/", RABBITMQ_CONFIG["credentials"]
        )
    )
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_CONFIG["queue"])
    channel.basic_consume(queue=RABBITMQ_CONFIG["queue"], on_message_callback=callback, auto_ack=True)
    print("Waiting for messages. To exit, press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    consume_messages()
