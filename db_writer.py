import pika
import psycopg2
import json
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

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
    channel.basic_publish(exchange="", routing_key=queue, body=json.dumps(message))
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

def get_selenium_soup(url):
    options = Options()
    options.add_argument('--headless')  # Arka planda çalışır
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("start-maximized")
    options.add_argument("disable-infobars")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    # Bot algılama özelliklerini devre dışı bırak
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """
    })

    driver.get(url)
    
    dt_string = datetime.utcnow() + timedelta(hours=3)
    driver.save_screenshot(f"ss_notifications/{dt_string}.png")

    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    driver.quit()
    return soup

def get_and_check_other_merchants(url):
    try:
        soup = get_selenium_soup(url)

        other_merchants_div = soup.find('div', {'data-test-id': 'other-merchants'})
        if other_merchants_div:
            data_hbus = other_merchants_div.get('data-hbus')
            if data_hbus:
                data_hbus_json = json.loads(data_hbus.replace('&quot;', '"'))
                price_range = data_hbus_json.get('data', {}).get('price_range')

                if price_range:
                    price_range = price_range.replace(".", "").replace(",", ".")
                    try:
                        min_str, max_str = price_range.split(" - ")
                        min_price = float(min_str)
                        max_price = float(max_str)
                        print("Price Range:", price_range)
                        return min_price, price_range
                    except ValueError:
                        print("Invalid price range format:", price_range)
                        return 9999999999, None
        # Eğer `other_merchants_div` bulunamazsa veya eksikse
        print("No other merchants found")
        return 9999999999, None

    except Exception as e:
        print("get_and_check_other_merchants error:", e)
        return 9999999999, None
    
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
                print(f"Valid Table Name: {table_name}")
                cursor.execute(query_compare.format(table_name=table_name))
                rows = cursor.fetchall()
                if rows:
                    for id, current_price, prev_current_price, control_price_daily in rows:
                        if current_price and prev_current_price:
                            if not control_price_daily:
                                control_price_daily = 99999999999
                            if current_price < prev_current_price and current_price < control_price_daily:
                                change = (current_price - prev_current_price) / prev_current_price * 100
                                daily_change = (current_price - control_price_daily) / control_price_daily * 100
                                if (change < -1) and (daily_change < -1):
                                    min_price, price_range = get_and_check_other_merchants(id)
                                    message_text = (
                                        f"Url: {id}, Anlık Fiyat: {current_price}, Bir Önceki Fiyat: {prev_current_price}, "
                                        f"Gece Fiyatı: {control_price_daily}\n"
                                        f"Diğer Satıcılarda Fiyat Aralığı: {price_range}\n"
                                        f"Anlık Değişim Oranı: {change:.2f}%"
                                    )
                                    print(f"Change for {table_name}, Message: {message_text}")
                                    message = {
                                        "text": message_text,
                                        "category": 1
                                    }
                                    publish_message(message)

                                    if change < -50:
                                        message = {
                                        "text": message_text,
                                        "category": 2
                                        }
                                        publish_message(message)

                                    if (change < -30) and (daily_change < -30):
                                        if current_price <= min_price:
                                            message = {
                                                "text": message_text,
                                                "category": 4
                                                }
                                            publish_message(message)
                                
                            else:
                                print(f"Price Changed for id: {id} -> current_price: {current_price} , prev_current_price: {prev_current_price} , control_price_daily: {control_price_daily}")
                        else:
                            print(f"Some None Values for id: {id} -> current_price: {current_price} , prev_current_price: {prev_current_price}")
                else:
                    print(f"There is no change for {table_name}")
            else:
                print(f"Invalid Table Name: {table_name}")

def tel_5k_query(table_name):
    query_tel = """
    SELECT id, current_price, prev_current_price, control_price_daily
    FROM {table_name};
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query_tel.format(table_name=table_name))
            rows = cursor.fetchall()
            if rows:
                for id, current_price, prev_current_price, control_price_daily in rows:
                    if table_name == "hepsiburada_tel" and current_price < 4000:
                            message_text = f"Url: {id}, Anlık Fiyat: {current_price}"
                            message = {
                                "text": message_text,
                                "category": 3
                                }
                            publish_message(message)

# Process incoming RabbitMQ messages
def callback(ch, method, properties, body):
    print("----------------------------------------------------------------")
    print(datetime.now()) 
    data = json.loads(body)
    name, time, link_price = data["name"], data["time"], data["link_price"]
    print("Table: ", name)
    create_or_connect_table(name)
    update_tables(table_name=name)
    insert_data(name, time, link_price)
    compare_columns(table_name=name)
    tel_5k_query(table_name=name)
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
