import psycopg2
from datetime import datetime
import pika

DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "mydatabase"
DB_USER = "myuser"
DB_PASSWORD = "mypassword"

def publish_message(message):
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', credentials))
    channel = connection.channel()

    channel.queue_declare(queue='telegram')

    channel.basic_publish(exchange='', routing_key='telegram', body=message)

    connection.close()

def compare_columns():
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Get all table names in the public schema
        cursor.execute("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'public';
        """)
        tables = cursor.fetchall()

        for table in tables:
            table_name = table[0]
            try:
                # Check if the table has the required columns
                cursor.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}' AND column_name IN ('current_price', 'control_price_hourly', 'control_price_daily');
                """)
                columns = cursor.fetchall()
                if len(columns) == 3:  # Proceed only if all three columns exist
                    cursor.execute(f"""
                        SELECT id, current_price, control_price_hourly, control_price_daily  
                        FROM {table_name} 
                        WHERE (current_price IS DISTINCT FROM control_price_hourly)
                        AND id NOT IN (
                            SELECT row_id 
                            FROM alert_logs 
                            WHERE table_name = '{table_name}' 
                            AND processed_at > NOW() - INTERVAL '2 HOURS'
                            AND row_id = id
                        );
                    """)

                    rows = cursor.fetchall()
                    if rows:
                        for row in rows:
                            id, current_price, control_price_hourly, control_price_daily = row
                            changes = []
                            if current_price is not None and control_price_hourly is not None and current_price < control_price_hourly:
                                print(id)
                                change = abs(current_price - control_price_hourly) / control_price_hourly * 100
                                changes.append(f"Saatlik fiyata göre anlık fiyatın değişimi: {change:.2f}%")

                                cursor.execute("""
                                    INSERT INTO alert_logs (table_name, row_id) VALUES (%s, %s);
                                """, (table_name, id))
                                conn.commit()
                            if changes:
                                message = f"Url: {id}, Anlık Fiyat:{current_price}, Saatlik Fiyat:{control_price_hourly}, \n{', '.join(changes)}\n"
                                print(message)
                                publish_message(message)
                    else:
                        print(f"Table: {table_name} - No differing values found.")
                else:
                    pass

            except Exception as e:
                print(f"Failed to process table {table_name}: {e}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Database connection error: {e}")

if __name__ == "__main__":
    print(datetime.now())
    compare_columns()