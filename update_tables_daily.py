import psycopg2
from datetime import datetime

DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "mydatabase"
DB_USER = "myuser"
DB_PASSWORD = "mypassword"

def update_tables():
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        cursor = conn.cursor()

        cursor.execute("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'public';
        """)
        tables = cursor.fetchall()

        for table in tables:
            table_name = table[0]
            try:
                cursor.execute(f"""
                    UPDATE {table_name} 
                    SET control_price_daily = current_price 
                    WHERE control_price_daily IS DISTINCT FROM current_price;
                """)
                conn.commit()
                print(f"Daily Updated table: {table_name}")
            except Exception as e:
                print(f"Daily Failed to update table {table_name}: {e}")
                conn.rollback()

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Daily Database connection error: {e}")

if __name__ == "__main__":
    print(datetime.now())
    update_tables()
