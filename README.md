# pricing_bot

db log
-------

CREATE TABLE alert_logs(  
    table_name TEXT,
    row_id TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


crontab
-------
*/5 * * * * /home/berkyyld/miniconda3/envs/pricing_bot/bin/python3 -u /home/berkyyld/pricing_bot/price_data_sender.py >> /home/berkyyld/pricing_bot/logs/price_sender.log 2>&1
0 0 * * * /home/berkyyld/miniconda3/envs/pricing_bot/bin/python3 -u /home/berkyyld/pricing_bot/update_tables_daily.py  >> /home/berkyyld/pricing_bot/logs/update_tables.log 2>&1

constantly works scripts
------------------------
nohup /home/berkyyld/miniconda3/envs/pricing_bot/bin/python3 -u /home/berkyyld/pricing_bot/db_writer.py > /home/berkyyld/pricing_bot/logs/db_writer.log 2>&1 &
nohup /home/berkyyld/miniconda3/envs/pricing_bot/bin/python3 -u /home/berkyyld/pricing_bot/telegram_sender.py > /home/berkyyld/pricing_bot/logs/telegram_sender.log 2>&1 &
