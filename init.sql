
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Cron job tanımı
SELECT cron.schedule(
    'update_all_tables',
    '*/2 * * * *', -- Her 2 dakikada bir çalışır
    $$
    DO $$
    DECLARE
        tbl RECORD;
    BEGIN
        FOR tbl IN
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
        LOOP
            EXECUTE format(
                'UPDATE %I SET x = y WHERE x IS DISTINCT FROM y;',
                tbl.tablename
            );
        END LOOP;
    END $$;
    $$;
);