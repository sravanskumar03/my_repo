SELECT table_schema, table_name, created AS create_date, last_altered AS modify_date
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
ORDER BY table_schema, table_name;
