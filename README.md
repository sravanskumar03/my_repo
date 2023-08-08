SELECT REPLACE(column_name COLLATE SQL_Latin1_General_CP1251_CS_AS, CHAR(0x0D), '')
FROM table_name
WHERE column_name LIKE '%[^ -~]%' ESCAPE '|';
