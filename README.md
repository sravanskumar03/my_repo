SELECT '["' + REPLACE(your_column, ';', '","') + '"]' AS your_column_array
FROM your_table
