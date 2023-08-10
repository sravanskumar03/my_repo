SELECT CONCAT(SUBSTRING_INDEX(name, ' ', -1), ', ', SUBSTRING_INDEX(name, ' ', 1)) as formatted_name
FROM table_name;
