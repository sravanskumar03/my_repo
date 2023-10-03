SELECT column_name
FROM table_name
ORDER BY
  CASE column_name
    WHEN 'BP' THEN 1
    WHEN 'PR' THEN 2
    WHEN 'UM' THEN 3
  END;
