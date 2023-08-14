SELECT id, value
FROM your_table
LATERAL VIEW EXPLODE(SPLIT(column_to_split, ';')) exploded_table AS value;
