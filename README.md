SELECT a.shop_number, a.shop_name, a.owner_name AS owner_name_a, b.owner_name AS owner_name_b,
       a.fruits AS fruits_a, b.fruits AS fruits_b
FROM table_a a
INNER JOIN table_b b
ON a.shop_number = b.shop_number
WHERE CONCAT(SUBSTRING_INDEX(a.owner_name, ', ', -1), ', ', SUBSTRING_INDEX(a.owner_name, ', ', 1)) = b.owner_name
AND REPLACE(REPLACE(REPLACE(a.fruits, 'mango', 'MNG'), 'banana', 'BANA'), 'grapes', 'GRP') = b.fruits;
