SELECT
  lead(DATE_FORMAT(DATE_SUB(TO_DATE(date_column), INTERVAL 1 DAY), 'yyyy-MM-dd')) OVER (ORDER BY date_column) AS lead_date
FROM
  your_table
