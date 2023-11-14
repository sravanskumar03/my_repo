LEAD(TO_DATE(date_column, 'yyyy-MM-dd'), 1) OVER (ORDER BY TO_DATE(date_column, 'yyyy-MM-dd') DESC)
