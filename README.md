SELECT 
    "I'd",
    regexp_replace(
        regexp_replace(
            regexp_replace("lnoi vist.values", 'auto', 'au'), 
            'prop', 'pr'
        ),
        'pf', 'pf'
    ) AS "lnoi vist.values"
FROM your_table_name
WHERE "I'd" = 1;
