SELECT 
    ID,
    STUFF(
        (
            SELECT ';' + value
            FROM STRING_SPLIT(SemicolonSeparatedColumn, ';')
            ORDER BY value ASC
            FOR XML PATH('')
        ), 1, 1, '') AS ReorderedColumn
FROM MyTable;
