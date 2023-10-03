SELECT
    ArrayColumn,
    '[' + STRING_AGG(IndividualValue, ',') WITHIN GROUP (ORDER BY IndividualValue) + ']' AS SortedArray
FROM (
    SELECT
        ArrayColumn,
        TRIM('[]"' FROM value) AS IndividualValue
    FROM STRING_SPLIT(ArrayColumn, ',')
) AS SplitValues
GROUP BY ArrayColumn;
