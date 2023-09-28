SELECT *
FROM (
  SELECT [color], [Paul], [John], [Tim], [Eric]
  FROM yourTable
) AS SourceTable
UNPIVOT (
  Value FOR Name IN ([Paul], [John], [Tim], [Eric])
) AS UnpivotTable
PIVOT (
  MAX(Value) FOR [color] IN ([Red], [Green], [Blue])
) AS PivotTable;
