SELECT entity, value
FROM your_table t1
CROSS APPLY dbo.SplitString(t1.value, ';') s;
