DECLARE @tbl TABLE (YourString VARCHAR (100));
INSERT INTO @tbl VALUES ('Pillars 101 in an apartment'), ('Zuzu Durga International Hotel'), ('Wyndham Garden Fresh Meadows');

SELECT CAST ('<x>' + REPLACE ((SELECT YourString AS [*] FOR XML PATH('')), ' ', '</x><x>') + '</x>' AS XML).query('
  for $x in /x
  order by $x
  return concat($x/text()[1], " ")
').value('.', 'varchar(max)')
FROM @tbl;
