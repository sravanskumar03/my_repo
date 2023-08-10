-- Declare variables to hold the query result and the email body
DECLARE @queryResult TABLE (a_id INT, b_id INT, count INT);
DECLARE @body NVARCHAR(MAX);

-- Run the SELECT query on tables a and b
INSERT INTO @queryResult
SELECT a.id, b.id, COUNT(*)
FROM a
JOIN b
ON a.id = b.id -- Replace with your join condition
GROUP BY a.id, b.id;

-- Set the email body
SET @body = N'<html><body><h3>The query result is:</h3><table border="1"><tr><th>a_id</th><th>b_id</th><th>count</th></tr>';
SELECT @body = @body + N'<tr><td>' + CAST(a_id AS NVARCHAR) + N'</td><td>' + CAST(b_id AS NVARCHAR) + N'</td><td>' + CAST(count AS NVARCHAR) + N'</td></tr>'
FROM @queryResult;
SET @body = @body + N'</table></body></html>';

-- Send the email using sp_send_dbmail
EXEC msdb.dbo.sp_send_dbmail
    @profile_name = 'YourMailProfile', -- Replace with your mail profile name
    @recipients = 'you@example.com', -- Replace with your email address
    @subject = 'Query Result',
    @body = @body,
    @body_format = 'HTML';
    
