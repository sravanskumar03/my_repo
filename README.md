CREATE FUNCTION [dbo].[SplitString]
(
    @String NVARCHAR(MAX),
    @Delimiter CHAR(1)
)
RETURNS @Results TABLE (Value NVARCHAR(MAX))
AS
BEGIN
    DECLARE @Index INT = CHARINDEX(@Delimiter, @String)

    WHILE @Index > 0
    BEGIN
        INSERT INTO @Results (Value)
        SELECT SUBSTRING(@String, 1, @Index - 1)

        SET @String = SUBSTRING(@String, @Index + 1, LEN(@String) - @Index)

        SET @Index = CHARINDEX(@Delimiter, @String)
    END

    INSERT INTO @Results (Value)
    SELECT @String

    RETURN
END
