USE shuklari;

SELECT 
    TRANSFORM(
        IpAddr,
        Date,
        HttpRequest, 
        Response,
        StatusCode, 
        Browser
    )
    USING 'sed "s|http|ftp|g"'  
    AS (
        IpAddr STRING,
        Date STRING,  
        FtpRequest STRING, 
        Response SMALLINT, 
        StatusCode SMALLINT, 
        Browser STRING
    )
FROM Logs
LIMIT 10; 