USE shuklari;

SELECT 
    substr(FullTimeStamp, 1, 8) AS DATE,
    COUNT(*) AS UNIQUE_VISITS
FROM Logs
GROUP BY substr(FullTimeStamp, 1, 8)
ORDER BY UNIQUE_VISITS DESC;  

