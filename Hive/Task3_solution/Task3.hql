USE shuklari;
SELECT 
    Logs.browser,
    SUM(IF(Users.Gender = 'male', 1, 0)) AS Visits_by_Male,
    SUM(IF(Users.Gender = 'female', 1, 0)) AS Visits_by_Female
FROM Logs
JOIN Users  ON Logs.IpAddr = Users.ip
GROUP BY Logs.Browser
ORDER BY Visits_by_Male  DESC, Visits_by_Female DESC;