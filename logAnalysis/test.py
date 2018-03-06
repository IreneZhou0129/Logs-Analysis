SELECT aa.date, err/total*100 
FROM
    (SELECT a.date,SUM(count) as total 
    FROM 
        (SELECT status, DATE(time),count(status) 
        FROM log 
        GROUP BY status, DATE(time) 
        )AS a 
    group by a.date ) as aa
INNER JOIN 
    (SELECT b.date,SUM(count) as err 
    FROM 
        (SELECT status, DATE(time),COUNT(status)  
        FROM log  
        WHERE status = '404 NOT FOUND' 
        GROUP BY status, DATE(time)
        ) AS b 
    group by b.date ) as bb
ON (aa.date = bb.date) 

WHERE err/total*100 > 1 
GROUP BY aa.date , err/total*100
ORDER BY aa.date; 
