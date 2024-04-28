SELECT 
    date_format(publish_date, 'yyyy-MM') as month,
    count(*) as mentions
FROM news_data
WHERE lower(content) LIKE '%keyword%'
GROUP BY date_format(publish_date, 'yyyy-MM')
ORDER BY month;