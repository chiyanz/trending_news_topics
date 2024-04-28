
SELECT 
    content,
    count(*) as frequency
FROM news_data
WHERE date_format(publish_date, 'yyyy-MM') = '2024-05'
GROUP BY content
ORDER BY frequency DESC
LIMIT 10;
