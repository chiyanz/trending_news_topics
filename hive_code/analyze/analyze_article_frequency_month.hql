SELECT 
    date_format(publish_date, 'yyyy-MM') as month,
    count(*) as article_count
FROM news_data
GROUP BY date_format(publish_date, 'yyyy-MM')
ORDER BY month;