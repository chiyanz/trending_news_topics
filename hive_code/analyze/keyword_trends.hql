SELECT 
    date_format(publish_date, 'yyyy-MM') as month,
    count(*) as keyword_count
FROM news_data
WHERE lower(content) LIKE '%specific_keyword%' -- subsitute 'specific_keyword' to any keyword
GROUP BY date_format(publish_date, 'yyyy-MM')
ORDER BY month;