SELECT year_month, COUNT(*) as politics_count
FROM news_trend
WHERE source = 'HuffPost' AND politics = 1
GROUP BY year_month
ORDER BY year_month DESC;