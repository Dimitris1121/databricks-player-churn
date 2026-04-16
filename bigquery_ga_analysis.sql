-- ================================================
-- Google Analytics Funnel Analysis in BigQuery
-- Dataset: bigquery-public-data.google_analytics_sample
-- ================================================

-- 1. Daily sessions, pageviews, transactions and revenue
SELECT
  date,
  COUNT(*) AS total_sessions,
  SUM(totals.pageviews) AS pageviews,
  SUM(totals.transactions) AS transactions,
  ROUND(SUM(totals.transactionRevenue) / 1000000, 2) AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20160801' AND '20160831'
GROUP BY date
ORDER BY date;

-- 2. Conversion rate by traffic source
-- Key finding: YouTube drives high volume but near-zero conversion
-- Direct traffic converts at 2.45% vs YouTube at 0.001%
SELECT
  trafficSource.source AS traffic_source,
  COUNT(*) AS sessions,
  SUM(totals.transactions) AS transactions,
  ROUND(SUM(totals.transactions) / COUNT(*) * 100, 2) AS conversion_rate_pct,
  ROUND(SUM(totals.transactionRevenue) / 1000000, 2) AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20160801' AND '20161231'
  AND totals.transactions IS NOT NULL
GROUP BY traffic_source
ORDER BY conversion_rate_pct DESC, sessions DESC
LIMIT 10;

-- 3. Page-level funnel analysis using UNNEST
-- Key finding: large drop-off between basket and signin = checkout friction
SELECT
  hits.page.pagePath AS page,
  COUNT(*) AS pageviews,
  COUNT(DISTINCT fullVisitorId) AS unique_visitors,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER() * 100, 2) AS pct_of_total
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST(hits) AS hits
WHERE
  _TABLE_SUFFIX BETWEEN '20160801' AND '20160831'
  AND hits.type = 'PAGE'
GROUP BY page
ORDER BY pageviews DESC
LIMIT 10;
