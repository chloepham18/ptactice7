--- EXERCISE 1:
with cte as (
SELECT extract(year from transaction_date) as year,product_id,spend as curr_year_spend,
lag(spend) over(partition by product_id order by transaction_date) as prev_year_spend

from user_transactions)

select *,round(100.0*(curr_year_spend-prev_year_spend)/prev_year_spend,2) as yoy_rate
from cte;

--- EXERCISE 2:
SELECT DISTINCT card_name, 
first_value(issued_amount) OVER(PARTITION BY card_name ORDER BY make_date(issue_year,issue_month,'01')) as amount
FROM monthly_cards_issued
ORDER BY amount DESC;

--- EXERCISE 3:
 SELECT t1.user_id
     , t1.spend
     , t1.transaction_date
  FROM transactions t1
       INNER JOIN
       transactions t2
        ON t1.user_id = t2.user_id
       AND t2.transaction_date < t1.transaction_date
 GROUP BY t1.user_id
        , t1.spend
        , t1.transaction_date
HAVING COUNT(t2.transaction_date)=2;

--- EXERCISE 4:
SELECT 
transaction_date,
user_id,
COUNT(product_id)
FROM user_transactions
WHERE 
(user_id,transaction_date) IN (SELECT user_id,MAX(transaction_date) FROM user_transactions GROUP BY user_id)
GROUP BY 2,1
ORDER BY 1

--- EXERCISE 5:
SELECT
    t1.user_id,
    t1.tweet_date,
    ROUND(AVG(t2.tweet_count), 2) AS rolling_avg_3d
FROM
    tweets t1
JOIN
    tweets t2 ON t1.user_id = t2.user_id
             AND t2.tweet_date BETWEEN t1.tweet_date - INTERVAL '2 days' AND t1.tweet_date
GROUP BY
    t1.user_id, t1.tweet_date
ORDER BY
    t1.user_id, t1.tweet_date;

--- EXERCISE 6:
SELECT COUNT(*) AS payment_count
from ( 
  SELECT merchant_id, credit_card_id, amount, transaction_timestamp -
    LAG(transaction_timestamp,1) OVER(PARTITION BY merchant_id, credit_card_id, amount
    ORDER BY transaction_timestamp) as NEXT_TRANS
  FROM TRANSACTIONS
  ORDER BY merchant_id, credit_card_id, amount
) AS S1
WHERE EXTRACT(MINUTE FROM NEXT_TRANS) < 10

--- EXERCISE 7:
SELECT category, product, total_spend FROM (
  SELECT category, product, sum(spend) AS total_spend,
  DENSE_RANK() OVER (PARTITION BY category ORDER BY sum(spend) DESC) AS DenseRank
  FROM product_spend
  WHERE EXTRACT(YEAR FROM transaction_date) = 2022
  GROUP BY category, product) highestproduct
WHERE DenseRank <= 2

--- EXERCISE 8:
WITH cte AS (
    SELECT artist_name, COUNT(rank) AS song_appearances,
    DENSE_RANK() OVER(ORDER BY COUNT(rank) DESC) as artist_rank
    FROM artists a JOIN songs s
    ON a.artist_id = s.artist_id JOIN global_song_rank r
    ON s.song_id = r.song_id
    WHERE rank <= 10
    GROUP BY a.artist_name
)
SELECT artist_name, artist_rank
FROM cte 
WHERE artist_rank <= 5
