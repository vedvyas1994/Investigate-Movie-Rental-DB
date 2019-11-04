/*Query 1 - Used for first insight */

SELECT DISTINCT film_title, category_name,
	COUNT(rentals) OVER (PARTITION BY film_title) AS rental_count

	FROM (SELECT f.title as film_title,
			c.name AS category_name,
			r.rental_id AS rentals
		FROM category c
		JOIN film_category fc
		ON fc.category_id = c.category_id
		AND c.name IN ('Animation','Children','Classics','Comedy','Family ','Music')
		JOIN film f
		ON f.film_id = fc.film_id
		JOIN inventory i
		ON i.film_id = f.film_id
		JOIN rental r
		ON r.inventory_id = i.inventory_id
		) sub
	ORDER BY 2,1;




/*Query 2 - Used for second insight */

SELECT  DATE_PART('month',r.rental_date) AS Rental_month,
        DATE_PART('year', r.rental_date) AS Rental_year,
	sr.store_id AS Store_ID,
	COUNT(r.rental_id) AS Count_rentals
FROM rental r
JOIN payment p
ON r.rental_id = p.rental_id and r.customer_id = p.customer_id
JOIN staff s
ON  r.staff_id = s.staff_id
JOIN store sr
ON sr.store_id = s.store_id
GROUP BY 1,2,3
ORDER BY 4 DESC;




/*Query 3 - Used for third insight */

WITH t1 AS (SELECT DISTINCT p.customer_id as c_id,DATE_TRUNC('month', p.payment_date) AS p_month, DATE_PART('month', p.payment_date) AS p1_month,
COUNT(p.payment_id) OVER (PARTITION BY p.customer_id ORDER BY DATE_TRUNC('month', p.payment_date)) AS month_count,
SUM(p.amount) OVER (PARTITION BY p.customer_id ORDER BY DATE_TRUNC('month', p.payment_date)) AS top_sum
FROM payment p),

--only fetching 10 top ids
t2 AS (SELECT DISTINCT p.customer_id AS c_id,
SUM(p.amount) OVER (PARTITION BY p.customer_id ) AS top_sum
FROM payment p
ORDER by 2 DESC
LIMIT 10)

SELECT CONCAT(c.first_name,' ',c.last_name ) AS full_name, t1.p1_month,
t1.month_count - COALESCE(LAG(t1.month_count,1) OVER (PARTITION BY t1.c_id ORDER BY t1.p_month),0) AS pay_countpermon,
t1.top_sum - COALESCE(LAG(t1.top_sum,1) OVER (PARTITION BY t1.c_id ORDER BY t1.p_month),0) AS pay_amount
FROM t1
JOIN t2
ON t1.c_id = t2.c_id
JOIN customer c
ON c.customer_id = t1.c_id
WHERE EXTRACT('year' FROM t1.p_month) = 2007
ORDER BY 1, 2;





/*Query 4 - Used for fourth insight */

WITH t1 AS (SELECT DISTINCT p.customer_id as c_id, DATE_TRUNC('month', p.payment_date) AS p_month, DATE_PART('month', p.payment_date) AS p1_month,
COUNT(p.payment_id) OVER (PARTITION BY p.customer_id ORDER BY DATE_TRUNC('month', p.payment_date)) AS month_count,
SUM(p.amount) OVER (PARTITION BY p.customer_id ORDER BY DATE_TRUNC('month', p.payment_date)) AS top_sum
FROM payment p),

--only fetching 10 top ids
t2 AS (SELECT DISTINCT p.customer_id AS c_id,
SUM(p.amount) OVER (PARTITION BY p.customer_id ) AS top_sum
FROM payment p
ORDER by 2 DESC
LIMIT 10)


SELECT CONCAT(c.first_name,' ',c.last_name ) AS full_name, sub.p1_month, sub.pay_countpermon, sub.pay_amount, sub.pay_diff,
dense_rank() OVER (ORDER BY sub.pay_diff DESC) AS rank_pay_diff
FROM ( SELECT sub1.c_id, sub1.p_month, sub1.p1_month, sub1.pay_countpermon, sub1.pay_amount, sub1.pay_amount - COALESCE(LAG(sub1.pay_amount,1) OVER (PARTITION BY sub1.c_id ORDER BY sub1.p_month), 0) AS pay_diff
				FROM (SELECT t1.c_id, t1.p_month, t1.p1_month, t1.month_count - COALESCE(LAG(t1.month_count,1) OVER (PARTITION BY t1.c_id ORDER BY t1.p_month),0) AS pay_countpermon,
											t1.top_sum - COALESCE(LAG(t1.top_sum,1) OVER (PARTITION BY t1.c_id ORDER BY t1.p_month),0) AS pay_amount
							FROM t1 WHERE EXTRACT('year' FROM t1.p_month) = 2007) sub1) sub
JOIN t2
ON sub.c_id = t2.c_id
JOIN customer c
ON c.customer_id = t2.c_id
ORDER BY 1, 2;
