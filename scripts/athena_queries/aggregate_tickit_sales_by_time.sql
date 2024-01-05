-- tickits sold by time (year, month)
CREATE TABLE IF NOT EXISTS agg_tickit_sales_by_time
WITH (
	format = 'Parquet',
	write_compression = 'SNAPPY',
	external_location = 's3://tickit-lake/gold/agg_tickit_sales_by_time',
	partitioned_by  = ARRAY ['sale_year', 'sale_month']
) AS
SELECT
    e.eventname AS event_name,
    v.venuename AS venue_name,
    v.venuecity AS venue_city,
    v.venuestate AS venue_state,
    CONCAT(b.firstname, ' ', b.lastname) AS buyer_name,
    s.qtysold AS quantity_sold,
    CAST(s.pricepaid AS INT) AS price_paid,
    EXTRACT(YEAR FROM CAST(s.saletime AS TIMESTAMP)) AS sale_year,
    EXTRACT(MONTH FROM CAST(s.saletime AS TIMESTAMP)) AS sale_month
FROM refined_tickit_public_category c
LEFT JOIN refined_tickit_public_event e 
ON c.catid = e.catid
LEFT JOIN refined_tickit_public_sale s
ON e.eventid = s.eventid
LEFT JOIN refined_tickit_public_venue v
ON e.venueid = v.venueid
LEFT JOIN refined_tickit_public_buyer b
ON s.buyerid = b.buyerid
WHERE CAST(s.saletime AS TIMESTAMP) IS NOT NULL -- filtering NULLs

