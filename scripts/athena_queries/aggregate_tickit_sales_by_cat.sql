-- tickits sold by category (catname, catgroup)
CREATE TABLE IF NOT EXISTS agg_tickit_sales_by_category
WITH (
	format = 'Parquet',
	write_compression = 'SNAPPY',
	external_location = 's3://tickit-lake/gold/tickit-sales-by-category',
	partitioned_by  = ARRAY ['category_group', 'category_name']
) AS
SELECT
    e.eventname AS event_name,
    v.venuename AS venue_name,
    v.venuecity AS venue_city,
    v.venuestate AS venue_state,
    CONCAT(b.firstname, ' ', b.lastname) AS buyer_name,
    s.qtysold AS quantity_sold,
    CAST(s.pricepaid AS INT) AS price_paid,
    c.catgroup AS category_group,
    c.catname AS category_name
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

