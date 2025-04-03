
-- Query to calculate total sales per product category
SELECT p.product_category_name, SUM(o.price) AS total_sales
FROM orders o
JOIN products p ON o.product_id = p.product_id
GROUP BY p.product_category_name
ORDER BY total_sales DESC;

-- Query the average delivery time per seller from the fact table
SELECT seller_id, ISNULL(AVG(delivery_time), 0) AS avg_delivery_time
FROM orders 
GROUP BY seller_id;

-- Query the number of orders from each state from the customer dimension
SELECT c.customer_state, COUNT(o.order_id) AS num_orders
FROM orders o
JOIN customers c
ON o.customer_id = c.customer_id
GROUP BY c.customer_state;
