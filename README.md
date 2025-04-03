# data-eng

- Solutions are provided for both Pandas and PySpark(Databricks) approaches. Pandas directory contains the Jupiter Notebook(source.ipynb), the SQL Script(SQLQuery1.sql) and the database backup(de.bak). The PySpark directory contains the source code (DE.ipynb) which also contains the SQL queries(delta tables are used). Included are screenshots of the Azure Personal Storage, which contains the necessary datasets, the table deltas and the Databricks Service.

The code is documented on both sides, but a general explanation on the solution can be found below:

- Map each dataset to the correspondent dataframe

- Drop duplicate rows for each dataset case by case.
E.g. Customers dataset may wrongfully have 2 customers with the same customer_unique_id, as such the duplicates are dropped, followed by all rows with duplicate content in all of their columns. Rows whose columns have null values which are needed later on / make sense to have values have been dropped. E.g Customers: customer_unique_id, customer_city etc. Some columns have been filled with default value e.g. Products: product_category_name: No Category. This is to include in the queries/data analysis later on the products who have no category(despite this needing to be a system validation in both front-end and back-end).

- The following calculated columns have been created:
1. Total Price: The sum of orders['price'] + orders['freight_value']
2. Delivery time: order_purchase_timestamp and order_delivered_customer_date have been converted to timestamps to ensure format being Date not String. Then the difference in days is calculated
3. Payment Count: orders have been grouped by order_id, and the sum of payment_installments is calculated.(This is like doing SELECT SUM(payment_installments) FROM orders GROUP BY order_id)
4. Profit Margin: Calculated by subtracting freight_value from price for each row

- Window Functions Over Partitions have been calculated both with Pandas and PySpark. The solutions can be found in the respective directories.
1. Total sales per customer: First, order_items are grouped on order_id(to get the sum(price)).Then joined with orders to get the customer_id. In the end, data is grouped by customer_id to retrieve the cumulative sum of price, either through .cumsum() or using Window.partitionBy.
2. Average Delivery Time: The Delivery Time calculation code is already written in the calculated column above. The dataframes: order_items <--> orders <--> products  are joined together. The result is grouped by product_category_name and then we retrieve the delivery_time_days(aggregate)

- Creating a Fact table and Dimension tables
1. The fact table is created by merging the previously calculated columns with order_items
2. Only the date dimension needs to be created using order_id, order_purchase_timestamp and order_delivered_customer_date. All the other dimensions are already available in memory
3. All the tables are either saved in a database(.to_sql()) or as delta tables(.write.format("delta"))

- SQL Queries
The SQL Queries are included in a separate file for Pandas because a database was used. For PySpark, spark.sql() is used and the queries are found near the end of the file. 
