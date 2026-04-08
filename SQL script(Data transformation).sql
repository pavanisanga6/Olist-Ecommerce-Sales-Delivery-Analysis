create database Ecommerce;
use Ecommerce;

              ## creating orders table 
CREATE TABLE olist_orders_dataset (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_status VARCHAR(50),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME
);
             ## Creating order items table
CREATE TABLE olist_order_items_dataset (
    order_id VARCHAR(50),
    order_item_id INT,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date DATETIME,
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2),
    PRIMARY KEY(order_id, order_item_id)
);
          ##  Creating prducts table
CREATE TABLE olist_products_dataset (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);
## Creating customers table
CREATE TABLE olist_customers_dataset (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix INT,
    customer_city VARCHAR(100),
    customer_state VARCHAR(10)
);
## Creating sellers table
CREATE TABLE olist_sellers_dataset (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix INT,
    seller_city VARCHAR(100),
    seller_state VARCHAR(10)
);
## Creating reviews table
CREATE TABLE olist_order_reviews_dataset (
    review_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50),
    review_score INT,
    review_comment_title VARCHAR(255) NULL,
    review_comment_message TEXT NULL,
    review_creation_date DATETIME NULL,
    review_answer_timestamp DATETIME NULL
);
## Creating payments table
CREATE TABLE olist_order_payments_dataset (
    order_id VARCHAR(50),
    payment_sequential INT,
    payment_type VARCHAR(50),
    payment_installments INT,
    payment_value DECIMAL(10,2),
    PRIMARY KEY(order_id, payment_sequential)
);
## Creating geolocation table
CREATE TABLE olist_geolocation_dataset (
    geolocation_zip_code_prefix INT,
    geolocation_lat DECIMAL(9,6),
    geolocation_lng DECIMAL(9,6),
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(10)
);

## enabling file loading
## enabling local infile on the server
SHOW GLOBAL VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = 1;

##importing the csv files
LOAD DATA LOCAL INFILE 'C:\\Users\\phani\\OneDrive\\Desktop\\E-Commerce.csv\\olist_orders_dataset.csv'
INTO TABLE olist_orders_dataset
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@order_id, @customer_id, @order_status, 
 @order_purchase_timestamp, @order_approved_at, 
 @order_delivered_carrier_date, @order_delivered_customer_date, 
 @order_estimated_delivery_date)
SET 
  order_id = @order_id,
  customer_id = @customer_id,
  order_status = @order_status,
  order_purchase_timestamp      = STR_TO_DATE(NULLIF(@order_purchase_timestamp, ''), '%d-%m-%Y %H:%i:%s'),
  order_approved_at             = STR_TO_DATE(NULLIF(@order_approved_at, ''), '%d-%m-%Y %H:%i:%s'),
  order_delivered_carrier_date  = STR_TO_DATE(NULLIF(@order_delivered_carrier_date, ''), '%d-%m-%Y %H:%i:%s'),
  order_delivered_customer_date = STR_TO_DATE(NULLIF(@order_delivered_customer_date, ''), '%d-%m-%Y %H:%i:%s'),
  order_estimated_delivery_date = STR_TO_DATE(NULLIF(@order_estimated_delivery_date, ''), '%d-%m-%Y %H:%i:%s');
select * from olist_orders_dataset;
LOAD DATA LOCAL INFILE 'C:\\Users\\phani\\OneDrive\\Desktop\\E-Commerce.csv\\olist_order_items_dataset.csv'
INTO TABLE olist_order_items_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@order_id, @order_item_id, @product_id, @seller_id, @shipping_limit_date, @price, @freight_value)
set 
   order_id = @order_id,
   order_item_id=@order_item_id,
   product_id=@product_id,
   seller_id=@seller_id,
   shipping_limit_date=str_to_date(@shipping_limit_date,'%Y-%m-%d %H:%i:%s'),
   price=@price,
   freight_value=@freight_value;
select * from olist_order_items_dataset;
LOAD DATA LOCAL INFILE 'C:\\Users\\phani\\OneDrive\\Desktop\\E-Commerce.csv\\olist_products_dataset.csv'
INTO TABLE olist_products_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
select * from olist_products_dataset;
LOAD DATA LOCAL INFILE 'C:\\Users\\phani\\OneDrive\\Desktop\\E-Commerce.csv\\olist_customers_dataset.csv'
INTO TABLE olist_customers_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
select * from olist_customers_dataset;
LOAD DATA LOCAL INFILE 'C:\\Users\\phani\\OneDrive\\Desktop\\E-Commerce.csv\\olist_sellers_dataset.csv'
INTO TABLE olist_sellers_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
select * from olist_sellers_dataset;
LOAD DATA LOCAL INFILE 'C:\\Users\\phani\\OneDrive\\Desktop\\E-Commerce.csv\\olist_order_reviews_dataset.csv'
INTO TABLE olist_order_reviews_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
select * from olist_order_reviews_dataset;
LOAD DATA LOCAL INFILE 'C:\\Users\\phani\\OneDrive\\Desktop\\E-Commerce.csv\\olist_order_payments_dataset.csv'
INTO TABLE olist_order_payments_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
select * from olist_order_payments_dataset;
LOAD DATA LOCAL INFILE 'C:\\Users\\phani\\OneDrive\\Desktop\\E-Commerce.csv\\olist_geolocation_dataset.csv'
INTO TABLE olist_geolocation_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
select * from olist_geolocation_dataset;

## KPIS 
select count(distinct order_id) as total_orders from olist_orders_dataset;
select count(distinct product_category_name) as num_product_category from olist_products_dataset;
select concat(round(sum(payment_value)/1000,2),"k") as Total_payments from olist_order_payments_dataset;
select avg(review_score) as avg_review_score from olist_order_reviews_dataset;

## 1. Weekday Vs Weekend (order_purchase_timestamp) Payment Statistics
select 
      case 
          when dayofweek(o.order_purchase_timestamp) in (1,7) then "Weekend"
          else "Weekday"
	  end as Day_type,
	  count(*) As Total_payments,
      sum(p.payment_value)  as total_payment_value,
      avg(p.payment_value)  as Avg_payment_value 
from olist_orders_dataset as o 
     join 
     olist_order_payments_dataset as p
     on o.order_id = p.order_id
group by Day_type;

## 2. Number of Orders with review score 5 and payment type as credit card.
select count(p.order_id) as num_orders_5review_creditcard
       from olist_order_payments_dataset as p 
          join olist_order_reviews_dataset as r on p.order_id=r.order_id 
          where p.payment_type = "credit_card" and r.review_score = 5;
                        ## alternative way 
select count(o.order_id) num_orders_5review_creditcard
       from olist_order_payments_dataset as p 
		  join olist_orders_dataset as o on p.order_id=o.order_id
          join olist_order_reviews_dataset as r on o.order_id=r.order_id 
          where p.payment_type = "credit_card" and r.review_score = 5;

## 3.Average number of days taken for order_delivered_customer_date for pet_shop
select avg(datediff(o.order_delivered_customer_date,o.order_purchase_timestamp)) as Avg_delivery_days 
       from olist_orders_dataset as o 
       join olist_order_items_dataset as i on o.order_id=i.order_id 
       join olist_products_dataset as p on i.product_id=p.product_id
       where p.product_category_name = 'pet_shop' and o.order_delivered_customer_date is not null;

## 4. Average price and payment values from customers of sao paulo city
select avg(i.price) as Avg_product_price,
       avg(p.payment_value) as Avg_payment_value
              from olist_order_payments_dataset as p
              join olist_orders_dataset as o on p.order_id=o.order_id
              join olist_order_items_dataset as i on o.order_id=i.order_id
              join olist_customers_dataset as c on o.customer_id=c.customer_id
      where c.customer_city = "sao paulo" 
      group by c.customer_city;

## 5. Relationship between shipping days (order_delivered_customer_date - order_purchase_timestamp) Vs review scores.
select r.review_score,
       avg(datediff(o.order_delivered_customer_date,o.order_purchase_timestamp)) as avg_delivery_days,
       count(o.order_id) as total_orders
       from olist_orders_dataset as o 
       join olist_order_reviews_dataset as r on o.order_id=r.order_id
       group by r.review_score order by r.review_score;
	
####6. Data Transformation,Data Preparation,Data Modeling for Analytics,Building an Analytical Dataset
## Creating a fact table(Data mart).
CREATE TABLE fact_orders_analysis AS
select o.order_id,
       DATE(o.order_purchase_timestamp) AS order_date,
       datediff(o.order_delivered_customer_date,o.order_purchase_timestamp) as delivery_days,
	   case 
          when datediff(o.order_delivered_customer_date,o.order_estimated_delivery_date)>0
          then "Late"
          else "On time"
	   end as late_delivery_flag,
       p.payment_value AS revenue,
       r.review_score,
	   c.customer_city,
       pr.product_category_name
from olist_orders_dataset o
join olist_order_payments_dataset p 
     ON o.order_id = p.order_id
JOIN olist_order_reviews_dataset r 
     ON o.order_id = r.order_id
JOIN olist_customers_dataset c 
     ON o.customer_id = c.customer_id
JOIN olist_order_items_dataset oi 
     ON o.order_id = oi.order_id
JOIN olist_products_dataset pr 
     ON oi.product_id = pr.product_id
WHERE o.order_delivered_customer_date IS NOT NULL;
Select count(*) from fact_orders_analysis;

#7. Creating summary tables 

## A. Revenue Summary Table
create table category_revenue_summary as 
select 
      product_category_name,
      count(distinct order_id) as total_orders,
      sum(revenue) as total_revenue,
      Avg(delivery_days) as avg_delivery_days,
      Avg(review_score) as avg_review
FROM fact_orders_analysis
group by product_category_name;
select count(*) from  category_revenue_summary;

## B. Create Delivery Performance Summary Table
create table delivery_performance_summary as 
select 
	 late_delivery_flag,
     Count(order_id) as total_orders,
     AVG(delivery_days) AS avg_delivery_days,
    AVG(review_score) AS avg_review_score,
    SUM(revenue) AS total_revenue
FROM fact_orders_analysis
GROUP BY late_delivery_flag;
select * from  delivery_performance_summary;
