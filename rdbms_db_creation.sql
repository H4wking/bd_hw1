CREATE DATABASE book_reviews;

CREATE TABLE reviews_data (
 	marketplace VARCHAR(20),
    customer_id VARCHAR(10),
 	review_id VARCHAR(20),
    product_id VARCHAR(10),
 	product_parent VARCHAR(10),
 	product_title TEXT,
 	product_category VARCHAR(20),
    star_rating INT,
    helpful_votes INT,
    total_votes INT,
    vine BOOLEAN,
    verified_purchase BOOLEAN,
    review_headline TEXT,
    review_body TEXT,
    review_date DATE
);

COPY reviews_data FROM 'filepath.tsv' DELIMITER E'\t';

CREATE TABLE product (
	product_id VARCHAR(10) PRIMARY KEY,
	marketplace VARCHAR(20),
    product_parent VARCHAR(10),
    product_title TEXT,
    product_category VARCHAR(20)
);

CREATE TABLE review (
	review_id VARCHAR(20) PRIMARY KEY,
	customer_id VARCHAR(10),
    product_id VARCHAR(10),
    star_rating INT,
    helpful_votes INT,
    total_votes INT,
    vine BOOLEAN,
    verified_purchase BOOLEAN,
    review_headline TEXT,
    review_body TEXT,
    review_date DATE,
    FOREIGN KEY (product_id) REFERENCES product (product_id)
);

INSERT INTO product
SELECT DISTINCT product_id, marketplace, product_parent, product_title, product_category
FROM reviews_data;

INSERT INTO review
SELECT review_id, customer_id, product_id, star_rating, helpful_votes, total_votes, vine, verified_purchase, review_headline, review_body, review_date
FROM reviews_data;