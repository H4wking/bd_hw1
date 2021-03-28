CREATE KEYSPACE book_reviews WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Q1 and Q2 table
CREATE TABLE book_reviews.reviews_by_pid_and_star (
product_id text,
star_rating int,
review_id text,
customer_id text,
helpful_votes int,
total_votes int,
vine boolean,
verified_purchase boolean,
review_headline text,
review_body text,
review_date date,
PRIMARY KEY (product_id, star_rating, review_id)
);

-- Q1
SELECT * FROM book_reviews.reviews_by_pid_and_star WHERE product_id = '0';

-- Q2
SELECT * FROM book_reviews.reviews_by_pid_and_star WHERE product_id = '0' AND star_rating = 5;

-- Q3 table
CREATE TABLE book_reviews.reviews_by_cid (
customer_id text,
review_id text,
product_id text,
star_rating int,
helpful_votes int,
total_votes int,
vine boolean,
verified_purchase boolean,
review_headline text,
review_body text,
review_date date,
PRIMARY KEY (customer_id, review_id)
);

-- Q3
SELECT * FROM book_reviews.reviews_by_cid WHERE customer_id = '0';