from flask import Flask
from flask import jsonify
from flask import request
import psycopg2

app = Flask(__name__)


@app.route('/reviews/product/<product_id>')
def reviews_product(product_id):
    try:
        conn = psycopg2.connect("dbname=book_reviews user=postgres host=localhost")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM review WHERE product_id = '{}'".format(product_id))
        rows = cursor.fetchall()
        res = jsonify(rows)
        res.status_code = 200

        return res
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


@app.route('/reviews/product/<product_id>/<star_rating>')
def reviews_product_stars(product_id, star_rating):
    try:
        conn = psycopg2.connect("dbname=book_reviews user=postgres host=localhost")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM review WHERE product_id = '{}' AND star_rating = {}".format(product_id, star_rating))
        rows = cursor.fetchall()
        res = jsonify(rows)
        res.status_code = 200

        return res
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


@app.route('/reviews/customer/<customer_id>')
def reviews_customer(customer_id):
    try:
        conn = psycopg2.connect("dbname=book_reviews user=postgres host=localhost")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM review WHERE customer_id = '{}'".format(customer_id))
        rows = cursor.fetchall()
        res = jsonify(rows)
        res.status_code = 200

        return res
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


@app.route('/most_reviewed_items')
def most_reviewed_items():
    try:
        conn = psycopg2.connect("dbname=book_reviews user=postgres host=localhost")
        cursor = conn.cursor()

        if "n" not in request.args or "from" not in request.args or "to" not in request.args:
            return "Some of the arguments are missing. Required arguments: number of values to return (n=), starting date (from=), ending date (to=)."

        n, fr, to = int(request.args["n"]), request.args["from"], request.args["to"]
        cursor.execute("""SELECT most_reviews.product_id, marketplace, product_parent, product_title, product_category FROM 
                                (SELECT review.product_id, COUNT(review.product_id) FROM review
                                INNER JOIN product ON review.product_id = product.product_id
                                WHERE review_date <= '{}' AND review_date >= '{}'
                                GROUP BY review.product_id)
                            AS most_reviews
                            INNER JOIN product on product.product_id = most_reviews.product_id
                            ORDER BY count DESC
                            LIMIT {}""".format(to, fr, n))
        rows = cursor.fetchall()
        res = jsonify(rows)
        res.status_code = 200

        return res
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


@app.route('/most_productive_customers')
def most_productive_customers():
    try:
        conn = psycopg2.connect("dbname=book_reviews user=postgres host=localhost")
        cursor = conn.cursor()

        if "n" not in request.args or "from" not in request.args or "to" not in request.args:
            return "Some of the arguments are missing. Required arguments: number of values to return (n=), starting date (from=), ending date (to=)."

        n, fr, to = int(request.args["n"]), request.args["from"], request.args["to"]
        cursor.execute("""SELECT customer_id FROM review
                            WHERE verified_purchase = 'true' AND review_date <= '{}' AND review_date >= '{}'
                            GROUP BY customer_id
                            ORDER BY COUNT(customer_id) DESC
                            LIMIT {}""".format(to, fr, n))
        rows = cursor.fetchall()
        res = jsonify(rows)
        res.status_code = 200

        return res
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


@app.route('/best_products')
def best_products():
    try:
        conn = psycopg2.connect("dbname=book_reviews user=postgres host=localhost")
        cursor = conn.cursor()

        if "n" not in request.args:
            return "Some of the arguments are missing. Required arguments: number of values to return (n=)."

        n = int(request.args["n"])
        cursor.execute("""SELECT reviews_count.product_id, marketplace, product_parent, product_title, product_category FROM
                                (SELECT product_id, COUNT(product_id) all_reviews, SUM(CASE WHEN star_rating = 5 THEN 1 ELSE 0 END) five_star_reviews FROM review
                                WHERE verified_purchase = 'true'
                                GROUP BY product_id
                                ORDER BY all_reviews DESC)
                            AS reviews_count
                            INNER JOIN product ON reviews_count.product_id = product.product_id
                            WHERE all_reviews > 100
                            ORDER BY five_star_reviews / all_reviews::float DESC
                            LIMIT {}""".format(n))
        rows = cursor.fetchall()
        res = jsonify(rows)
        res.status_code = 200

        return res
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


@app.route('/most_productive_haters')
def most_productive_haters():
    try:
        conn = psycopg2.connect("dbname=book_reviews user=postgres host=localhost")
        cursor = conn.cursor()

        if "n" not in request.args or "from" not in request.args or "to" not in request.args:
            return "Some of the arguments are missing. Required arguments: number of values to return (n=), starting date (from=), ending date (to=)."

        n, fr, to = int(request.args["n"]), request.args["from"], request.args["to"]
        cursor.execute("""SELECT customer_id FROM review
                            WHERE star_rating < 3 AND review_date <= '{}' AND review_date >= '{}'
                            GROUP BY customer_id
                            ORDER BY COUNT(customer_id) DESC
                            LIMIT {}""".format(to, fr, n))
        rows = cursor.fetchall()
        res = jsonify(rows)
        res.status_code = 200

        return res
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


@app.route('/most_productive_backers')
def most_productive_backers():
    try:
        conn = psycopg2.connect("dbname=book_reviews user=postgres host=localhost")
        cursor = conn.cursor()

        if "n" not in request.args or "from" not in request.args or "to" not in request.args:
            return "Some of the arguments are missing. Required arguments: number of values to return (n=), starting date (from=), ending date (to=)."

        n, fr, to = int(request.args["n"]), request.args["from"], request.args["to"]
        cursor.execute("""SELECT customer_id FROM review
                            WHERE star_rating > 3 AND review_date <= '{}' AND review_date >= '{}'
                            GROUP BY customer_id
                            ORDER BY COUNT(customer_id) DESC
                            LIMIT {}""".format(to, fr, n))
        rows = cursor.fetchall()
        res = jsonify(rows)
        res.status_code = 200

        return res
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    app.run(debug=True)
