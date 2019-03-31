#!/usr/bin/python
import psycopg2

DBNAME = "news"


def answer_question(question_number):
    """This method defines the SQL statement for the given question in string
       format and calls the execute_query(string_query) method to query it
       in the database.

    RETURNS the SQL outcome from the database
    """
    query = ""

    if (question_number == 1):
        query = ("SELECT title, sum(count_path) AS total_views FROM ( "
                 "  SELECT title, A.paths, B.count_path FROM "
                 "    ( SELECT title, CONCAT('/article/', slug) AS paths FROM "
                 "      articles ) A "
                 "  LEFT JOIN ( SELECT DISTINCT path, count(*) as count_path "
                 "    FROM log GROUP BY path ) B "
                 "  ON A.paths = B.path ORDER BY B.count_path DESC LIMIT 3) C "
                 "GROUP BY title ORDER BY total_views DESC")
        return execute_query(query)

    elif (question_number == 2):
        query = ("SELECT name, Y.total_views FROM authors X "
                 "LEFT JOIN ( "
                 "  SELECT author, sum(count_path) AS total_views FROM ( "
                 "   SELECT author, A.paths, B.count_path "
                 "   FROM ( SELECT author, CONCAT('/article/', slug) AS paths "
                 "     FROM articles ) A "
                 "   LEFT JOIN ( SELECT DISTINCT path, count(*) AS count_path "
                 "     FROM log GROUP BY path ) B "
                 "   ON A.paths = B.path ORDER BY B.count_path DESC ) C "
                 "   GROUP BY author ORDER BY total_views DESC ) Y "
                 "ON X.id = Y.author")
        return execute_query(query)

    elif (question_number == 3):
        query = ("SELECT TO_CHAR(day, 'FMMonth DD, YYYY'), percentage "
                 "  FROM ("
                 "    SELECT total_requests_per_day.day, "
                 "      total_errors_per_day.total_errors / "
                 "      total_requests_per_day.total_requests::decimal "
                 "      AS percentage "
                 "    FROM total_requests_per_day "
                 "    INNER JOIN total_errors_per_day "
                 "    ON total_requests_per_day.day = "
                 "       total_errors_per_day.day) AS last_result "
                 "  WHERE percentage > 0.01")
        return execute_query(query)

    else:
        return "NA"


def execute_query(string_query):
    """This method connects to the database and executes the SQL
        statement.

    RETURNS the SQL outcome
    """
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(string_query)
    outcome = c.fetchall()
    db.close()
    return outcome

if __name__ == '__main__':
    q1_results = answer_question(1)
    q2_results = answer_question(2)
    q3_results = answer_question(3)

    print "Q1) The most popular three articles of all time are:"
    for q1_result in q1_results:
        print q1_result[0] + " - " + str(q1_result[1]) + " views"

    print ""

    print "Q2) The most popular article authors of all time are: "
    for q2_result in q2_results:
        print q2_result[0] + " - " + str(q2_result[1]) + " views"

    print ""

    print "Q3) The days where more than 1% of requests led to errors are:"
    for q3_result in q3_results:
        print str(q3_result[0]) + " - " + "%" + str(q3_result[1]*100)
