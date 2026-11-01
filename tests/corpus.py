"""Shared SQL corpora: used by the enforcer unit tests and the real-Postgres integration tests.

LEGIT: (role, sql) pairs that must be accepted.
ATTACKS: (sql, expected_reason_or_None) pairs a *student* must never get through.
"""

LEGIT = [
    ("student", "SELECT * FROM students"),
    ("student", "SELECT name, cgpa FROM students WHERE cgpa > 5"),
    (
        "student",
        "SELECT c.title, e.grade FROM enrollments e JOIN courses c ON c.course_id = e.course_id",
    ),
    ("student", "SELECT status, amount_due - amount_paid AS pending FROM fees"),
    ("student", "SELECT COUNT(*) FROM students"),
    (
        "student",
        "SELECT department, COUNT(*) FROM students GROUP BY department ORDER BY 2 DESC",
    ),
    ("student", "SELECT title FROM courses UNION SELECT dept_name FROM departments"),
    ("student", "WITH x AS (SELECT * FROM enrollments) SELECT course_id FROM x"),
    (
        "student",
        "SELECT s.name FROM students s WHERE EXISTS "
        "(SELECT 1 FROM enrollments e WHERE e.roll_no = s.roll_no)",
    ),
    ("student", "SELECT lpad(name, 20, '.') FROM students"),
    (
        "student",
        "SELECT a.name FROM students a JOIN students b ON a.department = b.department",
    ),
    ("teacher", "SELECT name, email FROM students"),
    (
        "teacher",
        "SELECT s.name, e.marks FROM students s JOIN enrollments e "
        "ON e.roll_no = s.roll_no ORDER BY e.marks DESC",
    ),
    ("teacher", "SELECT gross, net FROM salaries"),
    (
        "admin",
        "SELECT f.name, s.gross FROM faculty f JOIN salaries s ON s.emp_id = f.emp_id",
    ),
    ("admin", "SELECT name, email FROM students ORDER BY cgpa DESC LIMIT 3"),
    (
        "student",
        "SELECT UPPER(name), ROUND(cgpa, 1), COALESCE(hall, 'none') FROM students",
    ),
    ("student", "SELECT CASE WHEN cgpa > 9 THEN 'high' ELSE 'low' END FROM students"),
    (
        "student",
        "SELECT year, AVG(cgpa) FROM students GROUP BY year HAVING AVG(cgpa) > 1",
    ),
]


ATTACKS = [
    ("SELECT * INTO stolen FROM students", "select_into"),
    ("SELECT pg_read_file('/etc/passwd')", "dangerous_function"),
    ("SELECT pg_sleep(100000)", "dangerous_function"),
    ("SELECT * FROM students FOR UPDATE", "row_lock"),
    ("SELECT * FROM students FOR SHARE", "row_lock"),
    ("SELECT set_config('role','postgres',false)", "dangerous_function"),
    ("SELECT nextval('x')", "dangerous_function"),
    ("SELECT lo_import('/etc/passwd')", "dangerous_function"),
    ("SELECT pg_terminate_backend(1)", "dangerous_function"),
    ("SELECT current_setting('server_version')", None),
    ("SELECT * FROM pg_catalog.pg_shadow", "other_schema"),
    ("SELECT * FROM pg_shadow", "unresolved_reference"),
    ("SELECT table_name FROM information_schema.tables", "other_schema"),
    ("SELECT * FROM students, courses, enrollments", "cross_join"),
    ("SELECT 1; DROP TABLE students", "multiple_statements"),
    ("SELECT 1; SELECT 2", "multiple_statements"),
    (
        "WITH d AS (DELETE FROM students RETURNING *) SELECT * FROM d",
        "forbidden_construct",
    ),
    (
        "WITH i AS (INSERT INTO students(roll_no) VALUES ('x') RETURNING *) SELECT * FROM i",
        "forbidden_construct",
    ),
    ("SELECT version()", None),
    ("SELECT current_user", None),
    ("SELECT * FROM generate_series(1,100000000)", "table_function"),
    (
        "WITH RECURSIVE r AS (SELECT 1 UNION ALL SELECT 1 FROM r) SELECT * FROM r",
        "recursive",
    ),
    ("SELECT 'students'::regclass", "catalog_cast"),
    ("SELECT 'students'::regclass::oid", "catalog_cast"),
    ("UPDATE students SET cgpa = 10", "not_select"),
    ("INSERT INTO students(roll_no) VALUES ('x')", "not_select"),
    ("DELETE FROM students", "not_select"),
    ("DROP TABLE students", "not_select"),
    ("TRUNCATE students", "not_select"),
    ("ALTER TABLE students ADD COLUMN x int", "not_select"),
    ("CREATE TABLE t AS SELECT * FROM students", "not_select"),
    ("GRANT ALL ON students TO PUBLIC", "not_select"),
    ("COPY students TO '/tmp/x'", "not_select"),
    ("SET ROLE postgres", "not_select"),
    ("EXPLAIN ANALYZE SELECT * FROM students", "not_select"),
    ("SELECT * FROM salaries", "unresolved_reference"),
    ("SELECT * FROM erp_users", "unresolved_reference"),
    ("SELECT password_hash FROM erp_users", "unresolved_reference"),
    ("SELECT aadhaar_no FROM students", "unresolved_reference"),
    ("SELECT phone FROM students", "unresolved_reference"),
    ("SELECT advisor_emp_id FROM students", "unresolved_reference"),
    ("SELECT * FROM faculty WHERE email LIKE 'r%'", "unresolved_reference"),
    (
        "SELECT s.* FROM students s JOIN faculty f ON f.phone = s.phone",
        "unresolved_reference",
    ),
    ("SELECT * FROM students WHERE $1 = roll_no", "bind_param"),
    ("SELECT * FROM students s, LATERAL (SELECT * FROM salaries) x", None),
    # whole-row references would carry columns the role may not see (found by the AdventureWorks security track)
    ("SELECT students::text FROM students", "whole_row_reference"),
    ("SELECT s::text FROM students s", "whole_row_reference"),
    ("SELECT s FROM students s", "whole_row_reference"),
    ("SELECT s AS x FROM students s", "whole_row_reference"),
    ("SELECT (s).aadhaar_no FROM students s", "whole_row_reference"),
    ("SELECT CAST(s AS TEXT) FROM students s", "whole_row_reference"),
    ("SELECT * FROM (SELECT s FROM students s) t", "whole_row_reference"),
    ("SELECT COUNT(s) FROM students s", "whole_row_reference"),
    # a join condition that mentions no column is a cross join in disguise
    ("SELECT a.name FROM students a JOIN students b ON TRUE", "cross_join"),
    ("SELECT a.name FROM students a JOIN students b ON 1 = 1", "cross_join"),
    # allowed functions can be abused through their arguments
    ("SELECT repeat(name, 1000000000) FROM students", "function_not_allowed"),
    ("SELECT lpad(name, 1000000000, 'x') FROM students", "function_argument"),
    ("SELECT rpad(name, 2147483647) FROM students", "function_argument"),
    ("SELECT lpad(name, cgpa::int) FROM students", "function_argument"),
    ("", None),
    ("   ", None),
    ("-- just a comment", None),
    ("SELEC * FORM students", None),
    ("SELECT * FROM", None),
]
