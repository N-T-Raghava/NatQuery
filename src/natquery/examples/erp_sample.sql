-- Sample ERP-like schema + data for developing/testing NatQuery. NOT the real IIT KGP ERP schema.
DROP TABLE IF EXISTS erp_users, salaries, fees, enrollments, courses, students, faculty, departments CASCADE;

CREATE TABLE departments (dept_id TEXT PRIMARY KEY, dept_name TEXT NOT NULL);
CREATE TABLE faculty (
  emp_id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT, phone TEXT,
  department TEXT REFERENCES departments(dept_id), designation TEXT);
CREATE TABLE students (
  roll_no TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT, phone TEXT, aadhaar_no TEXT,
  department TEXT REFERENCES departments(dept_id), year INT, cgpa NUMERIC(4,2), hall TEXT,
  advisor_emp_id TEXT REFERENCES faculty(emp_id));
CREATE TABLE courses (
  course_id TEXT PRIMARY KEY, title TEXT NOT NULL, credits INT, department TEXT REFERENCES departments(dept_id),
  instructor_emp_id TEXT REFERENCES faculty(emp_id), semester TEXT);
CREATE TABLE enrollments (
  roll_no TEXT REFERENCES students(roll_no), course_id TEXT REFERENCES courses(course_id),
  semester TEXT, grade TEXT, marks INT, PRIMARY KEY (roll_no, course_id, semester));
CREATE TABLE fees (
  roll_no TEXT REFERENCES students(roll_no), semester TEXT, amount_due NUMERIC, amount_paid NUMERIC, status TEXT);
CREATE TABLE salaries (emp_id TEXT REFERENCES faculty(emp_id), month TEXT, gross NUMERIC, net NUMERIC);
CREATE TABLE erp_users (username TEXT PRIMARY KEY, password_hash TEXT NOT NULL, role TEXT);

INSERT INTO departments VALUES ('CSE','Computer Science and Engineering'),('EE','Electrical Engineering'),('MA','Mathematics');
INSERT INTO faculty VALUES
 ('F1001','Prof. Rao','rao@iitkgp.ac.in','9000000001','CSE','Professor'),
 ('F1002','Prof. Sen','sen@iitkgp.ac.in','9000000002','EE','Associate Professor'),
 ('F1003','Prof. Das','das@iitkgp.ac.in','9000000003','MA','Assistant Professor');
INSERT INTO students VALUES
 ('21CS10001','Asha Verma','asha@kgpian.iitkgp.ac.in','9111111111','123412341234','CSE',4,9.12,'RK','F1001'),
 ('21CS10002','Bhaskar Iyer','bhaskar@kgpian.iitkgp.ac.in','9111111112','234523452345','CSE',4,8.40,'RP','F1001'),
 ('21EE10003','Charu Nair','charu@kgpian.iitkgp.ac.in','9111111113','345634563456','EE',4,7.85,'LLR','F1002'),
 ('22MA10004','Deepak Roy','deepak@kgpian.iitkgp.ac.in','9111111114','456745674567','MA',3,8.95,'MMM','F1003'),
 ('22CS10005','Esha Gupta','esha@kgpian.iitkgp.ac.in','9111111115','567856785678','CSE',3,9.55,'RK','F1001'),
 ('23EE10006','Farhan Ali','farhan@kgpian.iitkgp.ac.in','9111111116','678967896789','EE',2,6.90,'LLR','F1002');
INSERT INTO courses VALUES
 ('CS201','Data Structures',4,'CSE','F1001','AUT-2025'),
 ('CS301','Databases',3,'CSE','F1001','SPR-2026'),
 ('EE201','Circuits',4,'EE','F1002','AUT-2025'),
 ('MA201','Linear Algebra',3,'MA','F1003','AUT-2025');
INSERT INTO enrollments VALUES
 ('21CS10001','CS201','AUT-2025','EX',92),('21CS10001','CS301','SPR-2026','A',85),
 ('21CS10002','CS201','AUT-2025','A',81),('21CS10002','MA201','AUT-2025','B',70),
 ('21EE10003','EE201','AUT-2025','A',84),('22MA10004','MA201','AUT-2025','EX',95),
 ('22CS10005','CS201','AUT-2025','EX',97),('23EE10006','EE201','AUT-2025','C',58);
INSERT INTO fees VALUES
 ('21CS10001','AUT-2025',100000,100000,'PAID'),('21CS10002','AUT-2025',100000,60000,'PARTIAL'),
 ('21EE10003','AUT-2025',100000,100000,'PAID'),('23EE10006','AUT-2025',100000,0,'DUE');
INSERT INTO salaries VALUES ('F1001','2026-09',250000,210000),('F1002','2026-09',180000,150000),('F1003','2026-09',150000,125000);
INSERT INTO erp_users VALUES ('21CS10001','pbkdf2$abc','student'),('F1001','pbkdf2$def','faculty');
