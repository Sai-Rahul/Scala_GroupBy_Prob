object Sql_Joins1 CREATE TABLE employees (
  emp_id INT PRIMARY KEY,
  name VARCHAR(50),
  department_id INT
);

INSERT INTO employees VALUES
  (1, 'Karthik', 101),
(2, 'Veena', 102),
(3, 'Meena', NULL),
(4, 'Veer', 103),
(5, 'Ajay', 104),
(6, 'Vijay', NULL),
(7, 'Keerthi', 105);

select * from employees;

CREATE TABLE departments (
  department_id INT PRIMARY KEY,
  department_name VARCHAR(50)
);

INSERT INTO departments VALUES
  (101, 'HR'),
(102, 'Finance'),
(103, 'IT'),
(104, 'Marketing'),
(106, 'Operations');

select * from departments;

/*List all employees, including those without a department, and their department details.*/

select e.emp_id,e.name,e.department_id,d.department_name
from employees e
left outer join
departments d
  on e.department_id = d.department_id
where d.department_name is null;

select e.emp_id,e.name,e.department_id,d.department_name
from employees e
join
departments d;

/* List all departments and their employee details, including departments with no
employees.*/
select e.emp_id,e.name,d.department_id,d.department_name
from employees e
right outer join
departments d
  on e.department_id = d.department_id;

/*12. Find departments without any employees assigned to them.*/

select e.emp_id,e.name,d.department_id,d.department_name
from employees e
right outer join
departments d
  on e.department_id = d.department_id
where emp_id is null;

/*  Fetch details of all employees and departments, showing 'No Department' for missing
department details and 'No Employee' for missing employee details. */

select
e.emp_id,e.name,
coalesce(d.department_id,'No department') AS department_id,
coalesce(d.department_name,'No department') AS department_name
  from employees e
left outer join
departments d
  on e.department_id = d.department_id

union

select
coalesce(e.emp_id, 'No_Employee') AS emp_id,
coalesce(e.name, 'No_Employee') AS name,
d.department_id,d.department_name
from employees e
right outer join
departments d
  on e.department_id = d.department_id;


SELECT
e.emp_id,
e.name,
COALESCE(d.department_id, 'No Department') AS department_id,
COALESCE(d.department_name, 'No Department') AS department_name
  FROM
employees e
  LEFT OUTER JOIN
departments d
  ON
e.department_id = d.department_id

UNION

SELECT
COALESCE(e.emp_id, 'No Employee') AS emp_id,
COALESCE(e.name, 'No Employee') AS name,
d.department_id,
d.department_name
FROM
employees e
  RIGHT OUTER JOIN
departments d
  ON
e.department_id = d.department_id;





CREATE TABLE dummy (
  emp_id INT PRIMARY KEY,
  name VARCHAR(50),
  department_id INT
);

INSERT INTO dummy VALUES
  (1, 'Karthik', 101),
(2, 'Veena', 102),
(3, 'Meena', NULL),
(4, 'Veer', 103),
(5, 'Ajay', NULL),
(6, 'Vijay', NULL),
(7, 'Keerthi', NULL);
select * from dummy;
CREATE TABLE depart (
  department_id INT PRIMARY KEY,
  department_name VARCHAR(50)
);

INSERT INTO depart VALUES
  (101, 'HR'),
(102, 'Finance'),
(NULL, 'IT'),
(NULL, 'Marketing'),
(NULL, 'Operations');

select emp_id, name,dummy.department_id,depart.department_name
from dummy
  left outer join
depart
on dummy.department_id = depart.department_id;{

}
