--- Basic sql complexity is Easy. This needs to be solved first before going to question2

-- 1. Display all the information of the Employee table.
SELECT * FROM cards_ingest.emp;


-- 2. Display unique Department names from Employee table.
-- department numbers from employee table
select distinct(deptno) from cards_ingest.emp order by deptno


-- department names from employee table
SELECT DISTINCT dname 
FROM  cards_ingest.dept dept 
JOIN cards_ingest.emp emp ON emp.deptno = dept.deptno;


-- 3. List the details of the employees in ascending order of their salaries.
select * from cards_ingest.emp order by sal


-- 4. List the employees who joined before 1981.
select * from cards_ingest.emp where hiredate<'1981-01-01'


-- 5. List the employees who are joined in the year 1981
select * from cards_ingest.emp where hiredate>='1980-01-01' and hiredate<='1980-12-31'


-- 6. List the Empno, Ename, Sal, Daily Sal of all Employees in the ASC order of AnnSal. (Note devide sal/30 as annsal)
select empno,ename,sal,sal/30 as annalsal from  cards_ingest.emp order by annalsal


-- 7. List the employees who are working for the department name ACCOUNTING
SELECT empno, ename 
FROM cards_ingest.emp emp 
JOIN cards_ingest.dept dept ON emp.deptno = dept.deptno 
WHERE dname = 'ACCOUNTING';


-- 8. List the employees who does not belong to department name ACCOUNTING
SELECT empno, ename 
FROM cards_ingest.emp emp 
JOIN cards_ingest.dept dept ON emp.deptno = dept.deptno 
WHERE dname != 'ACCOUNTING';