NOTE - this example used to work in Scope, now it is a an error

Error running script.

ScopeClient.CompilationErrorException: E_CSC_USER_NOEQUIJOINFOUNDFORCOMMAJOIN: N
o equijoin could be found for the comma-joined tables employees and departments.

Description:
When rewriting comma join syntax to INNER JOIN, Scope requires an equijoin in th
e WHERE clause for each pair of tables.
Resolution:
Rewrite the SELECT query to use explicit INNER JOIN syntax, and specify an equij
oin for the ON-clause. Use this pattern: employees INNER JOIN departments ON emp
loyees.? == departments.?... at token [FROM], line 33
near the ###:
**************

rs_implicit_join =
    SELECT employees.DepID AS EmpDepId,
        departments.DepID,
        employees.EmpName,
        departments.DepName
         ### FROM employees,departments;

OUTPUT rs_cross_join
TO "/my/Outputs/cross_join.txt";

OUTPUT rs_inner
TO "/my/Outputs/inner_join.txt";

OUTPUT rs_implici
**************