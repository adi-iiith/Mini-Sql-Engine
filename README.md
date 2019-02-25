# Mini-Sql-Engine
A mini sql engine like Mysql in python (3)

##Some Basic Queries to run :
1. select max(A) from table1;
2. select min(B) from table2;
3. select avg(C) from table1;
4. select sum(D) from table2;
5. select A,D from table1,table2;
6. select distinct(C) from table1;
7. select B,C from table1 where A=-900;
8. select A,B from table1 where A=775 OR B=803;
9. select * from table1,table2;
10. select * from table1,table2 where table1.B=table2.B;
11. select A,D from table1,table2 where table1.B=table2.B;
12. select table1.C from table1,table2 where table1.A<table2.B;
13. select A from table4;
14. select Z from table1;
15. select B from table1,table2;
16. select distinct A,B from table1;
17. select table1.C from table1,table2 where table1.A<table2.D OR table1.A>table2.B;
18. select table1.C from table1,table2 where table1.A=table2.D;
19. select table1.A from table1,table2 where table1.A<table2.B AND table1.A>table2.D;
20. select sum(table1.A) from table1,table2;
