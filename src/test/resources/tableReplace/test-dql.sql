-- select
select * from t1
-- t1:t1_0000
SELECT * FROM t1_0000

-- selectWithMuliTable
select * from t1,t2
-- t1:t1_0000,t2:t2_0000
SELECT * FROM t1_0000, t2_0000

-- selectWithMuliTableWithAlias
select * from t1 a1, t2
-- t1:t1_0000,t2:t2_0000,a1:a1_0000
SELECT * FROM t1_0000 a1, t2_0000

-- selectWithMuliTableWithAliasWithField
select t1.t1, t2, a1.t1, f1 from t1 a1, t2
-- t1:t1_0000,t2:t2_0000,a1:a1_0000
SELECT t1_0000.t1, t2, a1.t1, f1 FROM t1_0000 a1, t2_0000

-- selectFieldAdd
select t1.f1 + t2.f1 from t1, t2
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000,t2:t2_0000,a2:a2_0000,t3:t3_0000,a3:a3_0000
SELECT t1_0000.f1 + t2_0000.f1 FROM t1_0000, t2_0000

-- selectWithFunction
SELECT AVG(score), t1.* FROM t1
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000,t2:t2_0000,a2:a2_0000,t3:t3_0000,a3:a3_0000
SELECT AVG(score), t1_0000.* FROM t1_0000

-- selectWithFunctionAndOrderBy
SELECT CONCAT(t1.f1, ', ', t2.f1) AS f1 FROM s1.t1 ORDER BY t1.f1
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000,t2:t2_0000,a2:a2_0000,t3:t3_0000,a3:a3_0000
SELECT CONCAT(t1_0000.f1, ', ', t2_0000.f1) AS f1 FROM s1.t1_0000 ORDER BY t1_0000.f1

-- selectWithGroupByAndOrderBy
SELECT t1.f1, COUNT(t2.f1) FROM t1, t2 GROUP BY t1.f1 ORDER BY t2.f1
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000,t2:t2_0000,a2:a2_0000,t3:t3_0000,a3:a3_0000
SELECT t1_0000.f1, COUNT(t2_0000.f1) FROM t1_0000, t2_0000 GROUP BY t1_0000.f1 ORDER BY t2_0000.f1

-- selectWithGroupByAndOrderByAndHaving
SELECT t1.f1, COUNT(t2.f1) c FROM t1, t2 GROUP BY t1.f1 having c > t1.f1 ORDER BY t2.f1
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000,t2:t2_0000,a2:a2_0000,t3:t3_0000,a3:a3_0000
SELECT t1_0000.f1, COUNT(t2_0000.f1) AS c FROM t1_0000, t2_0000 GROUP BY t1_0000.f1 HAVING c > t1_0000.f1 ORDER BY t2_0000.f1

-- selectWithGroupByAndOrderByAndHavingAndLimit
SELECT t1.f1, COUNT(t2.f1) c FROM t1, t2 GROUP BY t1.f1 having c > t1.f1 ORDER BY t2.f1 limit 1
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000,t2:t2_0000,a2:a2_0000,t3:t3_0000,a3:a3_0000
SELECT t1_0000.f1, COUNT(t2_0000.f1) AS c FROM t1_0000, t2_0000 GROUP BY t1_0000.f1 HAVING c > t1_0000.f1 ORDER BY t2_0000.f1 LIMIT 1

-- selectWithInnerJoin
SELECT t1.f1, t2.f1 FROM t1 INNER JOIN t2 on t1.f1 = t2.f1
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000,t2:t2_0000,a2:a2_0000,t3:t3_0000,a3:a3_0000
SELECT t1_0000.f1, t2_0000.f1 FROM t1_0000 INNER JOIN t2_0000 ON t1_0000.f1 = t2_0000.f1

-- selectWithLeftJoin
SELECT t1.f1 FROM s1.t1 LEFT JOIN (t1, t2, t3) ON (t1.f1 = t2.f1 AND t2.f2 = t2.f2 AND t2.f1 = t3.f1)
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000,t2:t2_0000,a2:a2_0000,t3:t3_0000,a3:a3_0000
SELECT t1_0000.f1 FROM s1.t1_0000 LEFT JOIN t1_0000, t2_0000, t3_0000 ON t1_0000.f1 = t2_0000.f1 AND t2_0000.f2 = t2_0000.f2 AND t2_0000.f1 = t3_0000.f1

-- selectWithLeftJoinLeftJoin
SELECT t1.f1 FROM s1.t1 as a1 LEFT JOIN t2 ON a1.id = t2.f1 LEFT JOIN t3 ON t2.f1 = t3.f1
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000,t2:t2_0000,a2:a2_0000,t3:t3_0000,a3:a3_0000
SELECT t1_0000.f1 FROM s1.t1_0000 a1 LEFT JOIN t2_0000 ON a1.id = t2_0000.f1 LEFT JOIN t3_0000 ON t2_0000.f1 = t3_0000.f1

-- selectWithUnionWithOrderByWithLimit
(SELECT t1.f1 FROM s1.t1 a1) UNION (SELECT t2.f1 FROM t2) ORDER BY a1.f1, t2.f2 limit 1
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000,t2:t2_0000,a2:a2_0000,t3:t3_0000,a3:a3_0000
SELECT t1_0000.f1 FROM s1.t1_0000 a1 UNION (SELECT t2_0000.f1 FROM t2_0000) ORDER BY a1.f1, t2_0000.f2 LIMIT 1

-- selectWithSubQuery
SELECT t1.f1 FROM s1.t1 a1 WHERE a1.f1 = (SELECT t2.f1 FROM t2 where t2.f1 > 1 limit 1)
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000,t2:t2_0000,a2:a2_0000,t3:t3_0000,a3:a3_0000
SELECT t1_0000.f1 FROM s1.t1_0000 a1 WHERE a1.f1 = ( SELECT t2_0000.f1 FROM t2_0000 WHERE t2_0000.f1 > 1 LIMIT 1 )

-- selectWithSubQueryWithAny
SELECT t1.f1 FROM s1.t1 a1 WHERE a1.f1 > ANY (SELECT t2.f1 FROM t2 where t2.f1 > 1 limit 10)
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000,t2:t2_0000,a2:a2_0000,t3:t3_0000,a3:a3_0000
SELECT t1_0000.f1 FROM s1.t1_0000 a1 WHERE a1.f1 > ANY (SELECT t2_0000.f1 FROM t2_0000 WHERE t2_0000.f1 > 1 LIMIT 10)

-- selectWithSubQueryWithSome
SELECT t1.f1 FROM s1.t1 a1 WHERE a1.f1 <> SOME (SELECT t2.f1 FROM t2 where t2.f1 > 1 limit 10)
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000,t2:t2_0000,a2:a2_0000,t3:t3_0000,a3:a3_0000
SELECT t1_0000.f1 FROM s1.t1_0000 a1 WHERE a1.f1 <> SOME (SELECT t2_0000.f1 FROM t2_0000 WHERE t2_0000.f1 > 1 LIMIT 10)

-- selectWithNotExists
SELECT DISTINCT t1.f1 FROM s1.t1 WHERE EXISTS (SELECT f1 FROM t2 a2 WHERE t1.f1 = a2.f1)
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000,t2:t2_0000,a2:a2_0000,t3:t3_0000,a3:a3_0000
SELECT DISTINCT t1_0000.f1 FROM s1.t1_0000 WHERE EXISTS (SELECT f1 FROM t2_0000 a2 WHERE t1_0000.f1 = a2.f1)

-- selectWithSubQueryRow
SELECT t1.f1, (SELECT t2.f1 a2 from s1.t2 where t1.f1 = t2.f1) f2 FROM s1.t1 WHERE (t1.f1 , t1.f2) = (SELECT t3.f1, t3.f2 FROM t3 WHERE t3.f1 = 10)
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000,t2:t2_0000,a2:a2_0000,t3:t3_0000,a3:a3_0000
SELECT t1_0000.f1, ( SELECT t2_0000.f1 AS a2 FROM s1.t2_0000 WHERE t1_0000.f1 = t2_0000.f1 ) AS f2 FROM s1.t1_0000 WHERE (t1_0000.f1, t1_0000.f2) = ( SELECT t3_0000.f1, t3_0000.f2 FROM t3_0000 WHERE t3_0000.f1 = 10 )

-- selectWithCorrelatedSubQuery
SELECT t1.f1 FROM s1.t1 AS a1 WHERE a1.f1 = (SELECT f2 FROM t2 WHERE t2.f1 = (SELECT f1 FROM s1.t3 WHERE t2.f1 = t3.f1))
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000,t2:t2_0000,a2:a2_0000,t3:t3_0000,a3:a3_0000
SELECT t1_0000.f1 FROM s1.t1_0000 a1 WHERE a1.f1 = ( SELECT f2 FROM t2_0000 WHERE t2_0000.f1 = ( SELECT f1 FROM s1.t3_0000 WHERE t2_0000.f1 = t3_0000.f1 ) )

-- selectWithSubqueryAndUnion
SELECT t1.f1 FROM s1.t1 WHERE f1 IN (SELECT t2 FROM t2 a2 where a2.f1 > 1 UNION ALL SELECT f1 FROM t3 where t3.f1 > 2)
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000,t2:t2_0000,a2:a2_0000,t3:t3_0000,a3:a3_0000
SELECT t1_0000.f1 FROM s1.t1_0000 WHERE f1 IN (SELECT t2 FROM t2_0000 a2 WHERE a2.f1 > 1 UNION ALL SELECT f1 FROM t3_0000 WHERE t3_0000.f1 > 2)

-- selectWithSubQueryWithJoinWithAliasWithUpperCase
select a.a, bb, c from a ax left join bx c where ax.id in (select aid from BB where id = bx.id and name in (select name from d, e where d.x = 1))
-- a:a_0000,ax:ax_0000,x:x_0000,bx:bx_0000,bb:bb_0000,d:d_0000
SELECT a_0000.a, bb, c FROM a_0000 ax LEFT JOIN bx_0000 c WHERE ax.id IN (SELECT aid FROM bb_0000 WHERE id = bx_0000.id AND name IN (SELECT name FROM d_0000, e WHERE d_0000.x = 1))