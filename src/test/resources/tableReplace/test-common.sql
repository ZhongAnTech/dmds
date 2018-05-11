-- caseIgnore
select * from t1
-- T1:t1_0000
SELECT * FROM t1_0000

-- mulitSqls
select * from t1; select * from t2; select * from t3
-- t1:t1_0000,t2:t2_0000
SELECT * FROM t1_0000;
SELECT * FROM t2_0000;
SELECT * FROM t3

-- withFieldAndSchema
select t1.f1 from s1.t1, t2
-- t1:t1_0000,t2:t2_0000,s1:s1_0000
SELECT t1_0000.f1 FROM s1.t1_0000, t2_0000

-- withBackQuote
select `t1`.`f1` from `s1`.`t1`, `t2`
-- t1:t1_0000,t2:t2_0000,s1:s1_0000
SELECT `t1_0000`.`f1` FROM `s1`.`t1_0000`, `t2_0000`

-- witAlias
select t1.f1, a1.f2, t2.f1, a2.f2 from t1 a1, t2 as a2
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,a2:a2_0000
SELECT t1_0000.f1, a1.f2, t2_0000.f1, a2.f2 FROM t1_0000 a1, t2_0000 a2