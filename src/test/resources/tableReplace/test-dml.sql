-- delete
DELETE FROM t1
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
DELETE FROM t1_0000

-- deteleWithOrderBy
DELETE FROM t1 WHERE f1 = 'jcole' ORDER BY timestamp_column LIMIT 1
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
DELETE FROM t1_0000 WHERE f1 = 'jcole' ORDER BY timestamp_column LIMIT 1

-- deleteWithLeftJoin
DELETE s1.t1 FROM t1 LEFT JOIN t2 ON t1.f1 = t2.f1 WHERE t2.f1 IS NULL
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
DELETE s1.t1_0000 FROM t1_0000 LEFT JOIN t2_0000 ON t1_0000.f1 = t2_0000.f1 WHERE t2_0000.f1 IS NULL

-- insert
INSERT INTO t1 (f1) VALUES (1)
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
INSERT INTO t1_0000 (f1) VALUES (1)

-- insertMulValues
INSERT INTO s1.t1 (f1) VALUES (1), (2), (3)
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
INSERT INTO s1.t1_0000 (f1) VALUES (1), (2), (3)

-- insertFromSelect
INSERT INTO s1.t1 (id) SELECT f1 FROM s1.t2 as a1 WHERE a1.f1 > 100
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
INSERT INTO s1.t1_0000 (id) SELECT f1 FROM s1.t2_0000 a1 WHERE a1.f1 > 100

-- insertOnDuplictcatKey
INSERT INTO t1 (f1, b, c) VALUES (1, 2, 3) ON DUPLICATE KEY UPDATE f1 = f1 + 1
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
INSERT INTO t1_0000 (f1, b, c) VALUES (1, 2, 3) ON DUPLICATE KEY UPDATE f1 = f1 + 1

-- replaceTable
REPLACE INTO t1 VALUES (1, 'Old', '2014-08-20 18:47:00')
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
REPLACE INTO t1_0000 VALUES (1, 'Old', '2014-08-20 18:47:00')

-- updateTable
UPDATE t1 SET f1 = f1 + 1
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
UPDATE t1_0000 SET f1 = f1 + 1


-- updateTableWithOrderBy
UPDATE s1.t1 SET t1.f1 = f1 + 1 ORDER BY t1.f1 DESC
-- t1:t1_0000,t2:t2_0000,a1:a1_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
UPDATE s1.t1_0000 SET t1_0000.f1 = f1 + 1 ORDER BY t1_0000.f1 DESC