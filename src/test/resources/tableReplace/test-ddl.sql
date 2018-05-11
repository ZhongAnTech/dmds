-- createTable
CREATE TABLE `s1`.`t1` (`t1` INT NOT NULL, `f2` VARCHAR(45) NULL, PRIMARY KEY (`t1`))
-- T1:t1_0000
CREATE TABLE `s1`.`t1_0000` ( `t1` INT NOT NULL,  `f2` VARCHAR(45) NULL,  PRIMARY KEY (`t1`) )

-- alterTableDropColumn
ALTER TABLE `s1`.`t1` DROP COLUMN `f3`
-- t1:t1_0000
ALTER TABLE `s1`.`t1_0000` DROP COLUMN `f3`

-- alterTableAddColumn
ALTER TABLE `s1`.`blob` ADD COLUMN `blob` BLOB NULL AFTER `f2`
-- blob:blob_0000
ALTER TABLE `s1`.`blob_0000` ADD COLUMN `blob` BLOB NULL AFTER `f2`

-- renameTable
rename table `s1`.`t1` to t2
-- t1:t1_0000,t2:t2_0000
RENAME TABLE `s1`.`t1_0000` TO t2

-- alterTableRename
alter table `s1`.`t1` rename to t2
-- t1:t1_0000,t2:t2_0000
RENAME TABLE `s1`.`t1_0000` TO t2

-- alterTableCreateIndex
ALTER TABLE `s1`.`blob` ADD INDEX `idx_id` (`blob` ASC)
-- blob:blob_0000
ALTER TABLE `s1`.`blob_0000` ADD INDEX `idx_id` (`blob` ASC)

-- alterTableDropIndex
ALTER TABLE `s1`.`idx_id` DROP INDEX `idx_id`
-- idx_id:idx_id_0000
ALTER TABLE `s1`.`idx_id_0000` DROP INDEX `idx_id`

-- alterTableOptionAutoIncrment
ALTER TABLE t1 AUTO_INCREMENT = 13
-- t1:t1_0000,t2:t2_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
ALTER TABLE t1_0000 AUTO_INCREMENT = 13

-- alterTablecChangeColumn
ALTER TABLE t1 CHANGE f1 f2 BIGINT NOT NULL
-- t1:t1_0000,t2:t2_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
ALTER TABLE t1_0000 CHANGE COLUMN f1 f2 BIGINT NOT NULL

-- alterTablecModifyColumn
ALTER TABLE t1 MODIFY f1 BIGINT NOT NULL
-- t1:t1_0000,t2:t2_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
ALTER TABLE t1_0000 MODIFY COLUMN f1 BIGINT NOT NULL

-- alterTableDropForenignKey
ALTER TABLE t1 DROP FOREIGN KEY key1
-- t1:t1_0000,t2:t2_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
ALTER TABLE t1_0000 DROP FOREIGN KEY key1

-- createOrReplaceView
CREATE OR REPLACE VIEW `v1` AS select * from t1
-- t1:t1_0000,t2:t2_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
CREATE OR REPLACE VIEW `v1` AS SELECT * FROM t1_0000

-- alterTableAddIndex
ALTER TABLE t1 ADD INDEX (d), ADD UNIQUE (a)
-- t1:t1_0000,t2:t2_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
ALTER TABLE t1_0000 ADD INDEX (d), ADD UNIQUE INDEX (a)

-- showCreateTable
show create table t1
-- t1:t1_0000,t2:t2_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
SHOW CREATE TABLE t1_0000

-- showCreateTableWithSchema
show create table `s1`.`t1`
-- t1:t1_0000,t2:t2_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
SHOW CREATE TABLE `s1`.`t1_0000`

-- createIndex
CREATE INDEX idx_f1 ON t1 (f1(10))
-- t1:t1_0000,t2:t2_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
CREATE INDEX idx_f1 ON t1_0000 (f1(10))

-- createTableLike
CREATE TABLE t1 LIKE t2
-- t1:t1_0000,t2:t2_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
CREATE TABLE t1_0000 LIKE t2_0000

-- createTableAsSelect
CREATE TABLE t1 AS SELECT * FROM t2
-- t1:t1_0000,t2:t2_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
CREATE TABLE t1_0000 SELECT * FROM t2_0000

-- createTablePartitionByList
CREATE TABLE t1 (
  s1 INT,
  s2 INT
)
PARTITION BY LIST (s2) (
  PARTITION p1 VALUES IN (1)
)
-- t1:t1_0000,t2:t2_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
CREATE TABLE t1_0000 ( s1 INT,  s2 INT ) PARTITION BY LIST (s2) ( PARTITION p1 VALUES IN (1) )

-- createTrigger
CREATE TRIGGER tg1 BEFORE INSERT ON s1.t1 FOR EACH ROW SET @sum = @sum + NEW.amount
-- t1:t1_0000,t2:t2_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000,tg1:tg1_0000,s1:s1_0000
CREATE TRIGGER tg1 BEFORE INSERT ON s1.t1_0000 FOR EACH ROW SET @sum = @sum + NEW.amount

-- dropTable
DROP table t1
-- t1:t1_0000,t2:t2_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
DROP TABLE t1_0000

-- dropTableIfExists
DROP table IF EXISTS t1
-- t1:t1_0000,t2:t2_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
DROP TABLE IF EXISTS t1_0000

-- truncateTable
truncate table t1
-- t1:t1_0000,t2:t2_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000
TRUNCATE TABLE t1_0000