-- createTrigger
CREATE TRIGGER tg1 BEFORE INSERT ON s1.t1 FOR EACH ROW SET @sum = @sum + NEW.amount
-- t1:t1_0000,t2:t2_0000,v1:v1_0000,e1:e1_0000,f1:f1_0000,idx_f1:idx_f1_0000,tg1:tg1_0000,s1:s1_0000
CREATE TRIGGER tg1 BEFORE INSERT ON s1.t1_0000 FOR EACH ROW SET @sum = @sum + NEW.amount