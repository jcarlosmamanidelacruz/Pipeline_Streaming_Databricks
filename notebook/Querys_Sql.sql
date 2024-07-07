-- Databricks notebook source
select * from db_alumnos.tbalumnos order by fechaRegistroKafka desc limit 5 ;

-- COMMAND ----------

select * from TABLE_CHANGES('db_alumnos.tbalumnos', 0);

-- COMMAND ----------

select count(*)
from db_alumnos.tbalumnos

-- COMMAND ----------

select dni, curso, count(*)
from db_alumnos.tbalumnos
group by dni, curso
having count(*) > 1

-- COMMAND ----------

select dni, curso, count(*)
from db_alumnos.tbalumnos_unique
group by dni, curso
having count(*) > 1

-- COMMAND ----------

delete from db_alumnos.tbalumnos;
