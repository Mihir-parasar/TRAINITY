create database operation_analytics;
use operation_analytics;
create table if not exists job_data (
ds date not null,
job_id int,
actor_id int,
event varchar(50),
language varchar(50),
time_spent int,
org varchar(50));

insert into job_data values
("2020-11-30", 21,	1001,	"skip",	    "English",	15,	"A"),
("2020-11-30", 22,	1006,	"transfer",	"Arabic",	25,	"B"),
("2020-11-29", 23,	1003,	"decision",	"Persian",	20,	"C"),
("2020-11-28", 23,	1005,	"transfer",	"Persian",	22,	"D"),
("2020-11-28", 25,	1002,	"decision",	"Hindi",	11,	"B"),
("2020-11-27", 11,	1007,	"decision",	"French",	104,"D"),
("2020-11-26", 23,	1004,	"skip",	    "Persian",	56,	"A"),
("2020-11-25", 20,	1003,	"transfer",	"Italian",  45,	"C");


-- CASE STUDY 1
select * from job_data
-- A.Jobs Reviewed Over Time
select extract(day from ds) as day,
round(count(distinct job_id)/sum(time_spent/3600)) as jobs_reviewed_per_hour
from job_data
group by day
order by day;
-- ---------------------------------------------------------------------
-- B.Throughput Analysis:
with event_avg as (
SELECT day(ds) as day,
COUNT(event)/SUM(time_spent) AS event_per_sec
from job_data
GROUP BY day
)
select day,
event_per_sec,
AVG(event_per_sec) OVER (ORDER BY day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS 7_day_rolling_avg
FROM event_avg
-- -----------------------------------------------------------------------
-- C.Language Share Analysis:
with cte as (
select count(language) as total_cnt
from job_data)
select language,concat(round(count(language)/(select total_cnt from cte)*100),"%") as language_share
from job_data
group by language;
-- -------------------------------------------------------------------------
-- D. Duplicate Rows Detection:
select * 
from(select *,
row_number() over(partition by job_id) as cnt from job_data) as a
where cnt>1;
