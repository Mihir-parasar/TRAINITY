CREATE TABLE users (
    user_id INT,
    created_at VARCHAR(100),
    company_id INT,
    language VARCHAR(50),
    activated_at VARCHAR(100),
    state VARCHAR(50)
);

show variables like 'secure_file_priv';

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

alter table users add column temp_created_at datetime;
UPDATE users 
SET 
    temp_created_at = STR_TO_DATE(created_at, '%d-%m-%Y %H:%i');
alter table users drop column created_at;
alter table users add column temp_activated_at datetime;
UPDATE users 
SET 
    temp_activated_at = STR_TO_DATE(activated_at, '%d-%m-%Y %H:%i');
alter table users drop column activated_at;

CREATE TABLE events (
    user_id INT,
    occurred_at VARCHAR(100),
    event_type VARCHAR(100),
    event_name VARCHAR(100),
    location VARCHAR(100),
    device VARCHAR(100),
    user_type INT
);

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
INTO TABLE events
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

alter table events add column temp_occured_at datetime;
UPDATE events 
SET 
    temp_occured_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i'); 
alter table events drop column occurred_at;

CREATE TABLE email_events (
    user_id INT,
    occurred_at VARCHAR(100),
    action VARCHAR(100),
    user_type INT
);

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
INTO TABLE email_events
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

alter table email_events add column temp_occured_at datetime;
UPDATE email_events 
SET 
    temp_occured_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');
alter table email_events drop column occurred_at;
-- --------------------------------------------------------------------------------------------
-- CASE STUDY 2
-- --------------------------------------------------------------------------------------------
-- A. Weekly User Engagement:
select week(temp_occured_at) as week_no, 
count(user_id) as user_count 
from events 
where event_type='engagement' 
group by week_no;
-- --------------------------------------------------------------------------
-- B. User Growth Analysis:
-- Monthly growth
select month(temp_created_at) as month,
count(distinct user_id) as user_count,
lag(count(distinct user_id),1) over(),
CONCAT(ROUND((count(distinct user_id)-(lag(count(distinct user_id),1) over()))/count(distinct user_id)*100,1),'%') as User_growth
from users
where temp_activated_at IS NOT NULL
group by month;
-- --------------------------------------------------------------------------
-- Weekly growth percentage
select week(temp_created_at) as week,
CONCAT(ROUND((count(user_id)-(lag(count(user_id),1) over()))/count(user_id)*100,1),'%') as weekly_User_growth
from users
where temp_activated_at IS NOT NULL
group by week;
-- --------------------------------------------------------------------------
-- C.Weekly Retention Analysis:
select 
week(temp_occured_at) as week,
count(user_id) as user_retention
from events
where event_name = 'complete_signup'
group by week;
-- --------------------------------------------------------------------------
-- D.Weekly Engagement Per Device:
select week(temp_occured_at) as week,
device,
count(user_id) as users_engaged 
from events
where event_type='engagement'
group by week,device
order by users_engaged desc;
-- --------------------------------------------------------------------------
-- E.Email Engagement Analysis:
select action,count(user_id) as user_count
from email_events
where action like 'email%'
group by action;
-- --------------------------------------------------------------------------
select week(temp_occured_at) as week_no, count(distinct user_id) as user_with_email_engagement
from email_events 
where action like 'email%' 
group by week_no;