CREATE DATABASE trainity;
show databases;
use trainity;
show tables;

select * from job_data;

# Case Study 1: Job Data Analysis
# TASK - A - Jobs Reviewed Over Time: Write an SQL query to calculate the number of jobs reviewed per hour for each day in November 2020.
select * from job_data;

SELECT ds AS Dates, COUNT(job_id) AS job_review_counts,  ROUND((COUNT(job_id)/SUM(time_spent))*3600) AS job_review_per_hour_each_day  
FROM job_data
WHERE ds BETWEEN '01-11-2020' AND '30-11-2020'
GROUP BY ds 
ORDER BY ds;


# TASK - B - Throughput Analysis:
select round(count(event)/ sum(time_spent),2) as weekly_throughput
from job_data;

select ds as dates, round( count(event)/ sum(time_spent),2) as daily_throughput
from job_data
group by ds
order by ds;

# TASK - C - Language Share Analysis:

select  language, count(language)  as  "total of each language",
(count(language)/(select count(*) from job_data)*100) "percentage share of each language"
from job_data 
group by language;

# TASK - deallocate prepare - Duplicate Rows Detection:

SELECT * 
FROM job_data
GROUP BY ds, job_id, actor_id, event, language,  time_spent, org 
HAVING COUNT(*)>1;

# Case Study 2: Investigating Metric Spike

# Table 1 -  users

create table users(
user_id int, 
created_at varchar(100),
company_id int, 
language varchar(50),
activated_at varchar(100),
state varchar(50)
);

show variables like 'secure_file_priv';

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
into table users
fields terminated by ','
enclosed by '"'
lines terminated by "\n"
ignore 1 rows;

update users set created_at = str_to_date(created_at, '%d-%m-%Y %H:%i');
update users set activated_at = str_to_date(activated_at, '%d-%m-%Y %H:%i');
select * from users;

# table 2 - events

create table events(
user_id int, 
occred_at varchar(100), 
event_type varchar(50),
event_name varchar(100),
location varchar(50),
device varchar(50),
user_type int
);

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
into table events
fields terminated by ','
enclosed by '"'
lines terminated by "\n"
ignore 1 rows;

alter table events 
rename column occred_at to occurred_at;

update events set occurred_at = str_to_date(occurred_at, '%d-%m-%Y %H:%i');

# table 3 - email_events

create table email_events(
user_id int, 
occured_at varchar(100), 
action varchar(100), 
user_type int
);

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
into table email_events
fields terminated by ','
enclosed by '"'
lines terminated by "\n"
ignore 1 rows;

alter table email_events 
rename column occured_at to occurred_at;

update email_events set occurred_at = str_to_date(occurred_at, '%d-%m-%Y %H:%i');

select * from users;

SELECT * 
FROM events;

SELECT * 
FROM email_events;

# Tasks A: Weekly User Engagement: Write an SQL query to calculate the weekly user engagement.
select extract(week from occurred_at) as Weeks,
count(distinct user_id) as "Weekly Active Users"
from events
where event_type = 'engagement'
group by weeks
order by weeks;

# Task B: User Growth Analysis: Write an SQL query to calculate the user growth for the product.

select months, users, round(((users/Lag(users,1) over (order by months) - 1 ) *100),2) as "Growth in %"
from
(
select extract(month from created_at) as months, count(activated_at) as users
from users
where activated_at not in("") 
group by 1
order by 1 
) sub;

# Task C: Weekly Retention Analysis: Write an SQL query to calculate the weekly retention of users based on their sign-up cohort.

select first as Weeks, 
sum(case when weekNumber = 0 then 1 else 0 end) as "Week 0",
sum(case when weekNumber = 1 then 1 else 0 end) as "Week 1",
sum(case when weekNumber = 2 then 1 else 0 end) as "Week 2",
sum(case when weekNumber = 3 then 1 else 0 end) as "Week 3",
sum(case when weekNumber = 4 then 1 else 0 end) as "Week 4",
sum(case when weekNumber = 5 then 1 else 0 end) as "Week 5",
sum(case when weekNumber = 6 then 1 else 0 end) as "Week 6",
sum(case when weekNumber = 7 then 1 else 0 end) as "Week 7",
sum(case when weekNumber = 8 then 1 else 0 end) as "Week 8",
sum(case when weekNumber = 9 then 1 else 0 end) as "Week 9",
sum(case when weekNumber = 10 then 1 else 0 end) as "Week 10",
sum(case when weekNumber = 11 then 1 else 0 end) as "Week 11",
sum(case when weekNumber = 12 then 1 else 0 end) as "Week 12",
sum(case when weekNumber = 13 then 1 else 0 end) as "Week 13",
sum(case when weekNumber = 14 then 1 else 0 end) as "Week 14",
sum(case when weekNumber = 15 then 1 else 0 end) as "Week 15",
sum(case when weekNumber = 16 then 1 else 0 end) as "Week 16",
sum(case when weekNumber = 17 then 1 else 0 end) as "Week 17",
sum(case when weekNumber = 18 then 1 else 0 end) as "Week 18" 
from
( 
select m.user_id, m.login_week, n.first, m.login_week - first as weekNumber
from 
(select user_id, extract(week from occurred_at) as login_week 
from events
group by 1,2) m, 
(select user_id, min(extract(week from occurred_at)) as first
from events
group by 1) n
where m.user_id = n.user_id
) sub
group by first
order by first;

select extract(week from occurred_at) as weeks, 
count(distinct user_id) as no_of_users 
from events
where event_type="signup_flow"  and event_name="complete_signup" 
group by weeks 
order by weeks;

with cte1 as(
select distinct user_id, 
extract(week from occurred_at) as signup_week
from events
where event_type = 'signup_flow'
and event_name = 'complete_signup' and extract(week from occurred_at) > 30),
cte2 as (
select distinct user_id, 
extract(week from occurred_at) as engagement_week
from events
where event_type = 'engagement'
)
select  count(user_id) total_engaged_users, 
sum(case when retention_week > 0 then 1 else 0 end) as retained_users
from 
(select a.user_id, a.signup_week,
b.engagement_week, b.engagement_week - a.signup_week as retention_week
from cte1 a left join cte2 b on a.user_id = b.user_id
order by a.user_id ) sub;

# Task D: Weekly Engagement Per Device: Write an SQL query to calculate the weekly engagement per device.
 
select distinct device 
from events;

select device, 
extract(week from occurred_at) as weeks, 
count(distinct user_id) as no_of_users 
from events 
where event_type="engagement"
group by device, weeks 
order by weeks; 


# Task E: Email Engagement Analysis: Write an SQL query to calculate the email engagement metrics.

select  distinct action from email_events;

select user_id, action, count(action) as no_of_actions
from email_events
group by user_id, action;

select action , count(action) as action_count
from email_events 
group by action;

select 
(sum(case when email_category="email_opened" then 1 else 0 end)/sum(case when email_category="email_sent" then 1 else 0 end))*100 as open_rate,
(sum(case when email_category="email_clickthrough" then 1 else 0 end)/sum(case when email_category="email_sent" then 1 else 0 end))*100 as click_rate
from (
	select *, 
	case 
		when action in ("sent_weekly_digest", "sent_reengagement_email") then ("email_sent")
		when action in ("email_open") then ("email_opened")
		when action in ("email_clickthrough") then ("email_clickthrough")
	end as email_category
	from email_events) as alias;


