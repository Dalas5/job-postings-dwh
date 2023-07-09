
## DDL Commands
-- Q1
create table calendar_dim (
    date_key integer not null,
    full_date date not null,
    day_num_in_month integer not null,
    day_name varchar(128) not null,
    week_num_in_year integer not null,
    month integer not null,
    month_name varchar(128) not null,
    quarter integer not null,
    year integer not null,
    month_end_flag varchar(128) not null,
    same_day_year_ago date not null,
    primary key(date_key)
);

-- Q2
create view application_date_dim as
select
    date_key as app_date_key,
    full_date as app_full_day,
    day_num_in_month as app_day_num_in_month,
    day_name as app_day_name,
    week_num_in_year as app_week_num_in_year,
    month as app_month,
    month_name as app_month_name,
    quarter as app_quarter,
    year as app_year,
    month_end_flag as app_month_end_flag,
    same_day_year_ago as app_same_day_year_ago
from calendar_dim

-- Q3
create view adverts_date_dim as
select
    date_key as advert_date_key,
    full_date as advert_full_day,
    day_num_in_month as advert_day_num_in_month,
    day_name as advert_day_name,
    week_num_in_year as advert_week_num_in_year,
    month as advert_month,
    month_name as advert_month_name,
    quarter as advert_quarter,
    year as advert_year,
    month_end_flag as advert_month_end_flag,
    same_day_year_ago as advert_same_day_year_ago
from calendar_dim

-- Q4
create table adverts_dim (
    key integer not null,
    id varchar(128),
    activeDays integer,
    applyUrl varchar(),
    publicationDate date,
    status varchar(128),
    primary key(key)
);

-- Q5
create table applicant_dim (
    key integer not null,
    firstName varchar(128),
    lastName varchar(128),
    age integer,
    age_range varchar(128)
    primary key (key)
);

-- Q6
create table job_dim (
    key integer not null,
    job_id varchar(128),
    city varchar(128),
    title varchar(128),
    sector varchar(128),
    primary key(key)
);

-- Q7
create table benefit_dim (
    key integer not null,
    benefit_name varchar(128),
    primary key(key)
);

-- Q8
create table skill_dim (
    key integer not null,
    skill_name varchar(128),
    primary key(key)
);

-- Q9
create table company_dim (
    key integer not null,
    company_name varchar(128),
    primary key(key)
);

-- Q10
create table job_benefit_bridge (
    job_key integer not null,
    benefit_key integer not null,
    foreign key(job_key) references job_dim(key),
    foreign key(benefit_key) referencies benefit_dim(key)
);

-- Q11
create table application_fact (
    key intenger not null,
    date_key integer not null,
    applicant_key integer not null,
    company_key integer not null,
    job_key integer not null,
    primary key(key),
    foreign key(date_key) references calenar_dim(date_key),
    foreign key(applicant_key) references applicant_dim(key),
    foreign key(company_key) references company_dim(key),
    foreign key(job_key) references job_dim(key)
);

-- Q12
create table application_skillset_bridge (
    application_key integer not null,
    skill_key integer not null
    foreign key(application_key) references application_fact(key),
    foreign key(skill_key) references skill_dim(key)
);


## File Copy commands

COPY calendar_dim
FROM 's3://devtestsbucket/presentation/calendar_dim'
IAM_ROLE 'arn:aws:iam::0123456789012:role/MyRedshiftRole'
FORMAT AS PARQUET;


COPY advert_dim
FROM 's3://devtestsbucket/presentation/advert_dim'
IAM_ROLE 'arn:aws:iam::0123456789012:role/MyRedshiftRole'
FORMAT AS PARQUET;

COPY skill_dim
FROM 's3://devtestsbucket/presentation/skill_dim'
IAM_ROLE 'arn:aws:iam::0123456789012:role/MyRedshiftRole'
FORMAT AS PARQUET;

COPY job_dim
FROM 's3://devtestsbucket/presentation/job_dim'
IAM_ROLE 'arn:aws:iam::0123456789012:role/MyRedshiftRole'
FORMAT AS PARQUET;


COPY company_dim
FROM 's3://devtestsbucket/presentation/company_dim'
IAM_ROLE 'arn:aws:iam::0123456789012:role/MyRedshiftRole'
FORMAT AS PARQUET;

COPY benefit_dim
FROM 's3://devtestsbucket/presentation/benefit_dim'
IAM_ROLE 'arn:aws:iam::0123456789012:role/MyRedshiftRole'
FORMAT AS PARQUET;


COPY applicant_dim
FROM 's3://devtestsbucket/presentation/applicant_dim'
IAM_ROLE 'arn:aws:iam::0123456789012:role/MyRedshiftRole'
FORMAT AS PARQUET;

COPY job_benefit_bridge
FROM 's3://devtestsbucket/presentation/job_benefit_bridge'
IAM_ROLE 'arn:aws:iam::0123456789012:role/MyRedshiftRole'
FORMAT AS PARQUET;

COPY application_fact
FROM 's3://devtestsbucket/presentation/application_fact'
IAM_ROLE 'arn:aws:iam::0123456789012:role/MyRedshiftRole'
FORMAT AS PARQUET;

COPY application_skillset_bridge
FROM 's3://devtestsbucket/presentation/application_skillset_bridge'
IAM_ROLE 'arn:aws:iam::0123456789012:role/MyRedshiftRole'
FORMAT AS PARQUET;

