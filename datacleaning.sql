select * 
from layoffs;

-- copying raw data columns into staging table 
create table layoffs_staging
like layoffs;
select *
from layoffs_staging;

-- inserting all data of layoffs into layoff staging 
insert layoffs_staging
select *
from layoffs;

-- removing duplicates by partition by we add the row number to check the duplication
select *,
row_number() over(
partition by company, industry, total_laid_off,percentage_laid_off, 'date') as row_num
from layoffs_staging;

-- create the temporary table for above query 
with duplicate_cte as
(
select *,
row_number() over(
partition by company, location,
 industry, total_laid_off,percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
-- query to see only duplicate values 
select * from duplicate_cte
where row_num > 1;

select *
from layoffs_staging
where company = 'Cazoo';

with duplicate_cte as
(
select *,
row_number() over(
partition by company, location,
 industry, total_laid_off,percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
delete from duplicate_cte
where row_num > 1;
-- copy to clipboard then create statement to create exact copy table for adding row num in it 
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
   `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2
select *,
row_number() over(
partition by company, location,
 industry, total_laid_off,percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoffs_staging;

select *
from layoffs_staging2
where row_num > 1;

delete
from layoffs_staging2
where row_num > 1;
-- deleted the duplicate rows 
select *
from layoffs_staging2
where row_num > 1;

-- Standardizing data
-- 1. removing spaces
select company,trim(company)
from layoffs_staging2;

-- trim removes the white spaces 
update layoffs_staging2
set company = Trim(company);

select  *
from layoffs_staging2
where industry like 'Crypto%';
-- keeping same name of crypto removing cryptocurrency 
update layoffs_staging2
Set industry = 'Crypto'
where industry like 'Crypto%';

-- nothing to update in location,
select  *
from layoffs_staging2
where country like 'United States%'
order by 1;
-- remove period from US country 
select distinct country, TRIM(trailing '.' from country)
from layoffs_staging2
order by 1; 

-- removing duplicates of US 
update layoffs_staging2
set country = TRIM(trailing '.' from country)
where country like 'United States%';

select *
from layoffs_staging2;

-- changing date from text to date format 
select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;


update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');

select `date`
from layoffs_staging2;

-- above change it into date format now changing data type of date column 
alter table layoffs_staging2
modify column `date` date;

-- null blank values
select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2
where industry is null 
or industry = '';

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2 
 on t1.company = t2.company 
 set t1.industry = t2.industry 
 where t1.industry is null 
and t2.industry is not null;

update layoffs_staging2
set industry = null 
where industry = '';

select *
from layoffs_staging2
where company= 'Airbnb';

-- remove unneccessary column 
delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


select *
from layoffs_staging2;
alter table layoffs_staging2
drop column row_num;
