select * from layoffs;
-- 1. remove duplicate 
-- 2. standardize the data
-- 3. null value and blank value
-- 4. remove any columns

CREATE table layoff_staging like layoffs;
select * from layoff_staging2;

with duplicate_cte as 
(
select *,
row_number() over(
partition by company,location,industry,total_laid_off,
percentage_laid_off,`date`,stage,country,
funds_raised_millions) as row_num
from layoff_staging
)
select * from duplicate_cte where row_num > 1;

insert layoff_staging select * from layoffs;

-- select * from layoff_staging where company = 'Hibob' 
   
with duplicate_cte as 
(
select *,
row_number() over(
partition by company,location,industry,total_laid_off,
percentage_laid_off,`date`,stage,country,
funds_raised_millions) as row_num
from layoff_staging
)
delete from duplicate_cte where row_num > 1;

insert into layoff_staging2
select *,
row_number() over(
partition by company,location,industry,total_laid_off,
percentage_laid_off,`date`,stage,country,
funds_raised_millions) as row_num
from layoff_staging;

CREATE TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoff_staging2 where row_num > 1;

delete from layoff_staging2 where row_num > 1;

select * from layoff_staging2;

-- standardizing data

select distinct(trim(company)) from layoff_staging2;

select company,trim(company) from layoff_staging2;

update layoff_staging2 set company = trim(company);

select distinct industry from layoff_staging2 order by 1;

select * from layoff_staging2 where industry like 'Crypto%';
select distinct industry from layoff_staging2;

select distinct location from layoff_staging2 order by 1;

select distinct country from layoff_staging2;

select * from layoff_staging2 where country like 'United States%' order by 1;

select distinct country, trim(trailing '.' from country) from layoff_staging2
order by 1;

update layoff_staging2 set country = trim(trailing '.' from country) where country like 'United States%';


UPDATE layoff_staging2  set industry = "Crypto" where industry like "Crypto%";

select `date`
from layoff_staging2;

update layoff_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');

alter table layoff_staging2
modify column `date` DATE;

select * from layoff_staging2
where total_laid_off is null and percentage_laid_off is null;

select * from layoff_staging2 where industry is null or industry = "";

update layoff_staging2 set industry = null where industry = "";

select * from layoff_staging2 where company = "Bally's Interactive";

select t1.industry,t2.industry from layoff_staging2 t1 
join layoff_staging2 t2 
	on t1.company = t2.company 
    and t1.location = t2.location
where (t1.industry is null or t1.industry = "")
and t2.industry is not null;

update layoff_staging2 t1 
join layoff_staging2 t2 
	on t1.company = t2.company 
set t1.industry = t2.industry 
where t1.industry is null 
and t2.industry is not null;

select * from layoff_staging2;



select * from layoff_staging2
where total_laid_off is null and percentage_laid_off is null;

delete from layoff_staging2
where total_laid_off is null and percentage_laid_off is null;


alter table layoff_staging2 
drop column row_num;

-- end data cleaning

-- start data exporation
select max(total_laid_off),max(percentage_laid_off)
from layoff_staging2;

select * from layoff_staging2 where percentage_laid_off = 1
order by total_laid_off DESC;

select * from layoff_staging2;

select max(`date`),min(`date`) from layoff_staging2;

select company,sum(total_laid_off)
from layoff_staging2 
group by company
order by 2 DESC;

select industry,sum(total_laid_off)
from layoff_staging2 
group by industry
order by 2 DESC;


select year(`date`),sum(total_laid_off)
from layoff_staging2 
group by year(`date`)
order by 1 DESC;

select stage,sum(total_laid_off)
from layoff_staging2 
group by stage
order by 2 DESC;


select company,avg(percentage_laid_off)
from layoff_staging2 
group by company
order by 2 DESC;

select substring(`date`,1,7) as month, sum(total_laid_off)
from layoff_staging2
where substring(`date`,1,7) is not null
group by month
order by 1 asc;

with rolling_total as
(
select substring(`date`,1,7) as month, sum(total_laid_off) as total_off
from layoff_staging2
where substring(`date`,1,7) is not null
group by month
order by 1 asc
)
select month,total_off,sum(total_off) over(order by month) as rolling_role
from rolling_total;


select company,year(`date`),sum(total_laid_off)
from layoff_staging2 
group by company,year(`date`)
order by 3 desc;

with company_year(company,years,total_laid_off) as
(
select company,year(`datcoviddeathscoviddeathse`),sum(total_laid_off)
from layoff_staging2 
group by company,year(`date`)
), company_year_rank as (
select *,dense_rank() over(partition by years order by total_laid_off DESC) as ranking
from company_year
where years is not null
)
select * from company_year_rank
where ranking <=5;