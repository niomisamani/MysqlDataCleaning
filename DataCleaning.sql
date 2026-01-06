use world_layoffs;

select * from layoffs;

-- 1. Remove Duplicates
-- 2. Standardize Data 
-- 3. Null or Blank Values
-- 4. Remove rows and columns that are not necessary

create table layoffs_staging 
like layoffs;

select * from layoffs_staging;

insert layoffs_staging
select * from layoffs;

-- 1. Remove Duplicates

select * , row_number() 
over(partition by company,industry,total_laid_off,percentage_laid_off,`date`) as rownum
from layoffs_staging;


with duplicates_cte as 
(
select * , row_number() 
over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as rownum
from layoffs_staging
)
select * from duplicates_cte where rownum>1;

select * from layoffs_staging where company='Casper';

delete from duplicates_cte where rownum>1;


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
  `rownum` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select * from layoffs_staging2;
insert into layoffs_staging2
select * , row_number() 
over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as rownum
from layoffs_staging;

select * from layoffs_staging2 where rownum>1;

delete from layoffs_staging2 where rownum>1;

select * from layoffs_staging2;

-- Standardise data
select distinct(trim(company)) from layoffs_staging2;

update layoffs_staging2 set company=trim(company);

select distinct industry from layoffs_staging2 order by 1;

update layoffs_staging2 set industry='Crypto' where industry like '%Crypto%';

select distinct location from layoffs_staging2 order by 1;

select distinct country from layoffs_staging2 order by 1;

update layoffs_staging2 set country='United States' where country like '%United States%';

select country,trim(trailing '.' from country) from layoffs_staging2 order by 1;

update layoffs_staging2 set country=trim(trailing '.' from country) where country like '%United States%';

select `date` ,
str_to_date(`date`,'%Y/%m/%d')
from layoffs_staging2 order by 1;

update layoffs_staging2 set `date`=str_to_date(`date`,'%m/%d/%Y');

select `date` 
from layoffs_staging2 order by 1;

alter table layoffs_staging2 modify column `date` date;

select *
from layoffs_staging2 where total_laid_off is null and  percentage_laid_off is null;

select *
from layoffs_staging2 where industry is null or industry = '';

select *
from layoffs_staging2 where company='Airbnb';

select t1.industry,t2.industry from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company=t2.company
and t1.location=t2.location
where ((t1.industry is null or t1.industry='')  and t2.industry is not null);

update layoffs_staging2  set industry = Null where industry ='';

update layoffs_staging2 t1 
join layoffs_staging2 t2
on t1.company=t2.company
set t1.industry =t2.industry where 
((t1.industry is null or t1.industry='')  and t2.industry is not null);

select *
from layoffs_staging2 where company like 'Bally%';

select *
from layoffs_staging2 where total_laid_off is null and  percentage_laid_off is null;

delete from 
layoffs_staging2 where total_laid_off is null and  percentage_laid_off is null;

alter table layoffs_staging2 drop column rownum;

select *
from layoffs_staging2;