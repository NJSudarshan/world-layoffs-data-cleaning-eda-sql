-- EXPLORATORY DATA ANALYSIS


-- This dataset contains records from Mar'20 to Mar'23 and only reported
SELECT * 
FROM world_layoffs.layoffs_staging2;


-- Let's begin with simple query to check maximum total_laid_off and this is recorded for one day
SELECT MAX(total_laid_off)
FROM world_layoffs.layoffs_staging2
;


-- Looking at percentage_laid_off to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;

-- Companies which had 1 in percentage_laid_off is basically 100 percent of they company laid off
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1;
-- These companies looks like all went out of business and got shut down


-- Order by funds_raised_millions shows how big some of these companies were
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- ---------------------------------------------------------------------------------------------------------
-- Let's dig in by using GROUP BY and ORDER BY
-- Top 5 Companies with the biggest single Layoff, the data is shown for single day
SELECT company, total_laid_off
FROM world_layoffs.layoffs_staging
ORDER BY 2 DESC
LIMIT 5;


-- Top 10 Companies with the most Total Layoffs
SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;


-- Sum of laid off by location (Top 10 location)
SELECT location, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;


-- Sum of laid off by country
SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- United states tops the list by over 250000
-- Followed by India, Netherlands but lay offs in United States is 8 times more then India (second in the list of country) 


-- Now let's check by year wise
SELECT YEAR(date), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(date)
ORDER BY 1 desc;

-- Looks like 2023 has the most laid off 
-- Although the data is only for 3 months of 2023


-- Similarly let's check on Industry
SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- Consumer, Retailer, Transportation took the most hit


-- Now let's glance on stage 
SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- Almost of layoffs have been after Post-IPO and then by Aqusition


-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Now let's advance the Explortaion
-- Rolling Total of Layoffs Per Month
SELECT SUBSTRING(date,1,7) as `Month`, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
WHERE SUBSTRING(date,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC
;


-- Now using above query in a CTE to fecth the montly rolling_total_layoffs
WITH rolling_total AS 
(
SELECT SUBSTRING(date,1,7) as `Month`, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
WHERE SUBSTRING(date,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC
)
SELECT `Month`,
total_laid_off, 
SUM(total_laid_off) OVER (ORDER BY `Month` ASC) as rolling_total_layoffs
FROM rolling_total
;

-- Now we can see rolling_total_layoffs Order By Month


-- Earlier we looked at Companies with the most Layoffs. 
-- Now let's break down yearly the total_laid_off of Top 3 companies 

WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;
















































