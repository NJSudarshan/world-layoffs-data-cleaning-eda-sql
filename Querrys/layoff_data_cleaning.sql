
-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022


SELECT * 
FROM world_layoffs.layoffs;



-- First thing is to create a staging table, then will clean and work in staging table and keep the raw data as it is if something goes wrong
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;


INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;


-- Now let's claen the tabel layoffs_staging by following below steps:
-- 1. check for duplicates and remove if any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary 





-- -----------------------------------------------------------------------------------------------------------------
-- REMOVE DUPLICATES

# First let's check for duplicates
SELECT *
FROM world_layoffs.layoffs_staging
;

SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`) AS row_num
	FROM 
		world_layoffs.layoffs_staging;

SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
-- Let's look at company = oda to confirm duplicates
SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'oda'
;
-- Company = oda showing multiple entries which is legitimate and cannot be deleted.

-- So we need to check everything to be pricise. these are our real duplicates. 
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
-- These are real duplicates. The ones where the row number is > 1 or 2 or greater essentially needs to removed

-- Lets try to remove duplicates with CTE
WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1
)
DELETE
FROM DELETE_CTE
;
-- Since the delete statement cannot be update with CTE in MySQL. 


-- let's create a new column in layoffs_staging and add row numbers in. 
-- Then duplicate the layoffs_satging as layoffs_staging2, delete where row numbers is greater then 1 or 2, then delete that column.
ALTER TABLE world_layoffs.layoffs_staging ADD row_num INT;

SELECT *
FROM world_layoffs.layoffs_staging
;

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging;

-- Now lets remove the duplicates deleting row_num >= 2
DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;




-- -----------------------------------------------------------------------------------------------------------------
-- STANDARDIZING DATA

-- Let's look at each column separately 
SELECT company
FROM layoffs_staging2;

-- Let's trim the column company to remove the blank spaces at the beginning or ending and also update.
SELECT company, TRIM(company)
FROM layoffs_staging2; 

UPDATE layoffs_staging2
SET company = TRIM(company);

-- Let's check on column location
SELECT DISTINCT location
FROM layoffs_staging2;


-- let's check on column industry
SELECT DISTINCT industry
FROM  layoffs_staging2
order by 1;

-- Found some null and blank values
-- Also the crypto is written in various types but in actual it's crypto
-- So let's standardize crypto first

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'crypto%';

-- Now for country
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- Found United States written in various types let's fix it.
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


-- Since the data was imported without any modifications the date column is needs to be converted from text
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%y');

SELECT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;




-- ----------------------------------------------------------------------------------------------------------------
-- 3. NULL VALUES

-- Earlier when we glanced industry column it had blank space
-- Let's check once again including NULL
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT * 
FROM layoffs_staging2
WHERE commpany = 'Airbnb';	

-- Found 4 rows with blank or NULL in industry column
-- But before that let's convert blanks into NULL
UPDATE layoffs_staging2
SET industry = null
WHERE industry = ''; 

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
		ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

--  Now let's try populate by using company column
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
		ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

-- There was another NULL in industry column where company like Bally 
-- Let's check it
SELECT * 
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

-- Since there are no multiple rows for company Like 'Bally%'. Nothing can be done.	
-- Null values in total_laid_off, percentage_laid_off, and funds_raised_millions looks normal. 
-- Having null makes it easier for calculations during the EDA phase
-- So there isn't anything to be changed with the null values




-- -----------------------------------------------------------------------------------------------------------------
-- 4. REMOVING COLUMN OR ROWS 

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Null in both total_laid_off and percent_laid_off are useless data so let's remove those rows.
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging2;

-- Let's remove row_num from layoffs_staging2 which completes the data cleaning
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;





-- HERE IS THE FIANL TABLE AFTER CLEANING THE DATA
SELECT * 
FROM world_layoffs.layoffs_staging2;
