use layoffs_data;

-- Creation of table for importation of data

CREATE TABLE layoffs (
	company text,
    location text,
    industry text,
    total_laid_off int DEFAULT NULL,
    percentage_laid_off text,
    date text,
    stage text,
    country text,
    fund_raised_millions int DEFAULT NULL
);

-- Enable of local_infile 

SHOW VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = 1;

-- Load date into table

LOAD DATA LOCAL INFILE '/Users/shiyuan/Documents/layoffs/layoffs.csv'
INTO TABLE layoffs_data
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS (
	company,
    location,
    industry,
    total_laid_off,
    percentage_laid_off,
    date,
    stage,
    country,
    fund_raised_millions
);

-- Check num of row imported

SELECT 
	COUNT(*)
FROM
	layoffs;
  
-- 1. Remove duplicates  

-- Create a duplicate table to clean the data from

CREATE TABLE layoffs_staging 
LIKE layoffs;

INSERT layoffs_staging
SELECT * 
FROM layoffs;

-- Count the number of row to identify duplicate 

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
	PARTITION BY company, 
    location, 
    industry, 
    total_laid_off, 
    percentage_laid_off, 
    date, 
    stage, 
    country, 
    fund_raised_millions
) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Ensure it is a duplicate

SELECT *
FROM layoffs_staging
WHERE company = 'Cazoo';

-- Make another duplicate table BUT with addition column of row_num

CREATE TABLE layoffs_staging_2 (
	company text,
    location text,
    industry text,
    total_laid_off int DEFAULT NULL,
    percentage_laid_off text,
    date text,
    stage text,
    country text,
    fund_raised_millions int DEFAULT NULL,
    row_num int 
);

INSERT INTO layoffs_staging_2
SELECT *,
ROW_NUMBER() OVER(
	PARTITION BY company, 
    location, 
    industry, 
    total_laid_off, 
    percentage_laid_off, 
    date, 
    stage, 
    country, 
    fund_raised_millions
) AS row_num
FROM layoffs_staging;

-- Delete the duplicate

DELETE 
FROM layoffs_staging_2
WHERE row_num > 1;

-- 2. Standardise the data

-- Trim company column 

SELECT DISTINCT company
FROM layoffs_staging_2
ORDER BY 1;

SELECT company, TRIM(company)
FROM layoffs_staging_2;

UPDATE layoffs_staging_2
SET company = TRIM(company);

-- Location is clean

SELECT DISTINCT location
FROM layoffs_staging_2
ORDER BY 1;

-- Ensure that same industry has been documented in the same name

SELECT DISTINCT industry
FROM layoffs_staging_2
ORDER BY 1;

SELECT *
FROM layoffs_staging_2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging_2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Ensure that same country has been documented in the same name

SELECT DISTINCT country
FROM layoffs_staging_2
ORDER BY 1;

SELECT *
FROM layoffs_staging_2
WHERE country LIKE 'United States%';

UPDATE layoffs_staging_2
SET country = 'United States'
WHERE country LIKE 'United States%';

-- Set date

SELECT date,
STR_TO_DATE(date, '%m/%d/%Y')
FROM layoffs_staging_2;

UPDATE layoffs_staging_2
SET date = STR_TO_DATE(date, '%m/%d/%Y');

ALTER TABLE layoffs_staging_2
MODIFY COLUMN date DATE;

-- 3. Null values or blank values

-- Remove row with Null/Blank in both total_laid_off & percentage_laid_off

SELECT *
FROM layoffs_staging_2
WHERE 
	total_laid_off IS NULL AND 
    percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging_2
WHERE 
	total_laid_off IS NULL AND 
    percentage_laid_off IS NULL;

-- Looking into industry Null/Blank
    
SELECT *
FROM layoffs_staging_2
WHERE 
	industry IS NULL OR 
    industry = '';
    
UPDATE layoffs_staging_2
SET industry = NULL
WHERE industry = '';
    
SELECT *
FROM layoffs_staging_2
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging_2 st1
JOIN layoffs_staging_2 st2
	ON st1.company = st2.company
WHERE 
	st1.industry IS NULL AND
    st2.industry IS NOT NULL;
    
UPDATE layoffs_staging_2 st1
JOIN layoffs_staging_2 st2
	ON st1.company = st2.company
SET st1.industry = st2.industry
WHERE 
	st1.industry IS NULL AND
    st2.industry IS NOT NULL;
    
-- 4. Remove any columns

ALTER TABLE layoffs_staging_2
DROP COLUMN row_num;




-- 5. Exploration of data

-- Total layoffs by Year 

SELECT 
	SUM(total_laid_off),
    YEAR(date)
FROM layoffs_staging_2
GROUP BY YEAR(date);

-- Total no of DISTINCT company

SELECT COUNT(DISTINCT company)
FROM layoffs_staging_2;

-- Average layoff per company

SELECT 
	SUM(total_laid_off) / COUNT(DISTINCT company)
FROM layoffs_staging_2;

-- Yearly layoffs

SELECT 
	SUM(total_laid_off),
    YEAR(date)
FROM layoffs_staging_2
GROUP BY YEAR(date)
ORDER BY 2;

-- Weekly trend

SELECT 
	SUM(total_laid_off),
    WEEK(date)
FROM layoffs_staging_2
GROUP BY WEEK(date)
ORDER BY 2;

-- Total laid off by TOP 5 country

SELECT 
	SUM(total_laid_off),
    country
FROM layoffs_staging_2
GROUP BY country
ORDER BY 1 DESC
LIMIT 5;

-- Total laid off by TOP 10 comapny
SELECT
	SUM(total_laid_off),
    company
FROM layoffs_staging_2
GROUP BY company
ORDER BY 1 DESC
LIMIT 10;

-- Total layoffs by TOP 10 industry

SELECT 
	SUM(total_laid_off),
    industry
FROM layoffs_staging_2
GROUP BY industry
ORDER BY 1 DESC
LIMIT 10;

-- Extract the data  

SELECT *
FROM layoffs_staging_2
ORDER BY 1
LIMIT 2000;
