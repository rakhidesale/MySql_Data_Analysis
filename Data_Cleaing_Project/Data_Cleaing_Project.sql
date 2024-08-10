-- Data Cleaning Steps

-- 1. Check for duplicates and remove any
-- 2. Standardize data and fix errors
-- 3. Handle null values and see what actions can be taken
-- 4. Remove unnecessary columns and rows

-- Step 1: Review the original data
SELECT *
FROM layoffs;

-- Step 1.1: Remove Duplicates

-- Create a staging table by copying the structure of the `layoffs` table
CREATE TABLE layoffs_staging LIKE layoffs;

-- Verify the structure of the newly created table
SELECT *
FROM layoffs_staging;

-- Insert all records from `layoffs` table into the `layoffs_staging` table
INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

-- Check for duplicate records in the staging table
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`
) AS row_num
FROM layoffs_staging;

-- Use a CTE (Common Table Expression) to identify and isolate duplicate rows
WITH duplicates_cte AS (
  SELECT *,
  ROW_NUMBER() OVER(
  PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`
  ) AS row_num
  FROM layoffs_staging
)
SELECT *
FROM duplicates_cte
WHERE row_num > 1;

-- Confirm if the duplicate data is correct by checking specific companies
SELECT *
FROM layoffs_staging
WHERE company = 'Oda';

-- Another check for duplicates, this time including more columns for partitioning
WITH duplicates_cte AS (
  SELECT *,
  ROW_NUMBER() OVER(
  PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
  `date`, stage, country, funds_raised_millions
  ) AS row_num
  FROM layoffs_staging
)
SELECT *
FROM duplicates_cte
WHERE row_num > 1;

-- Check for duplicates for another specific company
SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

-- Step 1.2: Remove Duplicates (Creating a new table and then deleting duplicates)

-- Create a new staging table with an additional column `row_num`
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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Verify the structure of the new staging table
SELECT *
FROM layoffs_staging2;

-- Insert data into `layoffs_staging2` along with row numbers for duplicates identification
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
`date`, stage, country, funds_raised_millions
) AS row_num
FROM layoffs_staging;

-- Check the rows with duplicates (row_num > 1)
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Delete duplicate records where row_num > 1
DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

-- Verify the deletion of duplicates
SELECT *
FROM layoffs_staging2;

-- Step 2: Standardize Data

-- 2.1: Trim white spaces from `company` column
SELECT company, TRIM(company)
FROM layoffs_staging2;

-- Update the `company` column to remove leading/trailing spaces
UPDATE layoffs_staging2
SET company = TRIM(company);

-- 2.2: Check distinct values in `industry` column
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- 2.3: Standardize the `industry` column by fixing similar but inconsistent values (e.g., 'Crypto' and 'CryptoCurrency')
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- Update `industry` column to a consistent value
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Check again to confirm the standardization
SELECT DISTINCT industry
FROM layoffs_staging2;

-- 2.4: Check distinct values in `location` and `country` columns
SELECT DISTINCT location
FROM layoffs_staging2;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- Identify inconsistencies in `country` column (e.g., 'United States.' and 'United States')
SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1;

-- Trim trailing periods from `country` column
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

-- Update the `country` column to remove the trailing period
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- 2.5: Standardize the `date` column format
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- Update the `date` column to the standardized format
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Modify the `date` column's data type to `DATE`
ALTER TABLE layoffs_staging2
MODIFY COLUMN `DATE` DATE;

-- Step 3: Handle Null Values

-- 3.1: Check for rows with null values in `total_laid_off` and `percentage_laid_off` columns
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- 3.2: Update rows with empty `industry` column to NULL
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Check if any `industry` column is still null or empty
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- 3.3: Look for any other inconsistencies related to null/empty `industry`
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Join the table to itself to find where `industry` is null in one row but not in another for the same company/location
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
	AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry='')
AND t2.industry IS NOT NULL;
    
-- Check the `industry` values in such rows
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
	AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry='')
AND t2.industry IS NOT NULL;

-- Update the `industry` column in rows where it is null by taking values from rows where it is not null
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

-- Recheck for any remaining null or empty `industry` columns
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- Step 4: Remove Unnecessary Columns/Rows

-- 4.1: Check for any specific records before deletion
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%' ;

-- View all remaining records
SELECT *
FROM layoffs_staging2;

-- 4.2: Remove rows where `total_laid_off` and `percentage_laid_off` are both null
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

-- Verify the deletion
SELECT *
FROM layoffs_staging2;

-- 4.3: Drop the `row_num` column as it's no longer needed
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
