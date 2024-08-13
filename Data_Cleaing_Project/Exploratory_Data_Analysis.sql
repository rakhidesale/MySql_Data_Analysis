-- Exploratory Data Analysis (EDA) for layoffs_staging2 Table

-- 1. Review the complete dataset
SELECT *
FROM layoffs_staging2;

-- 2. Identify the maximum values for `total_laid_off` and `percentage_laid_off`
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

-- 3. Find companies with 100% layoffs and sort by `total_laid_off` (largest first)
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

-- 4. Find companies with 100% layoffs and sort by `funds_raised_millions` (largest first)
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- 5. Calculate the total number of layoffs per company and sort by highest number of layoffs
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- 6. Identify the earliest and latest layoff dates
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- 7. Calculate total layoffs per industry and sort by highest number of layoffs
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- 8. Calculate total layoffs per country and sort by highest number of layoffs
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- 9. Review the entire dataset again for additional insights
SELECT *
FROM layoffs_staging2;

-- 10. Calculate total layoffs per date and sort by date (most recent first)
SELECT `date`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `date`
ORDER BY 1 DESC;

-- 11. Calculate total layoffs per year and sort by year (most recent first)
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- 12. Calculate total layoffs per stage of the company and sort by highest number of layoffs
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- 13. Calculate total percentage of layoffs per company and sort by highest percentage
SELECT company, SUM(percentage_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- 14. Calculate average percentage of layoffs per company and sort by highest average
SELECT company, AVG(percentage_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- 15. Review the entire dataset once more to ensure completeness
SELECT *
FROM layoffs_staging2;

-- 16. Calculate total layoffs per month and sort by month (ascending)
SELECT SUBSTRING(`date`,1,7) AS `Month`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC;

-- 17. Calculate rolling total of layoffs per month
WITH Rolling_Total AS (
  SELECT SUBSTRING(`date`,1,7) AS `Month`, SUM(total_laid_off) AS Total_off
  FROM layoffs_staging2
  WHERE SUBSTRING(`date`,1,7) IS NOT NULL
  GROUP BY `Month`
  ORDER BY 1 ASC
)
SELECT `Month`, Total_off,
SUM(Total_off) OVER(ORDER BY `MONTH`) AS Rolling_total
FROM Rolling_Total;

-- 18. Calculate total layoffs per company and sort by highest number of layoffs
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- 19. Calculate total layoffs per company per year and sort by company name (ascending)
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY company ASC;

-- 20. Calculate total layoffs per company per year and sort by highest number of layoffs
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

-- 21. Calculate ranking of companies by total layoffs per year using DENSE_RANK()
WITH Company_Year (Company, Years, Total_laid_off)  AS (
  SELECT company, YEAR(`date`), SUM(total_laid_off)
  FROM layoffs_staging2
  GROUP BY company, YEAR(`date`)
)
SELECT *, 
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
ORDER BY Ranking ASC;

-- 22. Identify the top 5 companies with the highest layoffs per year
WITH Company_Year (Company, Years, Total_laid_off)  AS (
  SELECT company, YEAR(`date`), SUM(total_laid_off)
  FROM layoffs_staging2
  GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS (
  SELECT *, 
  DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
  FROM Company_Year
  WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;
