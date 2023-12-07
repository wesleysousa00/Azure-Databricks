-- Completeness Check
SELECT COUNT(*) AS total_rows, COUNT(column_name) AS non_null_rows
FROM your_table;

-- Consistency Check
SELECT column_name, COUNT(DISTINCT column_name) AS distinct_values
FROM your_table
GROUP BY column_name
HAVING COUNT(DISTINCT column_name) > 1;

-- Uniqueness Check
SELECT column_name, COUNT(*) AS count_duplicates
FROM your_table
GROUP BY column_name
HAVING COUNT(*) > 1;

-- Timeliness Check
SELECT *
FROM your_table
WHERE date_column > CURRENT_DATE;

-- Relevance Check
SELECT *
FROM your_table
WHERE relevant_column IS NULL;

-- Validity Check
SELECT *
FROM your_table
WHERE column_name NOT BETWEEN min_value AND max_value;
