WITH date_spine AS (
    -- Menghasilkan tanggal dari 2010 hingga 2030
    SELECT UNNEST(generate_series(DATE '2010-01-01', DATE '2030-12-31', INTERVAL 1 DAY)) AS date_day
)

SELECT
    CAST(date_day AS DATE) AS date_id,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day,
    EXTRACT(DOW FROM date_day) AS day_of_week_num,
    DAYNAME(date_day) AS day_name,
    EXTRACT(QUARTER FROM date_day) AS quarter
FROM date_spine
