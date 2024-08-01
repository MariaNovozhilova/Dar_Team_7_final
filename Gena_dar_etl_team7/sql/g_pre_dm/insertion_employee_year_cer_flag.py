insertion_employee_year_cer_flag = '''
DO $$
BEGIN
    TRUNCATE TABLE g_pre_dm.employee_year_cer_flag;

    WITH RECURSIVE years AS (
      SELECT 1997 AS year
      UNION
      SELECT year + 1 AS year
      FROM years
      WHERE year < EXTRACT(YEAR FROM NOW())
    ),
    user_years AS (
      SELECT *
      FROM years, (SELECT DISTINCT id FROM g_pre_dm.employee e) AS e
    ),
    user_cer_prev_year AS (
      SELECT id,
             year,
             CASE
               WHEN EXISTS(SELECT *
                             FROM g_pre_dm.employee_certificate ec
                             WHERE u.id = user_id AND u.year = ec.year)
               THEN 1
               ELSE 0
             END cer_flag
      FROM user_years u
      ORDER BY id, year DESC
    )
    INSERT INTO g_pre_dm.employee_year_cer_flag(id, "year", cer_flag)
    SELECT *
    FROM user_cer_prev_year;

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error: %', SQLERRM;
END;
$$
 LANGUAGE plpgsql;
'''