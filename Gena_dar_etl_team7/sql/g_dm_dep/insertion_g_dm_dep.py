insertion_g_dm_dep='''
DO $$
BEGIN
    TRUNCATE TABLE g_dm_dep.employee;
    TRUNCATE TABLE g_dm_dep.employee_skill_grade;
    TRUNCATE TABLE g_dm_dep.grade;
    TRUNCATE TABLE g_dm_dep.skills;

    WITH RECURSIVE dates AS (
    SELECT
        CAST('1999-01-01' AS DATE) AS start_date,
        CAST('1999-03-31' AS DATE) AS end_date,
        1999 AS year,
        1 AS quarter
    UNION ALL
    SELECT
        (start_date + INTERVAL '3 MONTH')::DATE,
        (start_date + INTERVAL '3 MONTH' - INTERVAL '1 day' + INTERVAL '3 MONTH')::DATE,
        CASE 
            WHEN EXTRACT(MONTH FROM start_date + INTERVAL '3 MONTH') = 1 THEN year + 1 
            ELSE year 
        END,
        CASE 
            WHEN EXTRACT(MONTH FROM start_date + INTERVAL '3 MONTH') = 1 THEN 1 
            ELSE quarter + 1 
        END
    FROM dates
    WHERE start_date < '2024-02-01'
),
    joined_skills_data AS (
        SELECT 
            employee_id,
            skill_id,
            grade_id,
            dt AS skill_date,
            EXTRACT(YEAR FROM dt) AS y,
            EXTRACT(QUARTER FROM dt) AS q,
            sort
        FROM g_pre_dm.employee_skill_grade esg
    ),
    relevant_skill AS (
        SELECT 
            d.YEAR AS y,
            d.quarter::integer,
            jsd.employee_id,
            jsd.skill_id,
            jsd.grade_id,
            jsd.skill_date,
            ucpy."year" AS cer_year,
            jsd.sort,
            CASE 
                WHEN jsd.skill_date < d.start_date THEN 'исторические'
                ELSE 'актуальные'
            END AS data_type,
            row_number() OVER (PARTITION BY d.year, d.quarter, jsd.employee_id, jsd.skill_id ORDER BY jsd.sort DESC) AS rank_grade,
            ucpy.cer_flag AS cer_flag,
            ncl.quantity_with_cer AS quantity_with_cer,
            ncl.quatity_without_cer AS quantity_without_cer
        FROM dates d
        LEFT JOIN joined_skills_data jsd ON jsd.skill_date <= d.end_date
        JOIN g_pre_dm.employee_year_cer_flag ucpy ON ucpy."year" + 1 = jsd.y
            AND ucpy.id = jsd.employee_id
        JOIN (SELECT
                ucpy."year" AS dt_y,
                sum(cer_flag) AS quantity_with_cer,
                COUNT(DISTINCT id) - sum(cer_flag) AS quatity_without_cer
            FROM 
                g_pre_dm.employee_year_cer_flag ucpy
            GROUP BY ucpy."year"
            ) AS ncl ON ncl.dt_y + 1 = jsd.y
    )
    
    INSERT INTO g_dm_dep.employee_skill_grade(y, quarter, employee_id, skill_id, grade_id, skill_date, sort, data_type, rank_grade, cer_flag, quantity_with_cer, quantity_without_cer)
	SELECT
		y,
          quarter,
            employee_id,
            skill_id,
            grade_id,
            skill_date,
            cer_year
            sort,
            data_type,
            rank_grade,
            cer_flag,
            quantity_with_cer,
            quantity_without_cer
	FROM relevant_skill;
	
    INSERT INTO g_dm.employee(id, department, "position")
    SELECT id, department, "position"
    FROM g_pre_dm.employee;

    INSERT INTO g_dm.grade(id, grade)
    SELECT id, grade
    FROM g_pre_dm.grade;

    INSERT INTO g_dm.skills(id, skill_name, skill_type)
    SELECT id, skill_name, skill_type
    FROM g_pre_dm.skills;

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error: %', SQLERRM;
END;
$$
 LANGUAGE plpgsql;
'''
