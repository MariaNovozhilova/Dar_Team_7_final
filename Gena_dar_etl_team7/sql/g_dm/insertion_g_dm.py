insertion_g_dm='''
DO $$
BEGIN
    TRUNCATE TABLE g_dm.employee;
    TRUNCATE TABLE g_dm.employee_skill_grade;
    TRUNCATE TABLE g_dm.grade;
    TRUNCATE TABLE g_dm.skills;

    INSERT INTO g_dm.employee_skill_grade(employee_id, skill_id, grade_id, dt, sort, highest_grade, course_rank)
    SELECT
	    esg.employee_id,
        esg.skill_id,
        esg.grade_id,
        esg.dt,
        esg.sort,
        row_number() OVER (PARTITION BY employee_id, skill_id ORDER BY sort DESC) AS highest_grade,
        ec.course_rank
    FROM 
        g_pre_dm.employee_skill_grade esg
        LEFT JOIN (
            SELECT
                user_id, 
                "year" AS year,
                count(*) AS course_count,
                dense_rank() OVER (ORDER BY count(*) DESC) AS course_rank
            FROM g_pre_dm.employee_certificate
            GROUP BY user_id, "year"
        ) ec ON ec.user_id = esg.employee_id AND ec."year" = EXTRACT(YEAR FROM esg.dt);

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
$$ LANGUAGE plpgsql;
'''
