insertion_grade_table = '''
DO $$
BEGIN
    
    TRUNCATE TABLE g_pre_dm.grade;

    INSERT INTO g_pre_dm.grade(id, grade_name)
    SELECT 
        id, 
        grade
    FROM g_dds.grade;

    UPDATE g_pre_dm.grade
    SET sort = 100
    WHERE id = 283045;

    UPDATE g_pre_dm.grade
    SET sort = 0
    WHERE id = 115637;

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error: %', SQLERRM;
END;
$$
 LANGUAGE plpgsql;
'''
