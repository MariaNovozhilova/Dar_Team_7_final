insertion_esg_and_skills_tables = '''
DO $$
BEGIN
    
    TRUNCATE TABLE g_pre_dm.employee_skill_grade;
    TRUNCATE TABLE g_pre_dm.skills;

    CALL etl_func.pre_dm_em_skill_grade('dbms_and_employee_grade', 'dbms', 'dbms', 'dbms');
    CALL etl_func.pre_dm_em_skill_grade('program_and_employee_grade', 'program', 'program', 'program');
    CALL etl_func.pre_dm_em_skill_grade('programming_language_and_employee_grade', 'programming_language', 'programming_language', 'prog_lang');
    CALL etl_func.pre_dm_em_skill_grade('tool_and_employee_grade', 'tool', 'tool', 'tool');
    CALL etl_func.pre_dm_em_skill_grade('framework_and_employee_grade', 'framework', 'framework', 'framework');
    CALL etl_func.pre_dm_em_skill_grade('platform_and_employee_grade', 'platform', 'platform', 'platform');
    CALL etl_func.pre_dm_software_type_em_skill_grade('software_type_employee_grade', 'software_type', 'software_type', 'type', 'sw_t');

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Transaction rolled back due to error: %', SQLERRM;
END;
$$
 LANGUAGE plpgsql;
'''
