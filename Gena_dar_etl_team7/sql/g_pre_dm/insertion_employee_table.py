insertion_employee_table = '''
DO $$
BEGIN
    TRUNCATE TABLE g_pre_dm.employee;

    INSERT INTO g_pre_dm.employee(id, "name", surname, email, department, "position")
    SELECT 
        id, 
        "name", 
        surname, 
        email, 
        department, 
        "position"
    FROM g_dds.employee;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error: %', SQLERRM;
END;
$$
 LANGUAGE plpgsql;
'''