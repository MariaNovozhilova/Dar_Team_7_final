insertion_employee_cer_table = '''
DO $$
BEGIN
    TRUNCATE TABLE g_pre_dm.employee_certificate;

    INSERT INTO g_pre_dm.employee_certificate(user_id, title, "year")
    SELECT 
        user_id,
        title, 
        "year"
    FROM g_dds.employee_certificate;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error: %', SQLERRM;
END;
$$
 LANGUAGE plpgsql;
'''