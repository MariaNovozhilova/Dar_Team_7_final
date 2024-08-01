def clean_data():
    import psycopg
    import json
    import os


    def create_cfg_variable():
        f = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etl_base.json'))
        dsn_ods = json.load(f)
        conn_d = create_connection(dsn_ods["db_name"], dsn_ods["db_user"], dsn_ods["db_password"],
										dsn_ods["db_host"], dsn_ods["db_port"])
        return conn_d


    def create_connection(db_name, db_user, db_password, db_host, db_port):
        connection = None
        try:
            connection = psycopg.connect(
                dbname=db_name,
                user=db_user,
                password=db_password,
                host=db_host,
                port=db_port,
            )
            print("Connection to PostgreSQL DB successful")
        except psycopg.OperationalError as e:
            print(f"The error '{e}' occurred")
        return connection

# # tables = ['dbms', 'dbms_and_employee_level', 'domain', 'education', 'employee', 'employee_certificate', 
#           'employee_domain_experience', 'industry', 'industry_employee_experience', 'platform', 
#           'platform_and_employee_grade', 'program', 'program_and_employee_level', 'resume', 
#           'sde', 'sde_and_employee_grade']

# создаю словарь с sql скриптами всех обработок и по очереди записываю в него запросы

    SQL_dict = {
    'sql_dael' : '''TRUNCATE TABLE dds.dbms_and_employee_grade; INSERT INTO dds.dbms_and_employee_grade
    (id, user_id, updated_at, sort, grade, active, "date", dbms)
    SELECT id
    , CAST (regexp_replace(ldael.user_id, '[^0-9]', '', 'g') AS INTEGER)
    , CAST(ldael.updated_at AS date)
    , ldael.sort
    , CAST (CASE WHEN ldael.grade = '' THEN '115637'
        ELSE regexp_replace(ldael.grade, '[^0-9]', '', 'g') END AS INTEGER)
    , CAST(CASE WHEN ldael.active = 'Да' THEN 'True'
                WHEN ldael.active = 'Нет' THEN 'False' END AS BOOL)
    , CAST(NULLIF(ldael.date, '') AS date)
    , CAST(regexp_replace(ldael.dbms, '[^0-9]', '', 'g') AS INTEGER)
    FROM lgc.dbms_and_employee_grade AS ldael;
    UPDATE dds.dbms_and_employee_grade
    SET sort = CAST(CASE WHEN grade = '115637' THEN '100'
        WHEN grade = '115638' THEN '200'
        WHEN grade = '115639' THEN '300'
        WHEN grade = '115640' THEN '400'
        WHEN grade = '115641' THEN '500'
        WHEN grade = '283045' THEN '600' 
        ELSE NULL END AS INTEGER);
    -- delete duplicates
    DELETE FROM dds.dbms_and_employee_grade
    WHERE id IN
        (SELECT id FROM dds.dbms_and_employee_grade
        EXCEPT SELECT MIN(id) FROM dds.dbms_and_employee_grade
        GROUP BY user_id, sort, grade, dbms);
    -- delete wrong dbms id
    DELETE FROM dds.dbms_and_employee_grade
    WHERE dbms NOT IN (SELECT id FROM dds.dbms);
    '''}

    SQL_dict['sql_d'] = ''' TRUNCATE TABLE dds.dbms;
    INSERT INTO dds.dbms
    (id, updated_at, sort, active, dbms)
    SELECT ld.id
    , CAST(ld.updated_at AS date)
    , ld.sort
    , CAST(CASE WHEN ld.active = 'Да' THEN 'True'
                WHEN ld.active = 'Нет' THEN 'False' END AS BOOL)
    , CAST (ld.dbms as text)
    FROM lgc.dbms AS ld;

    UPDATE dds.dbms
    SET updated_at = CASE WHEN updated_at > NOW() THEN NOW() 
    ELSE updated_at END;'''

    SQL_dict['sql_dom'] =  '''TRUNCATE TABLE dds.domain; INSERT INTO dds.domain
    (id, updated_at, sort, active, "domain")
    SELECT ldom.id
    , CAST(ldom.updated_at AS date)
    , ldom.sort
    , CAST(CASE WHEN ldom.active = 'Да' THEN 'True'
                WHEN ldom.active = 'Нет' THEN 'False' END AS BOOL)
    ,  ldom."domain"
    FROM lgc.domain AS ldom;'''

    SQL_dict['sql_le'] = '''TRUNCATE TABLE dds.employee_education_level; INSERT INTO dds.employee_education_level
    (user_id, id, year_graduated, updated_at, institution_name, sort, level
    , faculty_department, short_name, active, qualification, specialty)
    SELECT le.user_id
    , le.id
    , CASE WHEN le.year_graduated > 2030 THEN null ELSE le.year_graduated END
    , CAST(le.updated_at AS date)
    , le.institution_name
    , le.sort
    , CAST(regexp_replace(le.level, '[^0-9]', '', 'g') AS integer)
    , le.faculty_department
    , le.short_name
    , CAST(CASE WHEN le.active = 'Да' THEN 'True'
                WHEN le.active = 'Нет' THEN 'False' END AS BOOL)
    , le.qualification
    , le.specialty
    FROM lgc.employee_education_level AS le;

    -- set first capital letters in specialty columns for better duplicates identification
    update dds.employee_education_level
    set specialty = initcap(specialty);

    -- delete duplicates
    DELETE FROM dds.employee_education_level
    WHERE id IN
        (SELECT id FROM dds.employee_education_level
        EXCEPT SELECT MIN(id) FROM dds.employee_education_level
        GROUP BY user_id, sort, level, year_graduated, faculty_department, specialty);'''

    SQL_dict['sql_lem'] = '''TRUNCATE TABLE dds.employee;
    INSERT INTO dds.employee
    (email, id, city, updated_at, registered_at, birth_date, last_check_in, active,
    "position", "name", company, login, department, gender, surname, frc)
    SELECT --COALESCE (NULLIF(lem.email,''),'ivanivanych@korus.ru')
    ge.email
    , lem.id
    , COALESCE (NULLIF(lem.city,''),'Городок') AS city
    , NULLIF(lem.updated_at,''):: date AS updated_at
    , NULLIF(lem.registered_at,''):: date AS registered_at
    , CAST(COALESCE (NULLIF(lem.birth_date,''),'1984-10-10') AS date) AS birth_date
    --, ge.birth_date
    , NULLIF(lem.last_check_in,''):: date AS last_check_in
    , CAST(CASE WHEN lem.active = 'Да' THEN 'True'
                WHEN lem.active = 'Нет' THEN 'False' END AS BOOL)
    , NULLIF(lem.position,'') AS "position"
    --, COALESCE (NULLIF(lem."name",''),'Иван') AS name
    , ge.name
    , lem.company
    , lem.login
    , ltrim(replace(lem.department, '.', ''))
    , COALESCE (NULLIF(lem.gender,''),'male') AS gender
    --, COALESCE (NULLIF(lem.surname,''),'Иванов') AS surname
    , ge.surname
    , lem.frc
    FROM lgc.employee AS lem
    LEFT JOIN g_dds.employee ge ON ge.id = lem.id;'''

    SQL_dict['sql_lemc'] = '''TRUNCATE TABLE dds.employee_certificate;
    INSERT INTO dds.employee_certificate
    (user_id, id, year, updated_at, title, organisation, sort, active)
    SELECT lemc.user_id
    , lemc.id
    , lemc.year
    , NULLIF(lemc.updated_at,''):: date AS updated_at
    , lemc.title
    , lemc.organisation
    , lemc.sort
    , CAST(CASE WHEN lemc.active = 'Да' THEN 'True'
                WHEN lemc.active = 'Нет' THEN 'False' END AS BOOL)
    FROM lgc.employee_certificate AS lemc;
    DELETE FROM dds.employee_certificate
    WHERE title LIKE '%Наименование%';'''

    SQL_dict['sql_lede'] = '''TRUNCATE TABLE dds.employee_domain_experience; INSERT INTO dds.employee_domain_experience
    (user_id, id, updated_at, domain, sort, experience, active, "date")
    SELECT lede.user_id
    , lede.id
    , NULLIF(lede.updated_at,''):: date AS updated_at
    , CAST(regexp_replace(lede.domain, '[^0-9]', '', 'g') AS INTEGER)
    , lede.sort
    , CAST(CASE WHEN lede.experience = '' THEN '115761' 
    ELSE regexp_replace(lede.experience, '[^0-9]', '', 'g') END AS INTEGER)
    , CAST(CASE WHEN lede.active = 'Да' THEN 'True'
                WHEN lede.active = 'Нет' THEN 'False' END AS BOOL)
    , NULLIF(lede.date,''):: date
    FROM lgc.employee_domain_experience AS lede;

    -- delete duplicates
    DELETE FROM dds.employee_domain_experience
    WHERE id IN
        (SELECT id FROM dds.employee_domain_experience
        EXCEPT SELECT MIN(id) FROM dds.employee_domain_experience
        GROUP BY user_id, sort, domain, experience);'''

    SQL_dict['sql_li'] = '''TRUNCATE TABLE dds.industry; INSERT INTO dds.industry
    (id, updated_at, sort, active, industry)
    SELECT li.id
    , NULLIF(li.updated_at,''):: date AS updated_at
    , li.sort
    , CAST(CASE WHEN li.active = 'Да' THEN 'True'
                WHEN li.active = 'Нет' THEN 'False' END AS BOOL)
    ,li.industry
    FROM lgc.industry AS li;'''

    SQL_dict['sql_liee'] = '''TRUNCATE TABLE dds.industry_employee_experience;
    INSERT INTO dds.industry_employee_experience
    (user_id, id, updated_at, sort, experience, active, "date", industry)
    SELECT liee.user_id
    , liee.id
    , NULLIF(liee.updated_at,''):: date AS updated_at
    , liee.sort
    , CAST(CASE WHEN liee.experience = '' THEN '115761'
        ELSE regexp_replace(liee.experience, '[^0-9]', '', 'g') END AS INTEGER)
    , CAST(CASE WHEN liee.active = 'Да' THEN 'True'
                WHEN liee.active = 'Нет' THEN 'False' END AS BOOL)
    , NULLIF(liee."date",''):: date AS "date"
    , CAST(regexp_replace(liee.industry, '[^0-9]', '', 'g') AS INTEGER)
    FROM lgc.industry_employee_experience AS liee;

    -- delete duplicates
    DELETE FROM dds.industry_employee_experience
    WHERE id IN
        (SELECT id FROM dds.industry_employee_experience
        EXCEPT SELECT MIN(id) FROM dds.industry_employee_experience
        GROUP BY user_id, sort, industry, experience);'''

    SQL_dict['sql_lp'] = '''TRUNCATE TABLE dds.platform;
    INSERT INTO dds.platform
    (id, updated_at, sort, active, platform)
    SELECT lp.id
    , NULLIF(lp.updated_at,''):: date AS updated_at
    , lp.sort
    , CAST(CASE WHEN lp.active = 'Да' THEN 'True'
                WHEN lp.active = 'Нет' THEN 'False' END AS BOOL)
    , lp.platform
    FROM lgc.platform AS lp;'''

    SQL_dict['sql_lpeg'] = '''TRUNCATE TABLE dds.platform_and_employee_grade;
    INSERT INTO dds.platform_and_employee_grade
    (user_id, id, updated_at, sort, grade, active, "date", platform)
    SELECT lpeg.user_id
    , lpeg.id
    , NULLIF(lpeg.updated_at,''):: date AS updated_at
    , lpeg.sort
    , CAST(CASE WHEN lpeg.grade = '' THEN '115638'
            ELSE regexp_replace(lpeg.grade, '[^0-9]', '', 'g') END AS INTEGER)
    , CAST(CASE WHEN lpeg.active = 'Да' THEN 'True'
                WHEN lpeg.active = 'Нет' THEN 'False' END AS BOOL)
    , NULLIF(lpeg."date",''):: date AS "date"
    , CAST(regexp_replace(lpeg.platform, '[^0-9]', '', 'g') AS INTEGER)
    FROM lgc.platform_and_employee_grade AS lpeg;
    UPDATE dds.platform_and_employee_grade
    SET sort = CAST(CASE WHEN grade = '115637' THEN '100'
        WHEN grade = '115638' THEN '200'
        WHEN grade = '115639' THEN '300'
        WHEN grade = '115640' THEN '400'
        WHEN grade = '115641' THEN '500'
        WHEN grade = '283045' THEN '600' 
        ELSE NULL END AS INTEGER);

    -- delete duplicates
    DELETE FROM dds.platform_and_employee_grade
    WHERE id IN
        (SELECT id FROM dds.platform_and_employee_grade
        EXCEPT SELECT MIN(id) FROM dds.platform_and_employee_grade
        GROUP BY user_id, sort, grade, platform);

    -- delete wrong platform id
    DELETE FROM dds.platform_and_employee_grade
    WHERE platform NOT IN (SELECT id FROM dds.platform);
    '''

    SQL_dict['sql_program'] = '''TRUNCATE TABLE dds.program;
    INSERT INTO dds.program
    (id, updated_at, sort, active, program)
    SELECT id
    , NULLIF(updated_at,''):: date AS updated_at
    , sort
    , CAST(CASE WHEN active = 'Да' THEN 'True'
                WHEN active = 'Нет' THEN 'False' END AS BOOL)
    , program
    FROM lgc.program;'''

    SQL_dict['sql_program_and_employee_grade'] = '''TRUNCATE TABLE dds.program_and_employee_grade;
    INSERT INTO dds.program_and_employee_grade
    (id, updated_at, sort, grade, active, "date", program, user_id)
    SELECT id
    , NULLIF(updated_at,''):: date AS updated_at
    , sort
    , CAST(CASE WHEN grade = '' THEN '115638'
            ELSE regexp_replace(grade, '[^0-9]', '', 'g') END AS INTEGER)
    , CAST(CASE WHEN active = 'Да' THEN 'True'
                WHEN active = 'Нет' THEN 'False' END AS BOOL)
    , NULLIF("date",''):: date AS "date"			
    , CAST(regexp_replace(program, '[^0-9]', '', 'g') AS INTEGER)
    , CAST (regexp_replace(user_id, '[^0-9]', '', 'g') AS INTEGER)
    FROM lgc.program_and_employee_grade;
    UPDATE dds.program_and_employee_grade
    SET sort = CAST(CASE WHEN grade = '115637' THEN '100'
        WHEN grade = '115638' THEN '200'
        WHEN grade = '115639' THEN '300'
        WHEN grade = '115640' THEN '400'
        WHEN grade = '115641' THEN '500'
        WHEN grade = '283045' THEN '600' 
        ELSE NULL END AS INTEGER);
    -- delete duplicates
    DELETE FROM dds.program_and_employee_grade
    WHERE id IN
        (SELECT id FROM dds.program_and_employee_grade
        EXCEPT SELECT MIN(id) FROM dds.program_and_employee_grade
        GROUP BY user_id, sort, grade, program);
        
    '''

    SQL_dict['sql_resume'] = '''TRUNCATE TABLE dds.resume;
    INSERT INTO dds.resume
    (id, user_id, active, dbms, program, education, industry, platform, domain, 
    certificate, sde, tool, software_type, framework, language, programming_language)
    SELECT id
    , user_id
    , CAST(CASE WHEN active = 'Да' THEN 'True'
                WHEN active = 'Нет' THEN 'False' END AS BOOL)
    , NULLIF(dbms, '')
    , NULLIF(program, '')
    , NULLIF(education, '')
    , NULLIF(industry, '')
    , NULLIF(platform, '')
    , NULLIF(domain, '')
    , NULLIF(certificate, '')
    , NULLIF(sde, '')
    , NULLIF(tool, '')
    , NULLIF(software_type, '')
    , NULLIF(framework, '')
    , NULLIF(language, '')
    , NULLIF(programming_language, '')
    FROM lgc.resume;'''

    SQL_dict['sql_sde'] = '''TRUNCATE TABLE dds.sde;
    INSERT INTO dds.sde (id, updated_at, sort, active, sde)
    SELECT id
    , NULLIF(updated_at,''):: date AS updated_at
    , sort
    , CAST(CASE WHEN active = 'Да' THEN 'True'
                WHEN active = 'Нет' THEN 'False' END AS BOOL)
    , sde
    FROM lgc.sde;'''

    SQL_dict['sql_sde_and_employee_grade'] = '''TRUNCATE TABLE dds.sde_and_employee_grade;
    INSERT INTO dds.sde_and_employee_grade (id, updated_at, sort, sde, grade, active, 
    "date", user_id)
    SELECT id
    , NULLIF(updated_at,''):: date AS updated_at
    , sort
    , CAST(regexp_replace(sde, '[^0-9]', '', 'g') AS INTEGER)
    , CAST(CASE WHEN grade = '' THEN '115638'
            ELSE regexp_replace(grade, '[^0-9]', '', 'g') END AS INTEGER)
    , CAST(CASE WHEN active = 'Да' THEN 'True'
                WHEN active = 'Нет' THEN 'False' END AS BOOL)
    , NULLIF("date",''):: date AS "date"
    , CAST (regexp_replace(user_id, '[^0-9]', '', 'g') AS INTEGER)
    FROM lgc.sde_and_employee_grade;

    -- delete duplicates
    DELETE FROM dds.sde_and_employee_grade
    WHERE id IN
        (SELECT id FROM dds.sde_and_employee_grade
        EXCEPT SELECT MIN(id) FROM dds.sde_and_employee_grade
        GROUP BY user_id, sort, grade, sde);'''



     
    conn_d = create_cfg_variable()
	
    for sql in SQL_dict:
        sql = SQL_dict[sql]
        cur = conn_d.cursor()
        cur.execute(sql)
        conn_d.commit()
        
    return 'Done'


if __name__ == "__main__":
    clean_data()
