import psycopg
import json

f = open('etl_base.json')
dsn_source = json.load(f)

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

conn = create_connection(dsn_source["db_name"], dsn_source["db_user"], dsn_source["db_password"], dsn_source["db_host"], dsn_source["db_port"])

tables = ['dbms', 'dbms_and_employee_grade', 'domain', 'employee_education_level', 'employee', 'employee_certificate', 
          'employee_domain_experience', 'industry', 'industry_employee_experience', 'platform', 
          'platform_and_employee_grade', 'program', 'program_and_employee_grade', 'resume', 
          'sde', 'sde_and_employee_grade']


#в каждую таблицу добавить столбец "причина добавления" и заполнять его для понимания, что плохо в данных

bad_dict = {'bad_data.employee_certificate':'''CREATE TABLE IF NOT EXISTS bad_data.employee_certificate
(LIKE lgc.employee_certificate); ALTER TABLE bad_data.employee_certificate ADD reason text; TRUNCATE TABLE bad_data.employee_certificate;
INSERT INTO bad_data.employee_certificate (user_id, id, year, updated_at, title, organisation, sort, active, reason)
SELECT user_id
, id
, year
, updated_at
, title
, organisation
, sort
, active
, 'wrong title' AS reason
FROM lgc.employee_certificate
WHERE title LIKE '%Наименование%';'''}

bad_dict['bad_data.dbms'] = '''CREATE TABLE IF NOT EXISTS bad_data.dbms
(LIKE lgc.dbms); TRUNCATE TABLE bad_data.dbms;
INSERT INTO bad_data.dbms (id, updated_at, sort, active, dbms)
SELECT id, updated_at, sort, active, dbms
FROM lgc.dbms
WHERE updated_at::date > NOW();'''

bad_dict['bad_data.dbms_and_employee_grade'] ='''CREATE TABLE IF NOT EXISTS bad_data.dbms_and_employee_grade
(LIKE lgc.dbms_and_employee_grade); TRUNCATE TABLE bad_data.dbms_and_employee_grade;
INSERT INTO bad_data.dbms_and_employee_grade
(id, user_id, updated_at, sort, grade, active, "date", dbms)
SELECT * FROM lgc.dbms_and_employee_grade
WHERE id IN
	(SELECT id FROM lgc.dbms_and_employee_grade
	EXCEPT SELECT MIN(id) FROM lgc.dbms_and_employee_grade
	GROUP BY user_id, sort, grade, dbms)
ORDER BY user_id, sort, grade, dbms; '''

bad_dict['bad_data.employee_education_level'] = '''CREATE TABLE IF NOT EXISTS bad_data.employee_education_level
(LIKE lgc.employee_education_level);  ALTER TABLE bad_data.employee_education_level ADD reason text;
TRUNCATE TABLE bad_data.employee_education_level;
INSERT INTO bad_data.employee_education_level (user_id, id, year_graduated, updated_at, institution_name, sort, level
, faculty_department, short_name, active, qualification, specialty, reason)
SELECT user_id, id, year_graduated, updated_at, institution_name, sort, level
, faculty_department, short_name, active, qualification, specialty, 'year is too big' AS reason
FROM lgc.employee_education_level
WHERE year_graduated > 2030;

INSERT INTO bad_data.employee_education_level(user_id, id, year_graduated, updated_at, institution_name, sort, level
, faculty_department, short_name, active, qualification, specialty, reason)
SELECT *, 'duplicate' as reason FROM lgc.employee_education_level
WHERE id IN
	(SELECT id FROM lgc.employee_education_level
	EXCEPT SELECT MIN(id) FROM lgc.employee_education_level
	GROUP BY user_id, sort, level, year_graduated, faculty_department, specialty)
ORDER BY user_id, sort, level, year_graduated, faculty_department, specialty;
'''

bad_dict['bad_data.industry_employee_experience'] = '''CREATE TABLE IF NOT EXISTS bad_data.industry_employee_experience (LIKE lgc.industry_employee_experience);
ALTER TABLE bad_data.industry_employee_experience ADD reason text;
TRUNCATE TABLE bad_data.industry_employee_experience;
INSERT INTO bad_data.industry_employee_experience
SELECT *, 'duplicate' AS reason FROM lgc.industry_employee_experience
WHERE id IN
	(SELECT id FROM lgc.industry_employee_experience
	EXCEPT SELECT MIN(id) FROM lgc.industry_employee_experience
	GROUP BY user_id, sort, industry, experience)
ORDER BY user_id, sort, industry, experience;
'''

bad_dict['bad_data.employee_domain_experience'] = '''
CREATE TABLE IF NOT EXISTS bad_data.employee_domain_experience (LIKE lgc.employee_domain_experience);
ALTER TABLE bad_data.employee_domain_experience ADD reason text;
TRUNCATE TABLE bad_data.employee_domain_experience;
INSERT INTO bad_data.employee_domain_experience
SELECT *, 'duplicate' AS reason FROM lgc.employee_domain_experience
WHERE id IN
	(SELECT id FROM lgc.employee_domain_experience
	EXCEPT SELECT MIN(id) FROM lgc.employee_domain_experience
	GROUP BY user_id, sort, domain, experience)
ORDER BY user_id, sort, domain, experience;
'''

bad_dict['bad_data.platform_and_employee_grade'] = '''
CREATE TABLE IF NOT EXISTS bad_data.platform_and_employee_grade (LIKE lgc.platform_and_employee_grade);
ALTER TABLE bad_data.employee_domain_experience ADD reason text;
TRUNCATE TABLE bad_data.platform_and_employee_grade;
INSERT INTO bad_data.platform_and_employee_grade(user_id, id, updated_at, sort, grade, active, "date", platform, reason)
SELECT *, 'duplicate' AS reason FROM lgc.platform_and_employee_grade
WHERE id IN
	(SELECT id FROM lgc.platform_and_employee_grade
	EXCEPT SELECT MIN(id) FROM lgc.platform_and_employee_grade
	GROUP BY user_id, sort, grade, platform)
ORDER BY user_id, sort, grade, platform;

SELECT *, 'wrong platform id' 
FROM dds.platform_and_employee_grade
WHERE platform NOT IN (SELECT id FROM dds.platform)
'''

bad_dict['bad_data.program_and_employee_grade'] = '''
CREATE TABLE IF NOT EXISTS bad_data.program_and_employee_grade (LIKE lgc.program_and_employee_grade);
TRUNCATE TABLE bad_data.program_and_employee_grade;
INSERT INTO bad_data.program_and_employee_grade(user_id, id, updated_at, sort, grade, active, "date", program)
SELECT user_id, id, updated_at, sort, grade, active, "date", program FROM lgc.program_and_employee_grade
WHERE id::integer IN
	(SELECT id::integer FROM lgc.program_and_employee_grade
	EXCEPT SELECT MIN(id::integer) FROM lgc.program_and_employee_grade
	GROUP BY user_id, sort, grade, program)
ORDER BY user_id, sort, grade, program;
'''


bad_dict['bad_data.sde_and_employee_grade'] = '''CREATE TABLE IF NOT EXISTS bad_data.sde_and_employee_grade (LIKE lgc.sde_and_employee_grade);
TRUNCATE TABLE bad_data.sde_and_employee_grade;
INSERT INTO bad_data.sde_and_employee_grade(id, updated_at, sort, sde, grade, active, 
"date", user_id)
SELECT * FROM lgc.sde_and_employee_grade
WHERE id IN
	(SELECT id FROM lgc.sde_and_employee_grade
	EXCEPT SELECT MIN(id) FROM lgc.sde_and_employee_grade
	GROUP BY user_id, sort, grade, sde)
ORDER BY user_id, sort, grade, sde;'''




# в цикле запускаю выполнение всех запросов и заполняю bad_data слой
for sql in bad_dict:
    sql = bad_dict[sql]
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()