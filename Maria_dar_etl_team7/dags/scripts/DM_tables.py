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


SQL_DM_dict = {'personal_data':'''
TRUNCATE TABLE dm.personal_data; INSERT INTO dm.personal_data (id, name, surname, email, frc, position, ranking)
SELECT emp.id, emp.name, emp.surname, emp.email, emp.frc, emp."position", tc.ranking
FROM dds.employee emp
LEFT JOIN (
		SELECT e.id
		, COALESCE(tc.total_certificates, 0) AS total_certificates
		, DENSE_RANK() OVER (ORDER BY COALESCE(tc.total_certificates, 0) DESC) AS ranking
		FROM dds.employee e
		LEFT JOIN (SELECT ec.user_id
		, COUNT(*) AS total_certificates
		FROM dds.employee_certificate ec
		GROUP BY ec.user_id) tc ON e.id = tc.user_id ) tc ON tc.id = emp.id
'''}

SQL_DM_dict['employee_skill'] = ''' TRUNCATE TABLE dm.employee_skill; INSERT INTO dm.employee_skill (employee_id,
skill_type, skill_name, grade_name, sort)
 SELECT e.id AS employee_id,
    'dbms'::text AS skill_type,
    d.dbms AS skill_name,
    g.grade as grade_name,
    deg.sort
   FROM dds.employee e
    -- только максимальные грейды  по каждой базе данных--
	 LEFT JOIN (SELECT * FROM dds.dbms_and_employee_grade
				WHERE dbms IS NOT NULL AND id IN
				(SELECT MAX(id) FROM dds.dbms_and_employee_grade
				GROUP BY user_id, dbms)) deg ON e.id = deg.user_id
     LEFT JOIN dds.dbms d ON d.id = deg.dbms
     LEFT JOIN dds.grade g ON g.id = deg.grade

UNION
 
 SELECT e.id AS employee_id,
    'program'::text AS skill_type,
    p.program AS skill_name,
    g.grade,
    peg.sort
   FROM dds.employee e
    -- только максимальные грейды  по каждой базе программе--
	 LEFT JOIN (SELECT * FROM dds.program_and_employee_grade
				WHERE program IS NOT NULL AND id IN
				(SELECT MAX(id) FROM dds.program_and_employee_grade
				GROUP BY user_id, program)) peg ON e.id = peg.user_id
     LEFT JOIN dds.program p ON p.id = peg.program
     LEFT JOIN dds.grade g ON g.id = peg.grade

UNION

SELECT e.id AS employee_id,
    'platform'::text AS skill_type,
    pl.platform AS skill_name,
    g.grade,
    pleg.sort
   FROM dds.employee e
     -- только максимальные грейды  по каждой платформе--
	 LEFT JOIN (SELECT * FROM dds.platform_and_employee_grade
				WHERE platform IS NOT NULL AND id IN
				(SELECT MAX(id) FROM dds.platform_and_employee_grade
				GROUP BY user_id, platform)) pleg ON e.id = pleg.user_id
     LEFT JOIN dds.platform pl ON pl.id = pleg.platform
     LEFT JOIN dds.grade g ON g.id = pleg.grade

UNION

SELECT e.id AS employee_id,
    'tool'::text AS skill_type,
    t.tool AS skill_name,
    g.grade,
    g.sort
   FROM dds.employee e
     -- только максимальные грейды  по каждой платформе--
	 LEFT JOIN (SELECT * FROM dds.tool_and_employee_grade
				WHERE tool IS NOT NULL AND id IN
				(SELECT MAX(id) FROM dds.tool_and_employee_grade
				GROUP BY user_id, tool)) teg ON e.id = teg.user_id
     LEFT JOIN dds.tool t ON t.id = teg.tool
     LEFT JOIN dds.grade g ON g.id = teg.grade

UNION

SELECT e.id AS employee_id,
    'framework'::text AS skill_type,
    f.framework AS skill_name,
    g.grade,
    g.sort
   FROM dds.employee e
    -- только максимальные грейды  по каждой платформе--
	 LEFT JOIN (SELECT * FROM dds.framework_and_employee_grade
				WHERE framework IS NOT NULL AND id IN
				(SELECT MAX(id) FROM dds.framework_and_employee_grade
				GROUP BY user_id, framework)) feg ON e.id = feg.user_id
     LEFT JOIN dds.framework f ON f.id = feg.framework
     LEFT JOIN dds.grade g ON g.id = feg.grade

UNION
 
 SELECT e.id AS employee_id,
    'sde'::text AS skill_type,
    s.sde AS skill_name,
    g.grade,
    seg.sort
   FROM dds.employee e
       -- только максимальные грейды  по каждой sde--
	 LEFT JOIN (SELECT * FROM dds.sde_and_employee_grade
				WHERE sde IS NOT NULL AND id IN
				(SELECT MAX(id) FROM dds.sde_and_employee_grade
				GROUP BY user_id, sde)) seg ON e.id = seg.user_id
     LEFT JOIN dds.sde s ON s.id = seg.sde
     LEFT JOIN dds.grade g ON g.id = seg.grade; '''

def dm_data():
    import psycopg
    import json
    conn_d = create_cfg_variable()
    # в цикле запускаю выполнение всех запросов и заполняю DM слой
    for sql in SQL_DM_dict:
        sql = SQL_DM_dict[sql]
        cur = conn_d.cursor()
        cur.execute(sql)
        conn_d.commit()
    return 'Done'


if __name__ == "__main__":
    dm_data()