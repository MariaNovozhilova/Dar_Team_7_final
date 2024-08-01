from sql.g_pre_dm.insertion_employee_table import insertion_employee_table
from sql.g_pre_dm.insertion_esg_and_skills_tables import insertion_esg_and_skills_tables
from sql.g_pre_dm.insertion_grade_table import insertion_grade_table
from sql.g_pre_dm.insertion_employee_year_cer_flag import insertion_employee_year_cer_flag
from sql.g_pre_dm.insertion_employee_cer_table import insertion_employee_cer_table


tables = [
'базы_данных',
'базы_данных_и_уровень_знаний_сотру',
'инструменты',
'инструменты_и_уровень_знаний_сотр',
'образование_пользователей',
'опыт_сотрудника_в_отраслях',
'опыт_сотрудника_в_предметных_обла',
'отрасли',
'платформы',
'платформы_и_уровень_знаний_сотруд',
'предметная_область',
'резюмедар',
'сертификаты_пользователей',
'сотрудники_дар',
'среды_разработки',
'среды_разработки_и_уровень_знаний_',
'технологии',
'технологии_и_уровень_знаний_сотру',
'типы_систем',
'типы_систем_и_уровень_знаний_сотру',
'уровень_образования',
'уровни_владения_ин',
'уровни_знаний',
'уровни_знаний_в_отрасли',
'уровни_знаний_в_предметной_област',
'фреймворки',
'фреймворки_и_уровень_знаний_сотру',
'языки',
'языки_пользователей',
'языки_программирования',
'языки_программирования_и_уровень',
]

dds_table_sql = {
'dbms' : ''' call etl_func.ref_table('dbms', 'lgc', 'g_dds', 'dbms'); ''',
'program' : ''' call etl_func.ref_table('program', 'lgc', 'g_dds', 'program'); ''',
'domain' : ''' call etl_func.ref_table('domain', 'lgc', 'g_dds', 'domain'); ''',
'industry' : ''' call etl_func.ref_table('industry', 'lgc', 'g_dds', 'industry'); ''',
'platform' : ''' call etl_func.ref_table('platform', 'lgc', 'g_dds', 'platform'); ''',
'sde' : ''' call etl_func.ref_table('sde', 'lgc', 'g_dds', 'sde'); ''',
'software_type' : ''' call etl_func.ref_table('software_type', 'lgc', 'g_dds', 'type'); ''',
'foreign_language_level' : ''' call etl_func.ref_table('foreign_language_level', 'lgc', 'g_dds', 'level'); ''',
'grade' : ''' call etl_func.ref_table('grade', 'lgc', 'g_dds', 'grade'); ''',
'framework' : ''' call etl_func.ref_table('framework', 'lgc', 'g_dds', 'framework'); ''',
'language' : ''' call etl_func.ref_table('language', 'lgc', 'g_dds', 'language'); ''',
'programming_language' : ''' call etl_func.ref_table('programming_language', 'lgc', 'g_dds', 'programming_language'); ''',
'education_level' : ''' call etl_func.ref_table('education_level', 'lgc', 'g_dds', 'level'); ''',
'industry_experience' : ''' call etl_func.ref_table_exp('industry_experience', 'lgc', 'g_dds', 'experience'); ''',
'experience' : ''' call etl_func.ref_table_exp('experience', 'lgc', 'g_dds', 'experience'); ''',
'tool' : ''' call etl_func.ref_table_tool('tool', 'lgc', 'g_dds', 'tool'); ''',
'dbms_and_employee_grade' : ''' call etl_func.skill_table('dbms_and_employee_grade', 'lgc', 'g_dds', 'dbms', 'grade'); ''',
'program_and_employee_grade' : ''' call etl_func.skill_table('program_and_employee_grade', 'lgc', 'g_dds', 'program', 'grade'); ''',
'sde_and_employee_grade' : ''' call etl_func.skill_table('sde_and_employee_grade', 'lgc', 'g_dds', 'sde', 'grade'); ''',
'tool_and_employee_grade' : ''' call etl_func.skill_table('tool_and_employee_grade', 'lgc', 'g_dds', 'tool', 'grade'); ''',
'software_type_employee_grade' : ''' call etl_func.skill_table('software_type_employee_grade', 'lgc', 'g_dds', 'software_type', 'grade'); ''',
'framework_and_employee_grade' : ''' call etl_func.skill_table('framework_and_employee_grade', 'lgc', 'g_dds', 'framework', 'grade'); ''',
'programming_language_and_employee_grade' : ''' call etl_func.skill_table('programming_language_and_employee_grade', 'lgc', 'g_dds', 'programming_language', 'grade'); ''',
'industry_employee_experience' : ''' call etl_func.skill_table_id('industry_employee_experience', 'lgc', 'g_dds', 'industry', 'experience'); ''',
'employee_domain_experience' : ''' call etl_func.skill_table_id('employee_domain_experience', 'lgc', 'g_dds', 'domain', 'experience'); ''',
'platform_and_employee_grade' : ''' call etl_func.skill_table_id('platform_and_employee_grade', 'lgc', 'g_dds', 'platform', 'grade'); ''',
'employee_language' : ''' call etl_func.skill_table_em_l('employee_language', 'lgc', 'g_dds', 'language', 'level'); '''
}

pre_dm = {
    'employee': insertion_employee_table,
    'grade': insertion_grade_table,
    'esg_skills': insertion_esg_and_skills_tables,
    'employee_cer_table': insertion_employee_cer_table,
    'employee_year_cer_flag': insertion_employee_year_cer_flag
}