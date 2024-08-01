from airflow.providers.postgres.hooks.postgres import PostgresHook
from faker import Faker
import pandas as pd


def transform_employee_table():
    hook = PostgresHook(postgres_conn_id='etl_db_7')
    engine = hook.get_sqlalchemy_engine()
    df = pd.read_sql_table('employee', engine, schema='lgc')

    df['id'] = pd.to_numeric(df['id'], errors='coerce')
    df = df.replace(r'^\s*$', None, regex=True)
    df['department'] = df['department'].str.replace(r'\.+', '', regex=True)
    df['department'] = df['department'].str.strip()
    df = df.drop(['frc', 'city', 'updated_at', 'registered_at', 'login', 'birth_date', 'gender', 'last_check_in', 'company', 'active'], axis=1)

    fake = Faker('ru_RU')
    df['name'] = [fake.first_name() for _ in range(len(df))]
    df['surname'] = [fake.last_name() for _ in range(len(df))]
    df['email'] = [fake.email() for _ in range(len(df))]

    df.to_sql('employee', con=engine, schema='g_dds', index=False, if_exists='replace')


    
    