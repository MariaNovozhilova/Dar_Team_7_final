def fullName(schema: str, table: str) -> str:
    if not isinstance(table, str) and isinstance(schema, str):
        print(f'{table} or {schema} is not str')
        pass
    return schema + '.' + table