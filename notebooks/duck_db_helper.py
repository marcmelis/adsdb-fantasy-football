
def get_tables(conn):
    tables_lists = conn.sql("SHOW TABLES").fetchall()
    return [t[0] for t in tables_lists]


def table_exists(table_name, conn):
    return table_name in get_tables(conn)

def get_table_df(table_name, conn):
    return conn.sql(f"SELECT * FROM \"{table_name}\";").df()

def drop_table(table_name, conn):
    if table_exists(table_name, conn):
        conn.sql(f"DROP TABLE \"{table_name}\"")

def create_table(table_name, df, conn, replace=True):
    if replace & table_exists(table_name, conn):
        drop_table(table_name, conn)
    conn.sql(f"CREATE TABLE \"{table_name}\" AS SELECT * FROM df")

def append_table(table_name, df, conn):
    conn.sql(f"INSERT INTO \"{table_name}\" SELECT * FROM df")
