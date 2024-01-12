import os
import sys


import pandas as pd
import duckdb
from sklearn.model_selection import train_test_split

from . import duck_db_helper


def split(data_dir):
    conn = duckdb.connect(os.path.join(data_dir, 'feature_generation', 'feature_generation.db'))
    tables = duck_db_helper.get_tables(conn)
    df_list = []
    df_list_names = []
    for table_name in tables:
        df = conn.sql(f"SELECT * FROM \"{table_name}\";").df()
        df_list.append(df)
        df_list_names.append(table_name)
    conn.close()
    df = df_list[0]

    X = df.drop('total_points', axis=1)  # Features (excluding the target variable)
    y = df['total_points']  # Target variable

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=df['name'], random_state=42)
    # read the target to the data
    X_train['total_points'] = y_train
    X_test['total_points'] = y_test


    conn = duckdb.connect(os.path.join(data_dir, 'feature_generation', 'train.db'))
    duck_db_helper.create_table('train_feature_1', X_train, conn)
    conn.close()
    conn = duckdb.connect(os.path.join(data_dir, 'feature_generation', 'test.db'))
    duck_db_helper.create_table('test_feature_1', X_test, conn)
    conn.close()
