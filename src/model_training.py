import os
import sys

import statsmodels.api as sm
import statsmodels.formula.api as smf
import pandas as pd
import numpy as np
import duckdb
from datetime import date

from . import duck_db_helper


def train_model(data_dir):
    table_name = 'train_feature_1'
    db_path = os.path.join(data_dir, 'feature_generation', 'train.db')
    conn = duckdb.connect(db_path)
    df = conn.sql(f"SELECT * FROM \"{table_name}\";").df()
    conn.close()

    model_formula = "total_points ~ name*(temperature+odds_for+last_3_matches_average)"
    model = smf.ols(model_formula, data=df).fit()

    model_name = 'model1'
    model.save(os.path.join(data_dir, 'models', f'{model_name}.pickle'))
    # create a database if it doesn't exist, that is the model registry.
    # name_of_model | dataset_used | date_trained
    with open(os.path.join(data_dir, 'models','train_registry.csv'), mode='a') as f:
        f.write(f'{str(date.today())},{model_name},{model_formula},{db_path}:{table_name},{model.rsquared}\n')
        print(f'{model_name}: R^2 {model.rsquared}')
