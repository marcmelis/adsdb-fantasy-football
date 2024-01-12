import os
import sys

import statsmodels.api as sm
import statsmodels.formula.api as smf
import pandas as pd
import numpy as np
import duckdb
from datetime import date
from statsmodels.iolib.smpickle import load_pickle

from . import duck_db_helper




def validate_model(data_dir):
    model_name = 'model1'
    new_results = load_pickle(os.path.join(data_dir, 'models',f'{model_name}.pickle'))

    table_name = 'test_feature_1'
    db_path = os.path.join(data_dir, 'feature_generation','test.db')
    conn = duckdb.connect(db_path)
    test_df = conn.sql(f"SELECT * FROM \"{table_name}\";").df()
    conn.close()


    test_df['total_pred'] = new_results.predict(test_df)

    rss = np.power(test_df['total_points']-test_df['total_pred'], 2).sum()
    tss = np.power(test_df['total_points']-test_df['total_points'].mean(), 2).sum()
    rsquared = 1 - rss/tss
    mse = rss / (len(test_df) - len(new_results.params))

    with open(os.path.join(data_dir, 'models','test_registry.csv'), mode='a') as f:
        f.write(f'{str(date.today())},{model_name},{db_path}:{table_name},{rsquared}\n')
        print(f'{model_name}: MSE {mse}')
