#!/usr/bin/env python
# coding: utf-8

# In[1]:


# packages
import pandas as pd
import numpy as np
import duckdb 
import math
from datetime import timedelta


# In[2]:


formatted_zone_db = '../data/formatted_zone/formatted_zone.db'
trusted_zone_db = '../data/trusted_zone/trusted_zone.db'


# In[3]:


def get_tables(conn):
    tables_lists = conn.sql("SHOW TABLES").fetchall()
    return [t[0] for t in tables_lists]

def is_numeric(column): 
    try: 
        pd.to_numeric(column, errors='raise')
        return True
    except: 
        return False
    
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


# In[4]:


# Gets the file with the last date, 
# ex: for filename=cleaned_merged_seasons would return cleaned_merged_seasons_2023-09-22.csv
from datetime import datetime
def get_last_table(table_names, fileformat="csv"):
    format_str = "%Y-%m-%d"  # Date format
    most_recent_file = max(
        table_names, 
        key=lambda f: datetime.strptime(f[-len("yyyy-MM-dd"):], format_str)
    )
    return most_recent_file


# In[5]:


# get all the tables in the formatted zone
conn = duckdb.connect(formatted_zone_db)
formatted_zone_tables = get_tables(conn)
formatted_zone_tables
conn.close()


# In[6]:


trusted_zone_table = 'team_stadium_location'
team_stadium_location_tables = filter(lambda x: x.startswith(trusted_zone_table), formatted_zone_tables)
# the data from all seasons is all in one table so we only need to find the latest version of the table
latest_table_name = get_last_table(team_stadium_location_tables)
conn = duckdb.connect(formatted_zone_db)
df = get_table_df(latest_table_name, conn)
conn.close()
# do some data quality checks
assert df.isna().sum().sum() == 0
assert isinstance(df.LAT.dtype, np.dtypes.Float64DType)
assert isinstance(df.LON.dtype, np.dtypes.Float64DType)


conn = duckdb.connect(trusted_zone_db)
create_table(trusted_zone_table, df, conn)
conn.close()


# In[7]:


trusted_zone_table = 'cleaned_merged_seasons'
cleaned_merged_seasons_tables = filter(lambda x: x.startswith(trusted_zone_table), formatted_zone_tables)
# the data from all seasons is all in one table so we only need to find the latest version of the table
latest_table_name = get_last_table(cleaned_merged_seasons_tables)
conn = duckdb.connect(formatted_zone_db)
df = get_table_df(latest_table_name, conn)
conn.close()
# do some data quality checks
conn = duckdb.connect(trusted_zone_db)
stadium_df = get_table_df("team_stadium_location", conn)
conn.close()

temp_set = set(df.team_x) - set(stadium_df.team_name)
temp_set.discard(None)
assert len(temp_set) == 0

assert not is_numeric(df.season_x)
assert not is_numeric(df.name)
assert not is_numeric(df.position)
assert not is_numeric(df.team_x)
assert not is_numeric(df.opp_team_name)

assert is_numeric(df.assists)
assert is_numeric(df.bonus)
assert is_numeric(df.bps)
assert is_numeric(df.clean_sheets)
assert is_numeric(df.creativity)
assert is_numeric(df.element)
assert is_numeric(df.fixture)
assert is_numeric(df.goals_conceded)
assert is_numeric(df.goals_scored)
assert is_numeric(df.ict_index)
assert is_numeric(df.influence)
assert is_numeric(df.kickoff_time)
assert is_numeric(df.minutes)
assert is_numeric(df.opponent_team)
assert is_numeric(df.own_goals)
assert is_numeric(df.penalties_missed)
assert is_numeric(df.penalties_saved)
assert is_numeric(df.red_cards)
assert is_numeric(df['round'])
assert is_numeric(df.saves)
assert is_numeric(df.selected)
assert is_numeric(df.team_a_score)
assert is_numeric(df.team_h_score)
assert is_numeric(df.threat)
assert is_numeric(df.total_points)
assert is_numeric(df.transfers_balance)
assert is_numeric(df.transfers_in)
assert is_numeric(df.transfers_out)
assert is_numeric(df.value)
assert is_numeric(df.was_home)
assert is_numeric(df.yellow_cards)
assert is_numeric(df.GW)

conn = duckdb.connect(trusted_zone_db)
create_table(trusted_zone_table, df, conn)
conn.close()


# In[8]:


trusted_zone_table = 'weather_station_locations'
weather_station_locations_tables = filter(lambda x: x.startswith(trusted_zone_table), formatted_zone_tables)
# the data from all seasons is all in one table so we only need to find the latest version of the table
latest_table_name = get_last_table(weather_station_locations_tables)
conn = duckdb.connect(formatted_zone_db)
df = get_table_df(latest_table_name, conn)
conn.close()
# do some data quality checks
assert df.isna().sum().sum() == 0
assert is_numeric(df.LAT)
assert is_numeric(df.LON)

conn = duckdb.connect(trusted_zone_db)
create_table(trusted_zone_table, df, conn)
conn.close()


# In[9]:


def euclidean_distance(lat1, lon1, lat2, lon2):
    # Assuming the Britain can be approximated as a flat plane, calculate the distance using Pythagoras' theorem
    lat_diff = lat2 - lat1
    lon_diff = lon2 - lon1
    distance = math.sqrt(lat_diff**2 + lon_diff**2)

    return distance

def wdir_to_deg(wdir): 
    wdir_dict = {
        "N": 0,
        "E": 90,
        "S": 180,
        "W": 270
    }

    for i, c in enumerate(wdir): 
        if i == 0: deg = wdir_dict[wdir[len(wdir) - 1 - i]]
        else: 
            deg = (deg + wdir_dict[wdir[len(wdir) - 1 - i]]) / 2

    return deg

def deg_to_wdir(deg): 
    wind_directions = [
        ('N', (345, 15)),
        ('NNE', (15, 30)),
        ('NE', (30, 60)),
        ('ENE', (60, 75)),
        ('E', (75, 105)),
        ('ESE', (105, 120)),
        ('SE', (120, 150)),
        ('SSE', (150, 165)),
        ('S', (165,195)),
        ('SSW', (195,210)),
        ('SW', (210,240)),
        ('WSW', (240,255)),
        ('W', (255,285)),
        ('WNW', (285,300)),
        ('NW', (300,330)),
        ('NNW', (330,345))
    ]
    
    # Loop through the wind direction abbreviations and degree ranges
    for direction, (lower, upper) in wind_directions:
        if lower <= deg < upper:
            return direction

    # If the input degrees are outside the defined ranges, return 'N' (North)
    return 'N'


# In[25]:


trusted_zone_table = 'Metoffice'
metoffice_tables = list(filter(lambda x: x.startswith(trusted_zone_table), formatted_zone_tables))
# group all tables for the same month
unique_met_months = list(set([t[:-len("_yyyy-MM-dd")] for t in metoffice_tables]))


# trusted_conn = duckdb.connect(trusted_zone_db)
# drop_table(trusted_zone_table, trusted_conn)
df = None

for month in unique_met_months: 
    month_tables = filter(lambda x: x.startswith(month), metoffice_tables)

    # pick the newest table for the season
    latest_table_name = get_last_table(month_tables)
    
    formated_conn = duckdb.connect(formatted_zone_db)
    df = pd.concat([df, get_table_df(latest_table_name,formated_conn)])
    formated_conn.close()


# impute missing values

conn = duckdb.connect(trusted_zone_db)
loc_df = get_table_df('weather_station_locations', conn)
conn.close()

df = df.reset_index(drop=True)
imputed_df = df.__deepcopy__()

for column in df.columns:
    # iterate through the dataframe
    for i, value in enumerate(df[column]):
        if pd.isna(value):
            station = df.loc[i, "Station_name"]
            date = df.iloc[i].Date

            # check if this station should be interpolated for this column
            start_date = date - timedelta(days = 1)
            end_date = date + timedelta(days = 1)
            
            prev_measure = np.nan
            post_measure = np.nan
            t = df.loc[(df.Station_name == station) & (df.Date == start_date)]
            if len(t) > 0: prev_measure = t[column].iloc[0]

            t = df.loc[(df.Station_name == station) & (df.Date == end_date)]
            if len(t) > 0: post_measure = t[column].iloc[0]

            if not np.isnan(prev_measure) and isinstance(prev_measure, (int, float)) and not np.isnan(post_measure) and isinstance(post_measure, (int, float)): 
                interpolated_value = np.mean([prev_measure, post_measure])
            else: 
                # mean of nearby stations
                lat = loc_df.loc[loc_df.SITE == station].LAT
                lon = loc_df.loc[loc_df.SITE == station].LON
                loc_df['Distance'] = loc_df.apply(lambda row: euclidean_distance(lat, lon, row['LAT'], row['LON']), axis=1)
                # Sort the DataFrame by distance
                loc_df = loc_df.sort_values(by='Distance')

                # Select the top three closest stations
                closest_stations = list(loc_df.head(4).SITE)
                closest_df = df.loc[(df.Date == date) & df.Station_name.apply(lambda x: x in closest_stations)]

                if column == 'WDIR': 
                    interpolated_value = deg_to_wdir(closest_df.loc[:,column].apply(wdir_to_deg).mean())
                else: 
                    interpolated_value = closest_df.loc[:,column].mean()
            
            # print(i, interpolated_value, station, column, date)
            imputed_df.loc[i, column] = interpolated_value

assert not is_numeric(imputed_df.Station_name)
assert not is_numeric(imputed_df.WDIR)

assert is_numeric(imputed_df.Station_no)
assert is_numeric(imputed_df.PRESS)
assert is_numeric(imputed_df.WSPD)
assert is_numeric(imputed_df.CLOUD)
assert is_numeric(imputed_df.TEMP)
assert is_numeric(imputed_df.TDEW)

conn = duckdb.connect(trusted_zone_db)
create_table(trusted_zone_table, imputed_df, conn)
conn.close()


# In[52]:


trusted_zone_table = 'football-data'
football_data_tables = list(filter(lambda x: x.startswith(trusted_zone_table), formatted_zone_tables))
# group all tables for the same season 
unique_seasons = list(set([t[:-len("_yyyy-MM-dd")] for t in football_data_tables]))


df = None
for season in unique_seasons: 
    season_tables = filter(lambda x: x.startswith(season), football_data_tables)
    # pick the newest table for the season
    latest_table_name = get_last_table(season_tables)
    
    # retrieve the formatted zone df
    formatted_conn = duckdb.connect(formatted_zone_db)
    df = pd.concat([df, get_table_df(latest_table_name, formatted_conn)])
    formatted_conn.close()

df = df.reset_index(drop=True)
imputed_df = df.__deepcopy__()


assert (df.FTR != df.apply(lambda row: "D" if row["FTHG"] == row["FTAG"] else "H" if row["FTHG"] > row["FTAG"] else "A", axis=1)).sum() == 0
assert (df.HTR != df.apply(lambda row: "D" if row["HTHG"] == row["HTAG"] else "H" if row["HTHG"] > row["HTAG"] else "A", axis=1)).sum() == 0
assert (df.HS < df.HST).sum() == 0
assert (df.AS < df.AST).sum() == 0

for column in df.columns[24:]:
    backup_column = None
    original_columns = None

    if   column in ["B365H","BWH","IWH","PSH","WHH","VCH"]: 
        backup_column = "AvgH" 
        original_columns = ["B365H","BWH","IWH","PSH","WHH","VCH"]
    elif column in ["B365D","BWD","IWD","PSD","WHD","VCD"]: 
        backup_column = "AvgD" 
        original_columns = ["B365D","BWD","IWD","PSD","WHD","VCD"]
    elif column in ["B365A","BWA","IWA","PSA","WHA","VCA"]: 
        backup_column = "AvgA" 
        original_columns = ["B365A","BWA","IWA","PSA","WHA","VCA"]
    elif column in ["B365>2.5","P>2.5"]: 
        backup_column = "Avg>2.5" 
        original_columns = ["B365>2.5","P>2.5"]
    elif column in ["B365<2.5","P<2.5"]: 
        backup_column = "Avg<2.5" 
        original_columns = ["B365<2.5","P<2.5"]
    elif column in ["AHh","B365AHH","PAHH"]: 
        backup_column = "AvgAHH" 
        original_columns = ["AHh","B365AHH","PAHH"]
    elif column in ["B365AHA","PAHA"]: 
        backup_column = "AvgAHA" 
        original_columns = ["B365AHA","PAHA"]
    elif column in ["B365CH","BWCH","IWCH","PSCH","WHCH","VCCH"]: 
        backup_column = "AvgCH" 
        original_columns = ["B365CH","BWCH","IWCH","PSCH","WHCH","VCCH"]
    elif column in ["B365CD","BWCD","IWCD","PSCD","WHCD","VCCD"]: 
        backup_column = "AvgCD" 
        original_columns = ["B365CD","BWCD","IWCD","PSCD","WHCD","VCCD"]
    elif column in ["B365CA","BWCA","IWCA","PSCA","WHCA","VCCA"]: 
        backup_column = "AvgCA" 
        original_columns = ["B365CA","BWCA","IWCA","PSCA","WHCA","VCCA"]
    elif column in ["B365C>2.5","PC>2.5"]: 
        backup_column = "AvgC>2.5" 
        original_columns = ["B365C>2.5","PC>2.5"]
    elif column in ["B365C<2.5","PC<2.5"]: 
        backup_column = "AvgC<2.5" 
        original_columns = ["B365C<2.5","PC<2.5"]
    elif column in ["AHCh","B365CAHH","PCAHH"]: 
        backup_column = "AvgCAHH" 
        original_columns = ["AHCh","B365CAHH","PCAHH"]
    elif column in ["B365CAHA","PCAHA"]: 
        backup_column = "AvgCAHA" 
        original_columns = ["B365CAHA","PCAHA"]

    
    if   column in ["AvgH", "MaxH"]:        original_columns = ["B365H","BWH","IWH","PSH","WHH","VCH"]
    elif column in ["AvgD", "MaxD"]:        original_columns = ["B365D","BWD","IWD","PSD","WHD","VCD"]
    elif column in ["AvgA", "MaxA"]:        original_columns = ["B365A","BWA","IWA","PSA","WHA","VCA"]
    elif column in ["Avg>2.5", "Max>2.5"]:  original_columns = ["B365>2.5","P>2.5"]
    elif column in ["Avg<2.5", "Max<2.5"]:  original_columns = ["B365<2.5","P<2.5"]
    elif column in ["AvgAHH", "MaxAHH"]:    original_columns = ["AHh","B365AHH","PAHH"]
    elif column in ["AvgAHA", "MaxAHA"]:    original_columns = ["B365AHA","PAHA"]
    elif column in ["AvgCH", "MaxCH"]:      original_columns = ["B365CH","BWCH","IWCH","PSCH","WHCH","VCCH"]
    elif column in ["AvgCD", "MaxCD"]:      original_columns = ["B365CD","BWCD","IWCD","PSCD","WHCD","VCCD"]
    elif column in ["AvgCA", "MaxCA"]:      original_columns = ["B365CA","BWCA","IWCA","PSCA","WHCA","VCCA"]
    elif column in ["AvgC>2.5", "MaxC>2.5"]:original_columns = ["B365C>2.5","PC>2.5"]
    elif column in ["AvgC<2.5", "MaxC<2.5"]:original_columns = ["B365C<2.5","PC<2.5"]
    elif column in ["AvgCAHH", "MaxCAHH"]:  original_columns = ["AHCh","B365CAHH","PCAHH"]
    elif column in ["AvgCAHA", "MaxCAHA"]:  original_columns = ["B365CAHA","PCAHA"]

    for i, value in enumerate(df[column]):
        if pd.isna(value): 
            if backup_column is None: # Avg or Max column
                if column.startswith("Avg"): 
                    imputed_value = df.loc[i, original_columns].mean()
                elif column.startswith("Max"): 
                    imputed_value = df.loc[i, original_columns].max()
            else: 
                if pd.isna(df.loc[i, backup_column]): 
                    imputed_value = df.loc[i, original_columns].mean()
                else: 
                    imputed_value = df.loc[i, backup_column]
            imputed_df.loc[i, column] = imputed_value


trusted_conn = duckdb.connect(trusted_zone_db)
create_table(trusted_zone_table, imputed_df, trusted_conn)
trusted_conn.close()


# In[ ]:




