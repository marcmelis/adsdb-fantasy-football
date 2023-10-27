# packages
import pandas as pd
import numpy as np
import duckdb
import math
import os
from datetime import timedelta
from datetime import datetime
from . import duck_db_helper

def __is_numeric__(column):
    try:
        pd.to_numeric(column, errors='raise')
        return True
    except:
        return False

def __get_last_table__(table_names, fileformat="csv"):
    format_str = "%Y-%m-%d"  # Date format
    most_recent_file = max(
        table_names,
        key=lambda f: datetime.strptime(f[-len("yyyy-MM-dd"):], format_str)
    )
    return most_recent_file

def __distance__(lat1, lon1, lat2, lon2):
    # Assuming the Britain can be approximated as a flat plane, calculate the distance using Pythagoras' theorem
    lat_diff = lat2 - lat1
    lon_diff = lon2 - lon1
    d = math.sqrt(lat_diff**2 + lon_diff**2)

    return d

def __wdir_to_deg__(wdir):
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

def __deg_to_wdir__(deg):
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

def move_to_trusted(data_dir):

    formatted_zone_db = os.path.join(data_dir, 'formatted_zone', 'formatted_zone.db')
    trusted_zone_db = os.path.join(data_dir, 'trusted_zone','trusted_zone.db')

    conn = duckdb.connect(formatted_zone_db)
    formatted_zone_tables = duck_db_helper.get_tables(conn)
    conn.close()

    trusted_zone_table = 'team_stadium_location'
    team_stadium_location_tables = filter(lambda x: x.startswith(trusted_zone_table), formatted_zone_tables)
    # the data from all seasons is all in one table so we only need to find the latest version of the table
    latest_table_name = __get_last_table__(team_stadium_location_tables)
    conn = duckdb.connect(formatted_zone_db)
    df = duck_db_helper.get_table_df(latest_table_name, conn)
    conn.close()
    # do some data quality checks
    assert df.isna().sum().sum() == 0, "Missing values in stadium location tables."
    assert df.LAT.dtype == np.float64, "Latitude column in stadium location tables is not numeric"
    assert df.LON.dtype == np.float64, "Longitude column in stadium location tables is not numeric"


    conn = duckdb.connect(trusted_zone_db)
    duck_db_helper.create_table(trusted_zone_table, df, conn)
    conn.close()


    trusted_zone_table = 'cleaned_merged_seasons'
    cleaned_merged_seasons_tables = filter(lambda x: x.startswith(trusted_zone_table), formatted_zone_tables)
    # the data from all seasons is all in one table so we only need to find the latest version of the table
    latest_table_name = __get_last_table__(cleaned_merged_seasons_tables)
    conn = duckdb.connect(formatted_zone_db)
    df = duck_db_helper.get_table_df(latest_table_name, conn)
    conn.close()
    # do some data quality checks
    conn = duckdb.connect(trusted_zone_db)
    stadium_df = duck_db_helper.get_table_df("team_stadium_location", conn)
    conn.close()

    temp_set = set(df.team_x) - set(stadium_df.team_name)
    temp_set.discard(None)
    assert len(temp_set) == 0, "Teams in cleaned_merged_seasons are not present in stadium location tables"

    assert not __is_numeric__(df.season_x), "Column season_x should not be numeric in cleaned_merged_seasons"
    assert not __is_numeric__(df.name), "Column name should not be numeric in cleaned_merged_seasons"
    assert not __is_numeric__(df.position), "Column position should not be numeric in cleaned_merged_seasons"
    assert not __is_numeric__(df.team_x), "Column team_x should not be numeric in cleaned_merged_seasons"
    assert not __is_numeric__(df.opp_team_name), "Column opp_team_name should not be numeric in cleaned_merged_seasons"

    assert __is_numeric__(df.assists),          "Column assists should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.bonus),            "Column bonus should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.bps),              "Column bps should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.clean_sheets),     "Column clean_sheets should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.creativity),       "Column creativity should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.element),          "Column element should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.fixture),          "Column fixture should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.goals_conceded),   "Column goals_conceded should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.goals_scored),     "Column goals_scored should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.ict_index),        "Column ict_index should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.influence),        "Column influence should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.kickoff_time),     "Column kickoff_time should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.minutes),          "Column minutes should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.opponent_team),    "Column opponent_team should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.own_goals),        "Column own_goals should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.penalties_missed), "Column penalties_missed should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.penalties_saved),  "Column penalties_saved should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.red_cards),        "Column red_cards should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df['round']),         "Column round should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.saves),            "Column saves should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.selected),         "Column selected should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.team_a_score),     "Column team_a_score should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.team_h_score),     "Column team_h_score should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.threat),           "Column threat should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.transfers_balance),"Column transfers_balance should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.transfers_in),     "Column transfers_in should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.transfers_out),    "Column transfers_out should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.value),            "Column value should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.was_home),         "Column was_home should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.yellow_cards),     "Column yellow_cards should be numeric in cleaned_merged_seasons"
    assert __is_numeric__(df.GW),               "Column GW should be numeric in cleaned_merged_seasons"

    conn = duckdb.connect(trusted_zone_db)
    duck_db_helper.create_table(trusted_zone_table, df, conn)
    conn.close()

    trusted_zone_table = 'weather_station_locations'
    weather_station_locations_tables = filter(lambda x: x.startswith(trusted_zone_table), formatted_zone_tables)
    # the data from all seasons is all in one table so we only need to find the latest version of the table
    latest_table_name = __get_last_table__(weather_station_locations_tables)
    conn = duckdb.connect(formatted_zone_db)
    df = duck_db_helper.get_table_df(latest_table_name, conn)
    conn.close()
    # do some data quality checks
    assert df.isna().sum().sum() == 0, "Missing values in weather_station_locations."
    assert __is_numeric__(df.LAT), "Latitude column in weather_station_locations is not numeric"
    assert __is_numeric__(df.LON), "Longitude column in weather_station_locations is not numeric"

    conn = duckdb.connect(trusted_zone_db)
    duck_db_helper.create_table(trusted_zone_table, df, conn)
    conn.close()



    trusted_zone_table = 'Metoffice'
    metoffice_tables = list(filter(lambda x: x.startswith(trusted_zone_table), formatted_zone_tables))
    # group all tables for the same month
    unique_met_months = list(set([t[:-len("_yyyy-MM-dd")] for t in metoffice_tables]))

    df = None
    for month in unique_met_months:
        month_tables = filter(lambda x: x.startswith(month), metoffice_tables)

        # pick the newest table for the season
        latest_table_name = __get_last_table__(month_tables)

        formated_conn = duckdb.connect(formatted_zone_db)
        df = pd.concat([df, duck_db_helper.get_table_df(latest_table_name,formated_conn)])
        formated_conn.close()

    # impute missing values
    conn = duckdb.connect(trusted_zone_db)
    loc_df = duck_db_helper.get_table_df('weather_station_locations', conn)
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
                    loc_df['__distance__'] = loc_df.apply(lambda row: __distance__(lat, lon, row['LAT'], row['LON']), axis=1)
                    # Sort the DataFrame by __distance__
                    loc_df = loc_df.sort_values(by='__distance__')

                    # Select the top three closest stations
                    closest_stations = list(loc_df.head(4).SITE)
                    closest_df = df.loc[(df.Date == date) & df.Station_name.apply(lambda x: x in closest_stations)]

                    if column == 'WDIR':
                        interpolated_value = __deg_to_wdir__(closest_df.loc[:,column].apply(__wdir_to_deg__).mean())
                    else:
                        interpolated_value = closest_df.loc[:,column].mean()

                # print(i, interpolated_value, station, column, date)
                imputed_df.loc[i, column] = interpolated_value

    assert not __is_numeric__(imputed_df.Station_name), "Column Station_name should not be numeric in Metdata"
    assert not __is_numeric__(imputed_df.WDIR), "Column WDIR should not be numeric in Metdata"

    assert __is_numeric__(imputed_df.Station_no), "Column Station_no should be numeric in Metdata"
    assert __is_numeric__(imputed_df.PRESS), "Column PRESS should be numeric in Metdata"
    assert __is_numeric__(imputed_df.WSPD), "Column WSPD should be numeric in Metdata"
    assert __is_numeric__(imputed_df.CLOUD), "Column CLOUD should be numeric in Metdata"
    assert __is_numeric__(imputed_df.TEMP), "Column TEMP should be numeric in Metdata"
    assert __is_numeric__(imputed_df.TDEW), "Column TDEW should be numeric in Metdata"

    conn = duckdb.connect(trusted_zone_db)
    duck_db_helper.create_table(trusted_zone_table, imputed_df, conn)
    conn.close()


    trusted_zone_table = 'football-data'
    football_data_tables = list(filter(lambda x: x.startswith(trusted_zone_table), formatted_zone_tables))
    # group all tables for the same season
    unique_seasons = list(set([t[:-len("_yyyy-MM-dd")] for t in football_data_tables]))


    df = None
    for season in unique_seasons:
        season_tables = filter(lambda x: x.startswith(season), football_data_tables)
        # pick the newest table for the season
        latest_table_name = __get_last_table__(season_tables)

        # retrieve the formatted zone df
        formatted_conn = duckdb.connect(formatted_zone_db)
        df = pd.concat([df, duck_db_helper.get_table_df(latest_table_name, formatted_conn)])
        formatted_conn.close()

    df = df.reset_index(drop=True)
    imputed_df = df.__deepcopy__()


    assert (df.FTR != df.apply(lambda row: "D" if row["FTHG"] == row["FTAG"] else "H" if row["FTHG"] > row["FTAG"] else "A", axis=1)).sum() == 0, "inconsitency with game outcome and goals scored"
    assert (df.HTR != df.apply(lambda row: "D" if row["HTHG"] == row["HTAG"] else "H" if row["HTHG"] > row["HTAG"] else "A", axis=1)).sum() == 0, "inconsitency with halftime outcome and goals scored"
    assert (df.HS < df.HST).sum() == 0, "inconsistencies with goals scored in halftime and full game"
    assert (df.AS < df.AST).sum() == 0, "inconsistencies with goals scored in halftime and full game"

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
    duck_db_helper.create_table(trusted_zone_table, imputed_df, trusted_conn)
    trusted_conn.close()


