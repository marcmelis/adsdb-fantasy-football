import os
import sys

import pandas as pd
import duckdb

from . import duck_db_helper

def calculate_streak(player_group):
    streaks = []
    win_count = 0

    for outcome in player_group['match_outcome']:
        if outcome == 'win':
            win_count += 1
        else:
            win_count = 0

        # If the last three matches are wins, streak is 'hot', else 'cold'
        if win_count > 3:
            streaks.append('hot')
        else:
            streaks.append('cold')
    player_group['streak'] = streaks
    return player_group

def match_outcome(row):
    if (row['was_home'] == 1 and row['FTR'] == 'H') or (row['was_home'] == 0 and row['FTR'] == 'A'):
        return 'win'
    elif (row['was_home'] == 1 and row['FTR'] == 'A') or (row['was_home'] == 0 and row['FTR'] == 'H'):
        return 'lose'
    else:
        return 'draw'

def calculate_avg_points_last_3_matches(player_group):
    last_points = []
    average_last_3 = []

    for points in player_group['total_points']:
        average_last_3.append(round(sum(last_points)/len(last_points)) if last_points else 0)

        last_points.append(points)
        if len(last_points) > 3:
            last_points.pop(0)

    player_group['last_3_matches_average'] = average_last_3
    return player_group


def move_to_feature_generation(data_dir):
    conn = duckdb.connect(os.path.join(data_dir, 'analytical_sandboxes', 'analytical_sandbox_zone.db'))
    tables = duck_db_helper.get_tables(conn)
    df_list = []
    df_list_names = []
    for table_name in tables:
        df = conn.sql(f"SELECT * FROM \"{table_name}\";").df()
        df_list.append(df)
        df_list_names.append(table_name)
    conn.close()

    dfm = df_list[0]
    dfp = df_list[1]

    # Merge dataframes into players
    df_home = pd.merge(dfp, dfm, left_on=['team_x', 'match_date'], right_on=['HomeTeam', 'Date'], how='inner')
    df_away = pd.merge(dfp, dfm, left_on=['team_x', 'match_date'], right_on=['AwayTeam', 'Date'], how='inner')
    df = pd.concat([df_home, df_away], ignore_index=True)

    # Remove join keys that are repeated
    cols_to_remove = ['Date','HomeTeam','AwayTeam']
    cols_to_remove = [col for col in cols_to_remove if col in df.columns]
    df = df.drop(cols_to_remove,axis=1)


    # Generating odds for home or not
    home_odds_col = 'AvgH'
    draw_odds_col = 'AvgD'
    away_odds_col = 'AvgA'

    df['odds_against'] = 0.0
    df['odds_for'] = 0.0

    # Fill in odds columns based on whether the player was home or not
    df.loc[df['was_home'] == 1, 'odds_for'] = df.loc[df['was_home'] == 1, home_odds_col]
    df.loc[df['was_home'] == 0, 'odds_for'] = df.loc[df['was_home'] == 0, away_odds_col]
    df.loc[df['was_home'] == 1, 'odds_against'] = df.loc[df['was_home'] == 1, away_odds_col]
    df.loc[df['was_home'] == 0, 'odds_against'] = df.loc[df['was_home'] == 0, home_odds_col]
    df.drop(columns=[home_odds_col, away_odds_col], inplace=True)
    df.rename(columns={'AvgD': 'odds_draw'}, inplace=True)


    df.loc[df.was_home==0].head(5)


    # Generate goals and goals against of the team and results according to the team

    # Calculate total_goals and total_against based on home or away status
    df['team_goals'] = df.apply(lambda x: x['FTHG'] if x['was_home'] == 1 else x['FTAG'], axis=1)
    df['team_goals_against'] = df.apply(lambda x: x['FTAG'] if x['was_home'] == 1 else x['FTHG'], axis=1)

    df['match_outcome'] = df.apply(match_outcome, axis=1)
    df.drop(columns=['FTHG', 'FTAG', 'FTR'], inplace=True)


    # Generate if the player is on a hot streak or cold streak

    df = df.sort_values(by=['name', 'match_date'])
    df = df.groupby('name', group_keys=False).apply(calculate_streak)
    df = df.sort_values(by=['match_date','team_x','name'])


    # Generate average points in the last 3 matches

    df = df.sort_values(by=['name', 'match_date'])
    df = df.groupby('name', group_keys=False).apply(calculate_avg_points_last_3_matches)
    df = df.sort_values(by=['match_date', 'team_x', 'name'])

    # Renaming and reordering columns
    df.rename(columns={
        'team_x': 'team',
        'opp_team_name': 'opponent',
        'PRESS': 'pressure',
        'WDIR': 'wind_direction',
        'WSPD': 'wind_speed',
        'CLOUD': 'cloud_coverage',
        'TEMP': 'temperature',
        'TDEW': 'dew_point'
    }, inplace=True)

    new_order = ['name','match_date', 'team', 'opponent', 'was_home', 'team_goals', 'team_goals_against',
                 'match_outcome', 'streak', 'position', 'assists', 'bonus', 'bps', 'clean_sheets', 'creativity',
                 'element', 'goals_conceded', 'goals_scored', 'ict_index', 'influence', 'minutes', 'own_goals',
                 'penalties_missed', 'penalties_saved', 'yellow_cards', 'red_cards', 'round', 'saves', 'selected',
                 'threat', 'total_points', 'last_3_matches_average', 'value', 'pressure', 'wind_direction', 'wind_speed', 'cloud_coverage',
                 'temperature', 'dew_point', 'odds_against', 'odds_for', 'odds_draw']
    df = df[new_order]


    # Save the data
    conn = duckdb.connect(os.path.join(data_dir, 'feature_generation', 'feature_generation.db'))
    duck_db_helper.create_table('feature_1', df, conn)
    conn.close()
