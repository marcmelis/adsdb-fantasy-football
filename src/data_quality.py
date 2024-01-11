import os
import sys

import pandas as pd
import matplotlib.pyplot as plt
import duckdb

from . import duck_db_helper


# DENIAL CONSTRAINTS

def check_unique_matches(match_df):
    duplicates = match_df.duplicated(subset=['Date', 'HomeTeam', 'AwayTeam'])
    if duplicates.any():
        print("DC1 Violation: Duplicate match entries found.")
        return match_df[duplicates].index.tolist(), ['Date', 'HomeTeam', 'AwayTeam']
    return [], []

def check_non_negative_goals(match_df):
    negative_goals = match_df[(match_df['FTHG'] < 0) | (match_df['FTAG'] < 0)]
    if not negative_goals.empty:
        print("DC2 Violation: Negative goal values found.")
        return negative_goals.index.tolist(), ['FTHG', 'FTAG']
    return [], []

def check_match_result_logic(match_df):
    result_logic = (
        (match_df['FTHG'] > match_df['FTAG']) & (match_df['FTR'] != 'H') |
        (match_df['FTHG'] < match_df['FTAG']) & (match_df['FTR'] != 'A') |
        (match_df['FTHG'] == match_df['FTAG']) & (match_df['FTR'] != 'D')
    )
    if result_logic.any():
        print("DC3, DC4, DC5 Violation: Inconsistent match results found.")
        return match_df[result_logic].index.tolist(), ['FTHG', 'FTAG', 'FTR']
    return [], []

def check_betting_odds_validity(match_df):
    invalid_odds = match_df[(match_df['AvgH'] <= 0) | (match_df['AvgD'] <= 0) | (match_df['AvgA'] <= 0)]
    if not invalid_odds.empty:
        print("DC6 Violation: Invalid betting odds found.")
        return invalid_odds.index.tolist(), ['AvgH', 'AvgD', 'AvgA']
    return [], []

def check_weather_data_integrity(match_df):
    weather_issues = match_df[
        (match_df['PRESS'] < 800) | (match_df['PRESS'] > 1080) |
        (match_df['TEMP'] < -50) | (match_df['TEMP'] > 60)
    ]
    if not weather_issues.empty:
        print("DC7, DC8 Violation: Weather data out of expected range found.")
        return weather_issues.index.tolist(), ['PRESS', 'TEMP']
    return [], []

def check_wind_direction_consistency(match_df):
    valid_directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW', 'NNE', 'ENE', 'ESE', 'SSE', 'SSW', 'WSW', 'WNW', 'NNW']
    invalid_wdir = match_df[~match_df['WDIR'].isin(valid_directions)]
    if not invalid_wdir.empty:
        print("DC9 Violation: Invalid wind directions found.")
        print(invalid_wdir[['HomeTeam','AwayTeam', 'Date','WDIR']])
        return invalid_wdir.index.tolist(), ['WDIR']
    return [], []

def check_position_consistency(player_df):
    inconsistent_positions = player_df.groupby('name')['position'].unique()
    inconsistent_names = inconsistent_positions[inconsistent_positions.apply(len) > 1].index
    if inconsistent_names.empty:
        return [], []
    print("DC1 Violation: Players with inconsistency in positions:")
    print(player_df[player_df['name'].isin(inconsistent_names)][['name', 'position']])
    return player_df[player_df['name'].isin(inconsistent_names)].index.tolist(), ['position']

def check_max_points(player_df, max_points=20):
    over_point_players = player_df[player_df['total_points'] > max_points]
    if over_point_players.empty:
        return [], []
    print("DC2 Violation: Players with unrealistically high total points found")
    print(over_point_players[['name', 'match_date', 'total_points']])
    return over_point_players.index.tolist(), ['total_points']

def check_double_red_card(player_df):
    illegal_players = player_df[player_df['red_cards'] > 1]
    if illegal_players.empty:
        return [], []
    print("DC3 Violation: Players with illegal red cards found")
    print(illegal_players[['name', 'match_date']])
    return illegal_players.index.tolist(), ['red_cards']

def check_goalkeepers_goals(player_df):
    gk_goal = player_df[(player_df['position'] == 'GK') & (player_df['goals_scored'] > 0)]
    if gk_goal.empty:
        return [], []
    print("DC4 Violation: Goalkeepers with goals found")
    print(gk_goal[['name', 'match_date', 'goals_scored']])
    return gk_goal.index.tolist(), ['goals_scored']

def check_unique_player_identification(player_df):
    duplicate_players = player_df[player_df.duplicated(['name', 'match_date', 'team_x'], keep=False)]
    if duplicate_players.empty:
        return [], []
    print("DC5 Violation: Duplicate player records in a single match from the same team found")
    print(duplicate_players[['name', 'match_date', 'team_x']])
    return duplicate_players.index.tolist(), ['name', 'match_date', 'team_x']

def check_player_play_time(player_df):
    overplayed_players = player_df[player_df['minutes'] > 90]
    if overplayed_players.empty:
        return [], []
    print("DC6 Violation: Players who played more than 90 minutes:")
    print(overplayed_players[['name', 'match_date', 'minutes']])
    return overplayed_players.index.tolist(), ['minutes']

def check_player_metrics(player_df):
    invalid_metrics = player_df[(player_df['creativity'] < 0) | (player_df['threat'] < 0) | (player_df['influence'] < 0)]
    if invalid_metrics.empty:
        return [], []
    print("DC7 Violation: Negative values in creativity, threat, or influence metrics.")
    print(invalid_metrics[['name', 'match_date']])
    return invalid_metrics.index.tolist(), ['creativity', 'threat', 'influence']

def check_player_goals_vs_team_goals(joined_df):
    joined_df['team_goals'] = joined_df.apply(lambda x: x['FTHG'] if x['was_home'] == 1 else x['FTAG'], axis=1)
    violations = joined_df[joined_df['goals_scored'] > joined_df['team_goals']].index.tolist()
    if violations:
        print("DC1 Violation: Players scoring more goals than their team in a match.")
    return violations, ['goals_scored']

def check_player_home_status_consistency(joined_df):
    violations = joined_df[(joined_df['was_home'] == 1) & (joined_df['team_x'] != joined_df['HomeTeam'])].index.tolist()
    if violations:
        print("DC2 Violation: Inconsistency in 'was_home' status.")
    return violations, ['was_home','team_x', 'HomeTeam']

def check_player_team_consistency(joined_df):
    violations = joined_df[(joined_df['team_x'] != joined_df['HomeTeam']) & (joined_df['team_x'] != joined_df['AwayTeam'])].index.tolist()
    if violations:
        print("DC3 Violation: Inconsistency in team names for players.")
    return violations, ['team_x','HomeTeam','AwayTeam']


class DenialConstraintsChecker:
    def __init__(self, denial_constraints, df, key_columns):
        self.dc_list = denial_constraints
        self.df = df
        self.key_columns = key_columns

    def check_denial_constraint(self, dc):
        invalid_rows, relevant_columns = dc(self.df)
        if invalid_rows:
            self.correct_rows(invalid_rows, relevant_columns)

    def check_denial_constraints(self):
        for dc in self.dc_list:
            self.check_denial_constraint(dc)
        print(f'FINISH: All denial constraints checked.')

    def correct_rows(self, invalid_rows, relevant_columns):
        for index in invalid_rows:
            key_values = ", ".join([f"{key}: {self.df.at[index, key]}" for key in self.key_columns])
            for col in relevant_columns:
                current_value = self.df.at[index, col]
                user_input = input(f"Input new value for: \n\t{col} ({key_values}) \n(Current: {current_value}, Enter for no change): \n")
                if user_input:
                    self.df.at[index, col] = user_input


def execute_data_quality(data_dir):
    conn = duckdb.connect(os.path.join(data_dir, 'analytical_sandboxes', 'analytical_sandbox_zone.db'))
    match_df = duck_db_helper.get_table_df("matches",conn)
    player_df = duck_db_helper.get_table_df("players",conn)
    conn.close()

    player_df['goals_scored'] = pd.to_numeric(player_df['goals_scored'], errors='coerce')
    player_df['assists'] = pd.to_numeric(player_df['assists'], errors='coerce')
    player_df['red_cards'] = pd.to_numeric(player_df['red_cards'], errors='coerce')
    player_df['total_points'] = pd.to_numeric(player_df['total_points'], errors='coerce')
    player_df['minutes'] = pd.to_numeric(player_df['minutes'], errors='coerce')
    player_df['creativity'] = pd.to_numeric(player_df['creativity'], errors='coerce')
    player_df['threat'] = pd.to_numeric(player_df['threat'], errors='coerce')
    player_df['influence'] = pd.to_numeric(player_df['influence'], errors='coerce')

    matches_denial_constraints = [
        check_unique_matches,
        check_non_negative_goals,
        check_match_result_logic,
        check_betting_odds_validity,
        check_weather_data_integrity,
        check_wind_direction_consistency
    ]

    players_denial_constraints = [
        check_position_consistency,
        check_max_points,
        check_double_red_card,
        check_goalkeepers_goals,
        check_unique_player_identification,
        check_player_play_time,
        check_player_metrics
    ]

    joined_denial_constraints = [
        check_player_goals_vs_team_goals,
        check_player_home_status_consistency,
        check_player_team_consistency
    ]

    match_dc = DenialConstraintsChecker(matches_denial_constraints,match_df,['HomeTeam','AwayTeam', 'Date'])
    match_dc.check_denial_constraints()

    player_dc = DenialConstraintsChecker(players_denial_constraints,player_df,['name','match_date'])
    player_dc.check_denial_constraints()

    df_home = pd.merge(player_df, match_df, left_on=['team_x', 'match_date'], right_on=['HomeTeam', 'Date'], how='inner')
    df_away = pd.merge(player_df, match_df, left_on=['team_x', 'match_date'], right_on=['AwayTeam', 'Date'], how='inner')
    joined_df = pd.concat([df_home, df_away], ignore_index=True)

    joined_dc = DenialConstraintsChecker(joined_denial_constraints,joined_df,['name','match_date'])
    joined_dc.check_denial_constraints()

    player_columns = [col for col in joined_df.columns if col in player_df.columns]
    match_columns = [col for col in joined_df.columns if col in match_df.columns]

    player_changes_df = joined_df[player_columns].drop_duplicates()
    match_changes_df = joined_df[match_columns].drop_duplicates()

    conn = duckdb.connect(os.path.join(data_dir, 'analytical_sandboxes', 'analytical_sandbox_zone.db'))
    duck_db_helper.create_table('matches', match_changes_df, conn)
    duck_db_helper.create_table('players', player_changes_df, conn)
    conn.close()
