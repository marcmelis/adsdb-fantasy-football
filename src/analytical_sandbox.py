import os
import sys

import duckdb
import pandas as pd

from . import duck_db_helper


def move_to_analytical_sandbox(data_dir):

    conn = duckdb.connect(os.path.join(data_dir, 'exploitation_zone', 'exploitation_zone.db'))
    tables = duck_db_helper.get_tables(conn)
    df_list = []
    df_list_names = []
    for table_name in tables:
        df = conn.sql(f"SELECT * FROM \"{table_name}\";").df()
        df_list.append(df)
        df_list_names.append(table_name)
    conn.close()

    dfm = df_list[0] # Matches
    dfp = df_list[1] # Players

    # Remove unused columns
    unused_players_cols = [ 'fixture', 'kickoff_time','transfers_balance','transfers_in','transfers_out']
    unused_players_cols = [col for col in unused_players_cols if col in dfp.columns]
    dfp = dfp.drop(unused_players_cols, axis=1)
    unused_matches_cols = [ 'Div','Time', 'HTHG', 'HTAG', 'HTR', 'Referee',
                           'HS', 'AS', 'HST', 'AST', 'HF', 'AF', 'HC', 'AC', 'HY', 'AY', 'HR', 'AR'
                        ]
    unused_matches_cols = [col for col in unused_matches_cols if col in dfm.columns]
    dfm = dfm.drop(unused_matches_cols, axis=1)
    unused_matches_betting_odds = ['B365H', 'B365D', 'B365A', 'BWH', 'BWD', 'BWA', 'IWH', 'IWD', 'IWA', 'PSH',
                                    'PSD', 'PSA', 'WHH', 'WHD', 'WHA', 'VCH', 'VCD', 'VCA', 'MaxH', 'MaxD', 'MaxA',
                                    'B365>2.5', 'B365<2.5', 'P>2.5', 'P<2.5', 'Max>2.5', 'Max<2.5',
                                    'Avg>2.5', 'Avg<2.5', 'AHh', 'B365AHH', 'B365AHA', 'PAHH', 'PAHA',
                                    'MaxAHH', 'MaxAHA', 'AvgAHH', 'AvgAHA', 'B365CH', 'B365CD', 'B365CA',
                                    'BWCH', 'BWCD', 'BWCA', 'IWCH', 'IWCD', 'IWCA', 'PSCH', 'PSCD', 'PSCA',
                                    'WHCH', 'WHCD', 'WHCA', 'VCCH', 'VCCD', 'VCCA', 'MaxCH', 'MaxCD', 'MaxCA',
                                    'AvgCH', 'AvgCD', 'AvgCA', 'B365C>2.5', 'B365C<2.5', 'PC>2.5', 'PC<2.5',
                                    'MaxC>2.5', 'MaxC<2.5', 'AvgC>2.5', 'AvgC<2.5', 'AHCh', 'B365CAHH', 'B365CAHA',
                                    'PCAHH', 'PCAHA', 'MaxCAHH', 'MaxCAHA', 'AvgCAHH', 'AvgCAHA'
                                    ]
    unused_matches_betting_odds = [col for col in unused_matches_betting_odds if col in dfm.columns]
    dfm = dfm.drop(unused_matches_betting_odds, axis=1)

    # Filter only by the dates we have match data (2022-2023)
    start = pd.to_datetime(min(dfm['Date']))
    end = pd.to_datetime(max(dfm['Date']))
    dfp = dfp[(dfp['match_date'] >= start) & (dfp['match_date'] <= end)]

    # Filter players with at least 10 minutes played in 10 different matches
    dfp_10 = dfp[dfp['minutes'] >= 10]
    name_counts = dfp_10.groupby('name').size()
    at_least_10_games = name_counts[name_counts >= 10]
    wanted_players = at_least_10_games.index.to_list()
    dfp = dfp[dfp['name'].isin(wanted_players)]

    # Save into the analytical sandbox
    conn = duckdb.connect(os.path.join(data_dir, 'analytical_sandboxes', 'analytical_sandbox_zone.db'))
    duck_db_helper.create_table('matches', dfm, conn)
    duck_db_helper.create_table('players', dfp, conn)
    conn.close()
