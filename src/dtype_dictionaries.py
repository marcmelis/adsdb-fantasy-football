import pandas as pd

def create_dtype_dict(filename): 
    print(filename)
    if 'cleaned_merged_seasons' in filename: 
        return ({
            'season_x': str,
            'name': str,
            'position': str,
            'team_x': str,
            'assists': int, 
            'bonus': int, 
            'bps': int, 
            'clean_sheets': int,
            'creativity': float, 
            'element': int,
            'fixture': int,
            'goals_conceded': int, 
            'goals_scored': int, 
            'ict_index': float,
            'influence': float, 
            'kickoff_time': str,
            'minutes': int,
            'opponent_team': int, 
            'opp_team_name': str,
            'own_goals': int,
            'penalties_missed': int, 
            'penalties_saved': int, 
            'red_cards': int, 
            'round': int, 
            'saves': int, 
            'selected': int, 
            'team_a_score': int, 
            'team_h_score': int,
            'threat': int,
            'total_points': int, 
            'transfers_balance': int, 
            'transfers_in': int, 
            'transfers_out': int, 
            'value': int, 
            'was_home': int, 
            'yellow_cards': int, 
            'GW': int
        },
        ['kickoff_time']
        )
    elif 'football-data' in filename: 
        return ({
            "Div": str, # = League Division
            "Date": str, # = Match Date (dd/mm/yy)
            "Time": str, # = Time of match kick off
            "HomeTeam": str, # = Home Team
            "AwayTeam": str, # = Away Team
            "FTHG": int, #  = Full Time Home Team Goals
            "FTAG": int, # = Full Time Away Team Goals
            "FTR": str, # Full Time Result (H=Home Win, D=Draw, A=Away Win)
            "HTHG": int, # = Half Time Home Team Goals
            "HTAG": int, # = Half Time Away Team Goals
            "HTR": str,  # = Half Time Result (H=Home Win, D=Draw, A=Away Win)

            "Referee": str, # = Match Referee
            "HS": int, # = Home Team Shots
            "AS": int, # = Away Team Shots
            "HST": int, # = Home Team Shots on Target
            "AST": int, # = Away Team Shots on Target
            "HF": int, # = Home Team Fouls Committed
            "AF": int, # = Away Team Fouls Committed
            "HC": int, # = Home Team Corners
            "AC": int, # = Away Team Corners
            "HY": int, # = Home Team Yellow Cards
            "AY": int, # = Away Team Yellow Cards
            "HR": int, # = Home Team Red Cards
            "AR": int, # = Away Team Red Cards

            "B365H": float, # = Bet365 home win odds
            "B365D": float, # = Bet365 draw odds
            "B365A": float, # = Bet365 away win odds
            "BWH": float, # = Bet&Win home win odds
            "BWD": float, # = Bet&Win draw odds
            "BWA": float, # = Bet&Win away win odds
            "IWH": float, # = Interwetten home win odds
            "IWD": float, # = Interwetten draw odds
            "IWA": float, # = Interwetten away win odds
            "PSH": float, # and PH = Pinnacle home win odds
            "PSD": float, # and PD = Pinnacle draw odds
            "PSA": float, # and PA = Pinnacle away win odds
            "WHH": float, # = William Hill home win odds
            "WHD": float, # = William Hill draw odds
            "WHA": float, # = William Hill away win odds
            "VCH": float, # = VC Bet home win odds
            "VCD": float, # = VC Bet draw odds
            "VCA": float, # = VC Bet away win odds
            
            "MaxH": float, # = Market maximum home win odds
            "MaxD": float, # = Market maximum draw win odds
            "MaxA": float, # = Market maximum away win odds
            "AvgH": float, # = Market average home win odds
            "AvgD": float, # = Market average draw win odds
            "AvgA": float, # = Market average away win odds

            "B365>2.5": float, # = Bet365 over 2.5 goals
            "B365<2.5": float, # = Bet365 under 2.5 goals
            "P>2.5": float, # = Pinnacle over 2.5 goals
            "P<2.5": float, # = Pinnacle under 2.5 goals
            "Max>2.5": float, # = Market maximum over 2.5 goals
            "Max<2.5": float, # = Market maximum under 2.5 goals
            "Avg>2.5": float, # = Market average over 2.5 goals
            "Avg<2.5": float, # = Market average under 2.5 goals
            
            "AHh": float, # = Market size of handicap (home team) (since 2019/2020)
            "B365AHH": float, # = Bet365 Asian handicap home team odds
            "B365AHA": float, # = Bet365 Asian handicap away team odds
            "PAHH": float, # = Pinnacle Asian handicap home team odds
            "PAHA": float, # = Pinnacle Asian handicap away team odds
            "MaxAHH": float, # = Market maximum Asian handicap home team odds
            "MaxAHA": float, # = Market maximum Asian handicap away team odds	
            "AvgAHH": float, # = Market average Asian handicap home team odds
            "AvgAHA": float, # = Market average Asian handicap away team odds

            # Closing odds
            "B365CH": float, # = Bet365 home win odds
            "B365CD": float, # = Bet365 draw odds
            "B365CA": float, # = Bet365 away win odds
            "BWCH": float, # = Bet&Win home win odds
            "BWCD": float, # = Bet&Win draw odds
            "BWCA": float, # = Bet&Win away win odds
            "IWCH": float, # = Interwetten home win odds
            "IWCD": float, # = Interwetten draw odds
            "IWCA": float, # = Interwetten away win odds
            "PSCH": float, # and PH = Pinnacle home win odds
            "PSCD": float, # and PD = Pinnacle draw odds
            "PSCA": float, # and PA = Pinnacle away win odds
            "WHCH": float, # = William Hill home win odds
            "WHCD": float, # = William Hill draw odds
            "WHCA": float, # = William Hill away win odds
            "VCCH": float, # = VC Bet home win odds
            "VCCD": float, # = VC Bet draw odds
            "VCCA": float, # = VC Bet away win odds
            
            "MaxCH": float, # = Market maximum home win odds
            "MaxCD": float, # = Market maximum draw win odds
            "MaxCA": float, # = Market maximum away win odds
            "AvgCH": float, # = Market average home win odds
            "AvgCD": float, # = Market average draw win odds
            "AvgCA": float, # = Market average away win odds

            "B365C>2.5": float, # = Bet365 over 2.5 goals
            "B365C<2.5": float, # = Bet365 under 2.5 goals
            "PC>2.5": float, # = Pinnacle over 2.5 goals
            "PC<2.5": float, # = Pinnacle under 2.5 goals
            "MaxC>2.5": float, # = Market maximum over 2.5 goals
            "MaxC<2.5": float, # = Market maximum under 2.5 goals
            "AvgC>2.5": float, # = Market average over 2.5 goals
            "AvgC<2.5": float, # = Market average under 2.5 goals
            
            "AHCh": float, # = Market size of handicap (home team) (since 2019/2020)
            "B365CAHH": float, # = Bet365 Asian handicap home team odds
            "B365CAHA": float, # = Bet365 Asian handicap away team odds
            "PCAHH": float, # = Pinnacle Asian handicap home team odds
            "PCAHA": float, # = Pinnacle Asian handicap away team odds
            "MaxCAHH": float, # = Market maximum Asian handicap home team odds
            "MaxCAHA": float, # = Market maximum Asian handicap away team odds	
            "AvgCAHH": float, # = Market average Asian handicap home team odds
            "AvgCAHA": float, # = Market average Asian handicap away team odds
        },
        ['Date', 'Time']
        )
    elif 'master_team_list' in filename: 
        return ({
            'season': str, 
            'team': int,
            'team_name': str
        },
        []
        )
    elif 'Metoffice' in filename: 
        return ({
            'Date': pd.Series([], dtype="datetime64[s]"),
            'Station_no': pd.Series([], dtype=int), 
            'Station_name': pd.Series([], dtype=str), 
            'PRESS': pd.Series([], dtype=float), 
            'WDIR': pd.Series([], dtype=str), 
            'WSPD': pd.Series([], dtype=float), 
            'CLOUD': pd.Series([], dtype=float), 
            'TEMP': pd.Series([], dtype=float)
        },
        ['Date']
        )
    elif 'team_stadium_location' in filename:
        return ({
            'team_name': str, 
            'NAME': str,
            'LON': float,
            'LAT': float
        },
        []
        )
    elif 'weather_station_locations' in filename:
        return ({
            'SITE': str,
            'LAT': float,
            'LON': float
        },
        []
        )