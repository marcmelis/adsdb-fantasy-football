import os

def create_directories(data_dir):
# List of directories to create
    directories = [
        'exploitation_zone/',
        'trusted_zone/',
        'formatted_zone/',
        'landing/temporal',
        'landing/persistent',
        'landing/persistent/Metoffice',
        'landing/persistent/cleaned_merged_seasons',
        'landing/persistent/master_team_list',
        'landing/persistent/football-data',
        'landing/persistent/team_stadium_location',
        'landing/persistent/weather_station_locations'
        'analytical_sandboxes/'
    ]

    for directory in directories:
        if not os.path.exists(os.path.join(data_dir,directory)):
            os.makedirs(os.path.join(data_dir,directory), exist_ok=True)
            print(f"\tDirectory {directory} created.")
        else:
            print(f"\tDirectory {directory} already exists.")
