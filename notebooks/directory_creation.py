import os

# List of directories to create
directories = [
    '../data/exploitation_zone/',
    '../data/trusted_zone/',
    '../data/formatted_zone/',
    '../data/landing/temporal',
    '../data/landing/persistent',
    '../data/landing/persistent/Metoffice',
    '../data/landing/persistent/cleaned_merged_seasons',
    '../data/landing/persistent/master_team_list',
    '../data/landing/persistent/football-data',
    '../data/landing/persistent/team_stadium_location',
    '../data/landing/persistent/weather_station_locations'
    '../data/analytical_sandboxes/'
]

for directory in directories:
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        print(f"\tDirectory {directory} created.")
    else:
        print(f"\tDirectory {directory} already exists.")
