import os

# List of directories to create
directories = [
    '../data/exploitation_zone/',
    '../data/trusted_zone/',
    '../data/formatted_zone/',
    '../data/landing/temporal',
    '../data/landing/persistent'
]

for directory in directories:
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        print(f"Directory {directory} created.")
    else:
        print(f"Directory {directory} already exists.")