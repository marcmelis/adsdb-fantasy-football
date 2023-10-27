import argparse
import os
from src import data_load
from src import directory_creation
from src import persistent_zone
from src import formatted_zone
from src import trusted_zone

def download_command(args):
    save_dir = "data/landing/temporal"
    if any(['all' == dataset for dataset in args.download]):
        print(f"Download data from all sources")
        data_load.download_metdata(save_dir)
        data_load.download_gameodds(save_dir)
        data_load.download_kaggle(save_dir)
        return
    if any(['metdata' == dataset for dataset in args.download]):
        print(f"Download data from metdata")
        data_load.download_metdata(save_dir)

    if any(['gameodds' == dataset for dataset in args.download]):
        print(f"Download data from gameodds")
        data_load.download_gameodds(save_dir)

    if any(['kaggle' == dataset for dataset in args.download]):
        print(f"Download data from kaggle")
        data_load.download_kaggle(save_dir)
    

def persistent_command(args):
    data_dir = "data"
    print("Creating directory structure")
    directory_creation.create_directories(data_dir)
    print("Moving files to persistent")
    persistent_zone.move_to_persistent(data_dir)

def formatted_command(args):
    print("Enable formatted data")
    formatted_zone.move_to_formatted("data")

def list_formatted_tables(args):
    formatted_zone.list_formatted_tables("data")

def trusted_command(args):
    print("Enable trusted data")
    trusted_zone.move_to_trusted("data")

def exploitation_command(args):
    print("Enable exploitation data")

def main():
    parser = argparse.ArgumentParser(description="Data Backbone Manager")

    parser.add_argument(
        "--download",
        "-d",
        nargs="*",
        choices=["all", "kaggle", "metdata", "gameodds"],
        help="Download data from specified sources",
    )

    parser.add_argument(
        "--persistent",
        "-p",
        action="store_true",
        help="Enable persistent storage",
    )

    parser.add_argument(
        "--formatted",
        "-f",
        action="store_true",
        help="Enable formatted data",
    )

    parser.add_argument(
        "--list_formatted_tables",
        action="store_true",
        help="Enable formatted data",
    )

    parser.add_argument(
        "--trusted",
        "-t",
        action="store_true",
        help="Enable trusted data",
    )

    parser.add_argument(
        "--exploitation",
        "-e",
        action="store_true",
        help="Enable exploitation data",
    )

    parser.add_argument(
        "--all",
        "-a",
        action="store_true",
        help="Run all options",
    )

    args = parser.parse_args()
    
    if args.download:
        download_command(args)
    if args.persistent:
        persistent_command(args)
    if args.formatted:
        formatted_command(args)
    if args.list_formatted_tables:
        list_formatted_tables(args)
    if args.trusted:
        trusted_command(args)
    if args.exploitation:
        exploitation_command(args)
    if args.all:
        download_command(args)
        persistent_command(args)
        formatted_command(args)
        trusted_command(args)
        exploitation_command(args)

if __name__ == "__main__":
    main()



