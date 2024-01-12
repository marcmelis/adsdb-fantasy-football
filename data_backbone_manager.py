import argparse
import os
from src import data_load
from src import directory_creation
from src import persistent_zone
from src import formatted_zone
from src import trusted_zone
from src import exploitation_zone
from src import analytical_sandbox
from src import feature_generation
from src import data_quality
from src import split
from src import model_training
from src import validate
from src import prediction

DATA_DIR = 'data'

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
    data_dir = DATA_DIR
    print("Moving files to persistent")
    persistent_zone.move_to_persistent(data_dir)

def formatted_command(args):
    print("Enable formatted data")
    formatted_zone.move_to_formatted(DATA_DIR)

def list_formatted_tables(args):
    formatted_zone.list_formatted_tables(DATA_DIR)

def trusted_command(args):
    print("Enable trusted data")
    trusted_zone.move_to_trusted(DATA_DIR)

def exploitation_command(args):
    print("Enable exploitation data")
    exploitation_zone.move_to_exploitation_zone(DATA_DIR)

def analytical_command(args):
    print("Enable analytical sandbox data")
    analytical_sandbox.move_to_analytical_sandbox(DATA_DIR)

def feature_command(args):
    print("Enable feature generation data")
    feature_generation.move_to_feature_generation(DATA_DIR)

def data_quality_command(args):
    print("Enabling Data Quality")
    data_quality.execute_data_quality(DATA_DIR)

def split_command(args):
    print('Splitting Train and Test')
    split.split(DATA_DIR)

def model_train_command(args):
    print("Train new model")
    model_training.train_model(DATA_DIR)

def model_validate_command(args):
    print("Validating new model")
    validate.validate_model(DATA_DIR)

def predict_command(args):
    print("Predict on new data")
    prediction.predict(DATA_DIR)

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
        "--analytical_sandbox",
        "-s",
        action="store_true",
        help="Enable analytical sandbox data",
    )

    parser.add_argument(
        "--feature_generation",
        "-g",
        action="store_true",
        help="Enable feature generation data",
    )

    parser.add_argument(
        "--data_quality",
        "-q",
        action="store_true",
        help="Enable feature generation data",
    )

    parser.add_argument(
        "--split",
        action="store_true",
        help="Split train and test",
    )

    parser.add_argument(
        "--train",
        action="store_true",
        help="Train new model",
    )

    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate new model",
    )

    parser.add_argument(
        "--predict",
        action="store_true",
        help="Run a prediction for a player",
    )

    parser.add_argument(
        "--second_part",
        action="store_true",
        help="Run second part of the project",
    )

    parser.add_argument(
        "--all",
        "-a",
        action="store_true",
        help="Run all options",
    )

    args = parser.parse_args()
    directory_creation.create_directories(DATA_DIR)

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
    if args.analytical_sandbox:
        analytical_command(args)
    if args.feature_generation:
        feature_command(args)
    if args.data_quality:
        data_quality_command(args)
    if args.split:
        split_command(args)
    if args.train:
        model_train_command(args)
    if args.validate:
        model_validate_command(args)
    if args.predict:
        predict_command(args)
    if args.all:
        download_command(args)
        persistent_command(args)
        formatted_command(args)
        trusted_command(args)
        exploitation_command(args)
        analytical_command(args)
        feature_command(args)
        split_command(args)
        model_train_command(args)
        model_validate_command(args)
    if args.second_part:
        analytical_command(args)
        feature_command(args)
        split_command(args)
        train_command(args)
        validate_command(args)
        model_train_command(args)
        model_validate_command(args)

if __name__ == "__main__":
    main()
