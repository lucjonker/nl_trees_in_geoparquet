import argparse
import os
import logging
import sys

import geoparquet_io as gpio
from geoparquet_io.core.validate import validate_geoparquet
from geoparquet_io.core.stac import generate_stac_item, generate_stac_collection

from retrieve_data import DatasetDownloader

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CONVERTED_DIRECTORY = "../data/nl_trees/"

CONFIG_PATH = "../data/config/datasets_config.json"

CRS = "EPSG:28992"


def convert_file(processor, dataset, dataset_name):
    # Download data
    response = processor.retrieve_data(dataset['metadata']['download_link'])

    # Parse data
    gdf = processor.parse_data(response, dataset)
    logger.info(f"Parsed {len(gdf)} records")

    # Standardize data
    gdf_standardized = processor.standardize_data(gdf, dataset)

    # Reproject
    gdf_standardized = gdf_standardized.to_crs(28992)
    # Remove rows where geometry is None or Empty
    gdf_standardized = gdf_standardized[~(gdf_standardized.geometry.is_empty | gdf_standardized.geometry.isna())]
    # Write as geoparquet file
    dataset_path = f'{CONVERTED_DIRECTORY}{dataset_name}/{dataset_name}.parquet'

    #If all of this worked, can make a directory and save
    os.makedirs(f'{CONVERTED_DIRECTORY}{dataset_name}/', exist_ok=True)
    gdf_standardized.to_parquet(dataset_path)

    return dataset_path


def add_space_filling_curve(dataset_path: str):
    logger.info("Adding bbox and performing hilbert sorting")
    table = gpio.read(dataset_path)
    table.add_bbox().sort_hilbert()
    table.write(dataset_path, compression='ZSTD', compression_level=15)


def validate(dataset_path: str):
    logger.info("Performing validation")

    validation_result = validate_geoparquet(dataset_path)
    logger.info(f"Validation passed: {validation_result.is_valid} | Num passed tests: {validation_result.passed_count} "
                f"| Num failed tests: {validation_result.failed_count} | Num warnings: {validation_result.warning_count}")

def generate_all_stac(base_directory):
    """
    Scans the data directory, generates an Item for each city subfolder,
    and then generates a root Collection.
    """
    
    # 1. Generate Items for each sub-folder (City)
    # We look for directories inside nl_trees
    subdirs = [d for d in os.listdir(base_directory) 
               if os.path.isdir(os.path.join(base_directory, d))]

    for city in subdirs:
        city_path = os.path.join(base_directory, city)
        parquet_file = os.path.join(city_path, f"{city}.parquet")

        if os.path.exists(parquet_file):
            logger.info(f"Generating STAC Item for: {city}")
            generate_stac_item(
                parquet_file,
                bucket_prefix="s3://bucket/path/to/data/nl_trees/",
            )
        else:
            logger.warning(f"Skipping {city}: No parquet file found at {parquet_file}")

    # 2. Generate the root Collection
    logger.info("Generating STAC Collection for all datasets...")
    
    generate_stac_collection(
        partition_dir=base_directory,
        bucket_prefix="s3://bucket/path/to/data/nl_trees/",
    )

    # 3. Validate
    #TODO: find out what the validate_stac function is called in version 0.8.0 

def main():
    parser = argparse.ArgumentParser(
        description="Pipeline for downloading geospatial tree datasets and converting them to parquet files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
    Examples:
      # Convert all datasets in the provided config file to parquet files
      python main.py convert
      
      # Convert one dataset in the provided config file to parquet files
      python main.py convert-one --name Groningen
      
      #todo: add one for deploying to s3
      #todo: add one for generating STAC metadata
            """
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # Convert all command
    add_parser = subparsers.add_parser('convert',
                                       help='Convert all datasets described in the config file to parquet files')
    add_parser.add_argument('--config', default=CONFIG_PATH, help='Path to config file')

    # Convert one command
    add_parser = subparsers.add_parser('convert-one',
                                       help='Converts one dataset described in the config file to parquet files')
    add_parser.add_argument('--name', help='Name of dataset to convert', required=True)
    add_parser.add_argument('--config', default=CONFIG_PATH, help='Path to config file')

    # Todo: deploy to s3

    # Generate STAC command
    subparsers.add_parser('stac', help='Generate STAC Items and Collection for existing parquet files')

    args = parser.parse_args()
    config_path = CONFIG_PATH

    processor = DatasetDownloader(config_path, logger=logger)
    datasets = processor.config

    if args.command == 'convert':
        print("---- COMMENCING GEOPARQUET CONVERSION ----")
        for dataset in datasets:
            dataset_name = dataset.get('name', 'unknown')
            logger.info(f"Processing dataset: {dataset_name}")
            try:
                process_dataset(dataset, dataset_name, processor)
            except Exception as e:
                logger.error(f"Failed to process {dataset_name}: {e}")
                continue
        sys.exit(0)
    elif args.command == 'convert-one':
        print(f"---- COMMENCING GEOPARQUET CONVERSION FOR {args.name} ----")
        dataset_name = str.capitalize(args.name)
        for dataset in [d for d in datasets if str.capitalize(d.get('name')) == dataset_name]:
            logger.info(f"Processing dataset: {dataset_name}")
            try:
                process_dataset(dataset, dataset_name, processor)
            except Exception as e:
                logger.error(f"Failed to process {dataset_name}: {e}")
                continue
        sys.exit(0)
    elif args.command == 'stac':
        raise NotImplementedError("STAC generation doesnt work yet, we need to host the data first.")
        print("---- COMMENCING STAC METADATA GENERATION ----")
        generate_all_stac(CONVERTED_DIRECTORY)
        sys.exit(0)
    else:
        parser.print_help()


def process_dataset(dataset, dataset_name, processor: DatasetDownloader):
    dataset_path = convert_file(processor, dataset, dataset_name)
    add_space_filling_curve(dataset_path)
    validate(dataset_path)

if __name__ == "__main__":
    main()
