import argparse
import os
# from os.path import isdir
import logging
import sys

import geoparquet_io as gpio
# from geoparquet_io.cli.main import check_all
from geoparquet_io.core.validate import validate_geoparquet

# import geopandas as gpd
# from pathlib import Path

# from shapely import wkt

from retrieve_data import DatasetDownloader

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

#Todo: is this needed anymore
SUPPORTED_TYPES = {
    ".geojson",
    ".json",
    ".shp",
    ".gpkg",
    ".gdb",
}

RAW_DIRECTORY = "../data/raw/"

CONVERTED_DIRECTORY = "../data/nl_trees/"

CONFIG_PATH = "../data/config/datasets_config.json"

CRS = "EPSG:28992"


# Todo implement processing only one dataset
# """Process all datasets from config file."""
# datasets = load_config()
# length = len(datasets)
# flag = 0
#
# for dataset in datasets:
#     flag += 1
#     dataset_name = dataset.get('name', 'unknown')
#     if dataset_name == name:
#         logger.info(f"Processing dataset: {dataset_name}")
#         try:
#             # Download data
#             response = retrieve_data(dataset['download_link'])
#
#             # Parse data
#             df = self.parse_data(response, dataset['file_type'])
#             logger.info(f"Parsed {len(df)} records")
#
#             # Standardize data
#             df_standardized = self.standardize_data(df, dataset)
#
#             # Save data
#             self.save_data(df_standardized, dataset_name)
#
#         except Exception as e:
#             logger.error(f"Failed to process {dataset_name}: {e}")
#             return False
#
#         logger.info(f"Reran {dataset_name} successfully")
#         return True
#
#     else:
#         if flag == length:
#             logger.warning(f"Dataset '{name}' not found in config")
#             return False
#         else:
#             continue


def convert_file(processor, dataset, dataset_name):

    # Download data
    response = processor.retrieve_data(dataset['download_link'])

    # Parse data
    gdf = processor.parse_data(response, dataset['file_type'])
    logger.info(f"Parsed {len(gdf)} records")

    # Standardize data todo: fix this function so that it standardizes the dataframe without fucking it
    # gdf_standardized = processor.standardize_data(gdf, dataset)
    gdf_standardized = gdf

    # Reproject
    gdf_standardized = gdf_standardized.to_crs(28992)
    # Remove rows where geometry is None or Empty
    gdf_standardized = gdf_standardized[~(gdf_standardized.geometry.is_empty | gdf_standardized.geometry.isna())]
    # Write as geoparquet file todo: put each parquet file in its own directory?
    dataset_path = f'{CONVERTED_DIRECTORY}{dataset_name}.parquet'
    gdf_standardized.to_parquet(dataset_path)

    return dataset_path

    # for file in os.listdir(RAW_DIRECTORY):
    #     full_path = os.path.join(RAW_DIRECTORY, file)
    #     filename, file_extension = os.path.splitext(file)
    #
    #     # If it is a directory, check inside for supported filetypes
    #     if isdir(full_path):
    #         for inner_file in os.listdir(full_path):
    #             inner_filename, inner_file_extension = os.path.splitext(inner_file)
    #             if inner_file_extension in SUPPORTED_TYPES:
    #                 full_path = os.path.join(full_path, inner_file)
    #                 file_extension = inner_file_extension
    #                 break

        # # Check that the file extension is supported by geoparquet-io
        # if file_extension in SUPPORTED_TYPES:
        #     print("Converting", file, "...")
        #
        #     gdf = gpd.read_file(full_path, use_arrow=True)
        #     # Reproject
        #     gdf = gdf.to_crs(28992)
        #     # Remove rows where geometry is None or Empty
        #     gdf = gdf[~(gdf.geometry.is_empty | gdf.geometry.isna())]
        #     # Write as geoparquet file
        #     gdf.to_parquet(f'{CONVERTED_DIRECTORY}{filename}.parquet')
        #
        # elif file_extension == ".csv":
        #     df = gpd.read_file(full_path, use_arrow=True)
        #     df['geometry'] = df['GEOM'].apply(wkt.loads)
        #     gdf = gpd.GeoDataFrame(df, crs='epsg:28992')
        #     path = Path(f'{CONVERTED_DIRECTORY}{filename}.parquet')
        #     gdf.to_parquet(path)
        # Move files that are already .parquet
        # elif file_extension == ".parquet":
        #     print(f"Already a parquet file: {file}, reprojecting and writing to nl_trees...")
        #     gdf = gpd.read_parquet(full_path)
        #     # Reproject
        #     gdf = gdf.to_crs(28992)
        #     path = Path(f'{CONVERTED_DIRECTORY}{filename}.parquet')
        #     gdf.to_parquet(path)

        # Error if unsupported filetype
        # else:
        #     print(f"the file {full_path} is not supported")


def add_space_filling_curve(dataset_path: str):
    logger.info("Adding bbox and performing hilbert sorting")
    table = gpio.read(dataset_path)
    table.add_bbox().sort_hilbert()
    table.write(dataset_path)


def validate(dataset_path: str):
    logger.info("Performing validation")

    validation_result = validate_geoparquet(dataset_path)
    print(f"PASSED = {validation_result.is_valid}")
    print(f"Passed {validation_result.passed_count}")
    print(f"Failed {validation_result.failed_count}")
    print(f"Warnings {validation_result.warning_count}")


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
    add_parser = subparsers.add_parser('convert', help='Convert all datasets described in the config file to parquet files')
    add_parser.add_argument('--config', default=CONFIG_PATH, help='Path to config file')

    # Convert one command
    add_parser = subparsers.add_parser('convert-one', help='Converts one dataset described in the config file to parquet files')
    add_parser.add_argument('--name', help='Name of dataset to convert', required=True)
    add_parser.add_argument('--config', default=CONFIG_PATH, help='Path to config file')

    #Todo: deploy to s3
    #Todo: generate STAC

    args = parser.parse_args()
    processor = DatasetDownloader(CONFIG_PATH, logger=logger)
    datasets = processor.config
    os.makedirs(CONVERTED_DIRECTORY, exist_ok=True)

    if args.command == 'convert':
        print("---- COMMENCING GEOPARQUET CONVERSION ----")
        for dataset in datasets:
            dataset_name = dataset.get('name', 'unknown')
            logger.info(f"Processing dataset: {dataset_name}")
            try:
                dataset_path = convert_file(processor, dataset, dataset_name)
                add_space_filling_curve(dataset_path)
                validate(dataset_path)
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
                dataset_path = convert_file(processor, dataset, dataset_name)
                add_space_filling_curve(dataset_path)
                validate(dataset_path)
            except Exception as e:
                logger.error(f"Failed to process {dataset_name}: {e}")
                continue
        sys.exit(0)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
