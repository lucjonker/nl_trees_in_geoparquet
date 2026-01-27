import argparse
import json
import os
import logging
import sys
import geopandas as gpd
import pandas as pd
import tempfile
import warnings
import geoparquet_io as gpio
from geoparquet_io.core.validate import validate_geoparquet
from geoparquet_io.core.stac import generate_stac_item, generate_stac_collection

from retrieve_data import DatasetDownloader

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CONVERTED_DIRECTORY = "../data/nl_trees/"

CONFIG_PATH = "../data/config/datasets_config.json"

TEMPLATE_PATH = "../data/config/dataset_template.json"

DEFAULT_BUCKET = "s3://us-west-2.opendata.source.coop/roorda-tudelft/public-trees-in-nl/nl_trees"

LOCAL_DIR = "../data/local/"

CRS = "EPSG:28992"


def convert_file(processor, dataset, dataset_name, local_path=None):
    gdf = None
    file_path = local_path
    temp_file = None

    if local_path:
        file_path = local_path
    else:
        # Download data and save to temp file
        response = processor.retrieve_data(dataset['metadata']['download_link'])

        # Save to temp file to check for layers
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.tmp')
        temp_file.write(response.content)
        temp_file.close()
        file_path = temp_file.name

    # Try to check for multiple layers
    try:
        # Supress warnings of wrong file suffix (.tmp works)
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*non conformant file extension.*')
            layers = gpd.list_layers(file_path)

        if len(layers) > 1:
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*non conformant file extension.*')
                gdf = combine_multiple_layers(file_path, layers)
        else:
            gdf = gpd.read_file(file_path)

    except Exception as e:
        # Not a multi-layer format, read normally
        if local_path:
            gdf = gpd.read_file(local_path)
        else:
            # Parse using the processor's parse_data method
            response = processor.retrieve_data(dataset['metadata']['download_link'])
            gdf = processor.parse_data(response, dataset)

    # Clean up temp file if created
    if temp_file:
        os.unlink(temp_file.name)

    logger.info(f"Parsed {len(gdf)} records")

    # Standardize data
    gdf_standardized = processor.standardize_data(gdf, dataset)

    # Reproject
    gdf_standardized = gdf_standardized.to_crs(28992)
    # Remove rows where geometry is None or Empty
    gdf_standardized = gdf_standardized[~(gdf_standardized.geometry.is_empty | gdf_standardized.geometry.isna())]
    # Write as geoparquet file
    dataset_path = f'{CONVERTED_DIRECTORY}{dataset_name}/{dataset_name}.parquet'

    # If all of this worked, can make a directory and save
    os.makedirs(f'{CONVERTED_DIRECTORY}{dataset_name}/', exist_ok=True)
    gdf_standardized.to_parquet(dataset_path)

    return dataset_path


def combine_multiple_layers(file_path, layers):
    logger.info(f"Found {len(layers)} layers in file")
    # Read and combine multiple layers
    gdfs = []
    for layer_name in layers['name']:
        layer_gdf = gpd.read_file(file_path, layer=layer_name)
        logger.info(f"Layer '{layer_name}': {len(layer_gdf)} records")
        gdfs.append(layer_gdf)

    # Combine all layers into one GeoDataFrame
    gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))
    logger.info(f"Combined {len(layers)} layers into {len(gdf)} total records")

    return gdf

def add_space_filling_curve(dataset_path: str):
    logger.info("Adding bbox and performing hilbert sorting")
    table = gpio.read(dataset_path)
    table = table.add_bbox().sort_hilbert()
    table.write(dataset_path, compression='ZSTD', compression_level=15)


def validate(dataset_path: str):
    logger.info("Performing validation")

    validation_result = validate_geoparquet(dataset_path)
    logger.info(f"Validation passed: {validation_result.is_valid} | Num passed tests: {validation_result.passed_count} "
                f"| Num failed tests: {validation_result.failed_count} | Num warnings: {validation_result.warning_count}")


def generate_all_stac(base_directory: str, bucket: str):
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
            # generate_stac_item(
            #     parquet_file,
            #     bucket_prefix=bucket,
            # )
        else:
            logger.warning(f"Skipping {city}: No parquet file found at {parquet_file}")

    # 2. Generate the root Collection
    logger.info("Generating STAC Collection for all datasets...")

    # generate_stac_collection(
    #     partition_dir=base_directory,
    #     bucket_prefix=bucket,
    # )

    # 3. Validate
    # TODO: find out what the validate_stac function is called in version 0.8.0

def upload_to_s3(dataset_path: str, dataset_name: str, bucket: str):
    logger.info(f"Uploading: {dataset_name}")
    (gpio
     .read(dataset_path)
     .upload(f'{bucket}/{dataset_name}/{dataset_name}.parquet')
     )


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
    convert_parser = subparsers.add_parser('convert',
                                           help='Convert all datasets described in the config file to parquet files')
    convert_parser.add_argument('--config', default=CONFIG_PATH, help='Path to config file')
    convert_parser.add_argument('--template', default=TEMPLATE_PATH, help='Path to template file')

    # Convert local directory command
    convert_parser = subparsers.add_parser('convert-local-dir', help='Convert all files in a local directory to parquet files')
    convert_parser.add_argument('--local_dir', default=LOCAL_DIR, help='Path to local directory containing dataset files')
    convert_parser.add_argument('--config', default=CONFIG_PATH, help='Path to config file')
    convert_parser.add_argument('--template', default=TEMPLATE_PATH, help='Path to template file')

    # Convert one command
    convert_one_parser = subparsers.add_parser('convert-one',
                                               help='Converts one dataset described in the config file to parquet files')
    convert_one_parser.add_argument('--name', help='Name of dataset to convert', required=True)
    convert_one_parser.add_argument('--config', default=CONFIG_PATH, help='Path to config file')
    convert_one_parser.add_argument('--template', default=TEMPLATE_PATH, help='Path to template file')
    convert_one_parser.add_argument('--local_path', help='Local path to dataset if you already have one downloaded')

    # Push parquet files to remote
    upload_parser = subparsers.add_parser('upload', help='Push parquet files to remote S3/Source.Coop')
    upload_parser.add_argument('--config', default=CONFIG_PATH, help='Path to config file')
    upload_parser.add_argument('--bucket', default=DEFAULT_BUCKET, help='Target S3 URI (e.g., s3://bucket-name/path/)')

    # Push parquet one file to remote
    upload_one_parser = subparsers.add_parser('upload-one', help='Push one parquet file to remote S3/Source.Coop')
    upload_one_parser.add_argument('--name', help='Name of dataset to upload', required=True)
    upload_one_parser.add_argument('--config', default=CONFIG_PATH, help='Path to config file')
    upload_one_parser.add_argument('--bucket', default=DEFAULT_BUCKET,
                                   help='Target S3 URI (e.g., s3://bucket-name/path/)')

    # Generate STAC command
    stac_parser = subparsers.add_parser('stac', help='Generate STAC Items and Collection for existing parquet files')
    stac_parser.add_argument('--bucket', default=DEFAULT_BUCKET, help='Target S3 URI (e.g., s3://bucket-name/path/)')

    args = parser.parse_args()

    if args.command == 'convert-local-dir':
        local_dir = args.local_dir
        processor = DatasetDownloader(args.config, args.template, logger=logger)
        datasets = processor.config

        for file_name in os.listdir(local_dir):
            dataset_name = str.capitalize(file_name.split('.')[0])
            local_path = os.path.join(local_dir, file_name)

            matching_datasets = [d for d in datasets if str.capitalize(d.get('name')) == dataset_name]
            
            if not matching_datasets:
                logger.warning(f"No config entry found for {dataset_name} (from {file_name}). Skipping.")
                continue

            for dataset in matching_datasets:
                logger.info(f"Processing: {dataset_name} ({file_name})")
                try:
                    process_dataset(dataset, dataset_name, processor, local_path)
                except Exception as e:
                    logger.error(f"Failed to process {dataset_name}: {e}")
        sys.exit(0)

    if args.command == 'convert':
        config_path = args.config
        template_path = args.template
        processor = DatasetDownloader(config_path, template_path, logger=logger)
        datasets = processor.config

        print("---- COMMENCING GEOPARQUET CONVERSION ----")
        for dataset in datasets:
            print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
            dataset_name = str.capitalize(dataset.get('name', 'unknown'))
            logger.info(f"Processing dataset: {dataset_name}")
            try:
                process_dataset(dataset, dataset_name, processor)
            except Exception as e:
                logger.error(f"Failed to process {dataset_name}: {e}")
                continue
        sys.exit(0)
    elif args.command == 'convert-one':
        config_path = args.config
        template_path = args.template
        local_path = args.local_path
        processor = DatasetDownloader(config_path, template_path, logger=logger)
        datasets = processor.config
        dataset_name = str.capitalize(args.name)

        print(f"---- COMMENCING GEOPARQUET CONVERSION FOR {dataset_name} ----")

        for dataset in [d for d in datasets if str.capitalize(d.get('name')) == dataset_name]:
            logger.info(f"Processing dataset: {dataset_name}")
            try:
                process_dataset(dataset, dataset_name, processor, local_path)
            except Exception as e:
                logger.error(f"Failed to process {dataset_name}: {e}")
                continue
        sys.exit(0)
    elif args.command == 'upload':
        print(f"---- COMMENCING UPLOAD TO {args.bucket} ----")
        # Todo: This currently assumes you have already exported the bucket credentials, might have to automate?
        config_path = args.config
        datasets = None
        with open(config_path, 'r') as f:
            datasets = json.load(f)

        for dataset in datasets:
            print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
            dataset_name = dataset.get('name', 'unknown')
            try:
                dataset_path = f"{CONVERTED_DIRECTORY}{dataset_name}/{dataset_name}.parquet"
                upload_to_s3(dataset_path, dataset_name, args.bucket)
            except Exception as e:
                logger.error(f"Failed to upload {dataset_name}: {e}")
                continue
        sys.exit(0)
    elif args.command == 'upload-one':
        name = str.capitalize(args.name)
        print(f"---- COMMENCING UPLOAD TO {args.bucket} for {name} ----")

        config_path = args.config
        datasets = None
        with open(config_path, 'r') as f:
            datasets = json.load(f)

        for dataset in [d for d in datasets if str.capitalize(d.get('name')) == name]:
            dataset_name = dataset.get('name', 'unknown')
            try:
                dataset_path = f"{CONVERTED_DIRECTORY}{dataset_name}/{dataset_name}.parquet"
                upload_to_s3(dataset_path, dataset_name, args.bucket)
            except Exception as e:
                logger.error(f"Failed to upload {dataset_name}: {e}")
                continue
        sys.exit(0)
    elif args.command == 'stac':
        print("---- COMMENCING STAC METADATA GENERATION ----")
        bucket = args.bucket
        generate_all_stac(CONVERTED_DIRECTORY, bucket)
        sys.exit(0)
    else:
        parser.print_help()


def process_dataset(dataset, dataset_name, processor: DatasetDownloader, local_path=None):
    dataset_path = convert_file(processor, dataset, dataset_name, local_path)
    add_space_filling_curve(dataset_path)
    validate(dataset_path)


if __name__ == "__main__":
    main()
    # TODO: should we add a small demo of querying the data for some arbitrary bbox?
