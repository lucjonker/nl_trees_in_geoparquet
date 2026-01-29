import argparse
import json
import os
import logging
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd
import tempfile
import warnings
import geoparquet_io as gpio
from geoparquet_io.core.validate import validate_geoparquet
from geoparquet_io.core.upload import upload
from geoparquet_io.core.stac import generate_stac_item, generate_stac_collection, write_stac_json

from retrieve_data import DatasetDownloader
from utils import calculate_file_size, compare_file_size

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CONVERTED_DIRECTORY = "../data/nl_trees_2/"

SIZE_DIRECTORY = "../data/original_datasets/"

CONFIG_PATH = "../data/config/datasets_config.json"

TEMPLATE_PATH = "../data/config/dataset_template.json"

DEFAULT_BUCKET = "s3://us-west-2.opendata.source.coop/roorda-tudelft/public-trees-in-nl/nl_trees_2"
DEFAULT_PUBLIC_URL = "https://data.source.coop/roorda-tudelft/public-trees-in-nl/nl_trees_2"

LOCAL_DIR = "../data/local/"

CRS = 4326

def convert_file(processor, dataset, dataset_name, record_size=False):
    gdf = None

    raw_size_mb_all_columns = None
    standardized_size_mb = None

    local_path = dataset.get('local_path')
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
    
    if record_size:
        raw_size_mb_all_columns = calculate_file_size(file_path)

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

    except Exception:
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
    gdf_standardized = gdf_standardized.to_crs(CRS)
    # Remove rows where geometry is None or Empty
    gdf_standardized = gdf_standardized[~(gdf_standardized.geometry.is_empty | gdf_standardized.geometry.isna())]
    # Write as geoparquet file
    dataset_path = f'{CONVERTED_DIRECTORY}{dataset_name}/{dataset_name}.parquet'

    # If all of this worked, can make a directory and save
    os.makedirs(f'{CONVERTED_DIRECTORY}{dataset_name}/', exist_ok=True)
    gdf_standardized.to_parquet(dataset_path)
    
    if record_size:
        #also save to original file type for measuring size
        driver_map = {"SHP": "ESRI Shapefile", "JSON": "GeoJSON", "GPKG": "GPKG", "PARQUET": "Parquet", "CSV": "CSV"}
        driver = driver_map.get(dataset["file_type"].upper())
        original_file_path = f'{SIZE_DIRECTORY}{dataset_name}/{dataset_name}_original.{dataset["file_type"].lower()}'
        os.makedirs(f'{SIZE_DIRECTORY}{dataset_name}/', exist_ok=True)


        if driver == "Parquet":
            return 0,0, dataset_path
        
        elif driver == "CSV":
            gdf_standardized.to_csv(original_file_path, index=False)
            standardized_size_mb = calculate_file_size(original_file_path)
            return raw_size_mb_all_columns, standardized_size_mb, dataset_path

        gdf_standardized.to_file(original_file_path, driver=driver)

        if driver == "ESRI Shapefile":
            raw_size_mb_all_columns = 0
            standardized_size_mb = 0
        else:
            standardized_size_mb = calculate_file_size(original_file_path)

    return raw_size_mb_all_columns, standardized_size_mb, dataset_path


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


def add_space_filling_curve_and_bbox(dataset_path: str):
    logger.info("Adding bbox and performing hilbert sorting")
    table = gpio.read(dataset_path)
    table = table.add_bbox().sort_hilbert()
    table.write(dataset_path, compression='ZSTD', compression_level=15)


def validate(dataset_path: str):
    logger.info("Performing validation")

    validation_result = validate_geoparquet(dataset_path)
    logger.info(f"Validation passed: {validation_result.is_valid} | Num passed tests: {validation_result.passed_count} "
                f"| Num failed tests: {validation_result.failed_count} | Num warnings: {validation_result.warning_count}")


def generate_all_stac(base_directory: str, bucket: str, single_dataset: str, up: bool):
    """
    Scans the data directory, generates an Item for each city subfolder,
    and then generates a root Collection.
    """

    # 1. Generate Items for each sub-folder (City)
    # We look for directories inside nl_trees_2
    subdirs = [d for d in os.listdir(base_directory)
               if os.path.isdir(os.path.join(base_directory, d))]

    for city in subdirs:
        if single_dataset and single_dataset != str.capitalize(city):
            continue

        city_path = os.path.join(base_directory, city)
        parquet_file = os.path.join(city_path, f"{city}.parquet")

        if os.path.exists(parquet_file):
            logger.info(f"Generating STAC Item for: {city}")
            # Todo: Public url is hardcoded at the moment, if another bucket is used it'd be nice to make this an argument
            item = generate_stac_item(
                parquet_file,
                bucket_prefix=bucket,
                public_url=f"{DEFAULT_PUBLIC_URL}/{city}/",
            )
            stac_path = os.path.join(city_path, f"{city}.json")
            write_stac_json(item, stac_path)

            logger.info("Uploading STAC file to S3")
            if up:
                upload(Path(stac_path), bucket + f"/{city}/{city}.json")
        else:
            logger.warning(f"Skipping {city}: No parquet file found at {parquet_file}")

    # 2. Generate the root Collection
    logger.info("Generating STAC Collection for all datasets...")

    collection = generate_stac_collection(
        verbose=True,
        partition_dir=base_directory,
        public_url=DEFAULT_PUBLIC_URL,
        bucket_prefix=bucket
    )[0]

    # Hack for partitioned files since it doesn't add the correct links
    for item in collection['links']:
        if item['rel'] == 'item':
            item_href = item['href']
            base, filename = item_href.rsplit('/', 1)
            city = filename.removesuffix('.json')
            item_href = f"{base}/{city}/{filename}"
            item['href'] = item_href

    collection_path = os.path.join(base_directory, "collection.json")
    write_stac_json(collection, collection_path)
    if up:
        upload(Path(collection_path), bucket + "/collection.json")


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
      Convert all datasets in the provided config file to parquet files
      > python main.py convert
      
      Convert one dataset in the provided config file to parquet files
      > python main.py convert --single_dataset <dataset_name>
      
      Upload all datasets in the provided config file to S3
      > python main.py upload --bucket <bucket>
      
      Generate STAC for generatet parquet files, and then upload to S3
      > python main.py stac --bucket <bucket> --upload
            """
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # Convert all command
    convert_parser = subparsers.add_parser('convert',
                                           help='Convert all datasets described in the config file to parquet files')
    convert_parser.add_argument('--config', default=CONFIG_PATH, help='Path to config file')
    convert_parser.add_argument('--template', default=TEMPLATE_PATH, help='Path to template file')
    convert_parser.add_argument('--single_dataset', help='Name of the one dataset to convert')
    convert_parser.add_argument('--record_size', help='Whether to record file sizes to CSV (y/n)', default='n')

    # Push parquet files to remote
    upload_parser = subparsers.add_parser('upload', help='Push parquet files to remote S3/Source.Coop')
    upload_parser.add_argument('--config', default=CONFIG_PATH, help='Path to config file')
    upload_parser.add_argument('--single_dataset', help='Name of the one dataset to upload')
    upload_parser.add_argument('--bucket', default=DEFAULT_BUCKET, help='Target S3 URI (e.g., s3://bucket-name/path/)')

    # Generate STAC command
    stac_parser = subparsers.add_parser('stac', help='Generate STAC Items and Collection for existing parquet files')
    stac_parser.add_argument('--bucket', default=DEFAULT_BUCKET, help='Target S3 URI (e.g., s3://bucket-name/path/)')
    stac_parser.add_argument('--single_dataset', help='Name of the one dataset to convert')
    stac_parser.add_argument('--upload', action=argparse.BooleanOptionalAction,
                             help='Whether to upload parquet files to provided S3 or not')

    args = parser.parse_args()

    if args.command == 'convert':
        config_path = args.config
        template_path = args.template
        processor = DatasetDownloader(config_path, template_path, logger=logger)
        datasets = processor.config
        record_size = args.record_size.lower() == 'y'

        name = args.single_dataset
        single_dataset = None

        if name:
            single_dataset = str.capitalize(args.single_dataset)

        print("---- COMMENCING GEOPARQUET CONVERSION ----")
        for dataset in datasets:
            # If converting one, skip the unneeded ones
            if single_dataset and not str.capitalize(dataset.get('name')) == single_dataset:
                continue

            print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
            dataset_name = dataset.get('name', 'unknown')
            logger.info(f"Processing dataset: {dataset_name}")
            try:
                process_dataset(dataset, dataset_name, processor, record_size)
            except Exception as e:
                logger.error(f"Failed to process {dataset_name}: {e}")
                continue
        sys.exit(0)
    elif args.command == 'upload':
        print(f"---- COMMENCING UPLOAD TO {args.bucket} ----")
        # Todo: This currently assumes you have already exported the bucket credentials in your terminal
        config_path = args.config
        datasets = None

        name = args.single_dataset
        single_dataset = None
        if name:
            single_dataset = str.capitalize(args.single_dataset)

        with open(config_path, 'r') as f:
            datasets = json.load(f)

        for dataset in datasets:
            # If uploading one, skip the unneeded ones
            if single_dataset and not str.capitalize(dataset.get('name')) == single_dataset:
                continue
            print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
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
        name = args.single_dataset
        single_dataset = None

        if name:
            single_dataset = str.capitalize(args.single_dataset)
        bucket = args.bucket
        upload = args.upload
        generate_all_stac(CONVERTED_DIRECTORY, bucket, single_dataset, upload)
        sys.exit(0)
    else:
        parser.print_help()


def process_dataset(dataset, dataset_name, processor: DatasetDownloader, record_size: bool = False ):
    raw_size_mb_all_columns, standardized_size_mb, dataset_path = convert_file(processor, dataset, dataset_name, record_size)
    add_space_filling_curve_and_bbox(dataset_path)
    validate(dataset_path)

    if record_size:
        compare_file_size(logger,dataset_name, raw_size_mb_all_columns, standardized_size_mb, dataset_path)

if __name__ == "__main__":
    main()
    # TODO: should we add a small demo of querying the data for some arbitrary bbox?
