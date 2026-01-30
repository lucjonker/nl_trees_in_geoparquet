import os
import zipfile
import boto3
import duckdb
import json
import sys
import jsonschema as js


import jsonschema as js
import json
import sys

def validate_config(config_path, schema_path, logger):
    """Validates configuration file against JSON schema and logs specific city errors."""
    with open(config_path, 'r') as f:
        config_data = json.load(f)
    with open(schema_path, 'r') as f:
        schema = json.load(f)
        
    try:
        js.validate(instance=config_data, schema=schema)
        logger.info("Configuration validation passed.")
    except js.exceptions.ValidationError as e:
        # e.absolute_path is a deque. The first item is the index in the list.
        # We can use this to find the specific city name in your config.
        path = list(e.absolute_path)
        
        error_location = "Root"
        city_info = "Unknown City"
        
        if path:
            index = path[0]
            # Try to get the name of the city that failed
            city_name = config_data[index].get('name', f'Index {index}')
            city_info = f"City: {city_name}"
            # The rest of the path tells you the specific field (e.g., 'metadata', 'download_link')
            error_location = " -> ".join(str(p) for p in path)

        logger.error(f"--- Configuration Validation Failed ---")
        logger.error(f"Location: {error_location}")
        logger.error(f"Context: {city_info}")
        logger.error(f"Error Message: {e.message}")
        logger.error(f"---------------------------------------")
        sys.exit(1)

def calculate_file_size(file_path):
    """Returns file size in Mb."""
    try:
        size_bytes = os.path.getsize(file_path)
        return round(size_bytes / (1024 * 1024), 2)
    except Exception:
        return 0

def compare_file_size(logger, dataset_name: str, raw_size_mb_all_columns: float, standardized_size_mb: float, dataset_path: str):
    final_size_mb = calculate_file_size(dataset_path)
    logger.info(f"Raw size with all columns {raw_size_mb_all_columns} MB | Raw Size: {standardized_size_mb} MB | Final Size: {final_size_mb} MB")

    with open("conversion_stats.csv", "a") as f:
        f.write(f"{dataset_name},{raw_size_mb_all_columns},{standardized_size_mb},{final_size_mb}\n")

def unzip_dir(directory_path):
    for filename in os.listdir(directory_path):
        if filename.endswith(".zip"):
            full_path = os.path.join(directory_path, filename)
            extract_to = full_path.replace('.zip', '')

            print(f"Extracting {filename} to {extract_to}...")

            with zipfile.ZipFile(full_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)

def delete_s3_item(bucket_name, object_name):
    s3 = boto3.client('s3')
    result = s3.list_objects(Bucket=bucket_name, Prefix="roorda-tudelft/public-trees-in-nl/", Delimiter='/')
    for o in result.get('CommonPrefixes'):
        if o.get('Prefix') == object_name:
            print(f"Deleting {o.get('Prefix')}")
            response = s3.delete_object(Bucket=bucket_name, Key=object_name)
            print(response)

def download_bbox_from_s3(bucket_name, output_path, xmin, xmax, ymin, ymax):
    # Create a connection to db file
    with duckdb.connect() as con:
        # Initialize
        con.install_extension("spatial")
        con.load_extension("spatial")
        # con.execute("SET s3_region='us-west-2';")
        # Download results from s3 and save to parquet file
        con.execute(f"""
            COPY (SELECT * FROM read_parquet('{bucket_name}/*/*.parquet', union_by_name=True) WHERE bbox.xmin > {xmin} AND bbox.xmax < {xmax} AND bbox.ymin > {ymin} AND bbox.ymax < {ymax}) 
            TO '{output_path}' (FORMAT parquet);
        """)