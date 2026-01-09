import json
import requests
import pandas as pd
from pathlib import Path
import argparse
import sys
import logging
from io import StringIO
from typing import Dict, Any#, List
# from collections import Counter
import geopandas as gpd
import io

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatasetDownloader:
    def __init__(self, config_path: str, output_dir: str = "standardized_data"):
        """
        Initialize the dataset downloader.

        Args:
            config_path: Path to JSON config file with dataset information
            output_dir: Directory to save standardized output files
        """
        self.config_path = config_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

    def load_config(self) -> list:
        """Load dataset configuration from JSON file."""
        with open(self.config_path, 'r') as f:
            return json.load(f)

    def retrieve_data(self, url: str) -> requests.Response:
        """
        Download data from URL.

        Args:
            url: API endpoint URL

        Returns:
            Response object from requests
        """
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            logger.info(f"Successfully retrieved data (Status: {response.status_code})")
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Error retrieving data from {url}: {e}")
            raise

    def parse_data(self, response: requests.Response, file_type: str):
        """
        Parse response data based on file type.

        Args:
            response: Response object from API
            file_type: Type of file (JSON, CSV, etc.)

        Returns:
            DataFrame containing parsed data
        """

        if file_type.upper() == "CSV":
            raise NotImplementedError("CSV file type parsing is not implemented in this snippet.")

        content = io.BytesIO(response.content)

        gdf = gpd.read_file(content)
        # print(gdf.columns)
    
        #save to geopackage
        # gpd.GeoDataFrame.to_file(gdf, self.output_dir / "output.gpkg", driver="GPKG")

        return gdf

        if file_type.upper() == "JSON":
            data = response.json()
            # Handle GeoJSON format
            if "features" in data:
                return pd.json_normalize(data["features"])
            return pd.json_normalize(data)
        elif file_type.upper() == "CSV":
            return pd.read_csv(StringIO(response.text))
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

    def standardize_data(self, df, dataset_info: Dict[str, Any]):
        """
        Standardize dataset by renaming columns

        Args:
            df: Raw DataFrame
            dataset_info: Dictionary with dataset metadata and column mappings

        Returns:
            Standardized DataFrame with only mapped columns
        """
        # Get column mappings if they exist
        column_mapping = dataset_info.get('column_mapping', {})

        if column_mapping:
            # Create reverse mapping (original -> standard)
            rename_dict = {}
            for standard_name, original_name in column_mapping.items():
                if standard_name == "Municipality":
                    continue
                elif original_name in df.columns:
                    rename_dict[original_name] = standard_name
                else:
                    logger.warning(f"Column '{original_name}' not found in dataset. Skipping.")

            # Rename columns
            df = df.rename(columns=rename_dict)

            # Keep only the standardized columns that exist
            standard_columns = [col for col in column_mapping.keys() if col in df.columns]
            df = df[standard_columns]
            df['Municipality'] = column_mapping['Municipality']

            # If lat - lon came from the same column, they were merged and need to be split up again
            has_lat = "Lat" in df.columns
            has_lon = "Lon" in df.columns

            if not (has_lat and has_lon):
                # Look for a column containing POINT(lat lon)
                point_col = next(
                    (col for col in df.columns if df[col].astype(str).str.contains("POINT", na=False).any()),
                    None
                )

                if point_col is None:
                    raise ValueError("No Lat/Lon columns and no POINT column found.")

                logger.info(f"Splitting lat/lon from column '{point_col}'")

                # Extract coordinates
                coords = df[point_col].str.extract(
                    r"POINT\s*\(\s*([-0-9\.]+)\s+([-0-9\.]+)\s*\)"
                )

                df["Lon"] = coords[0]
                df["Lat"] = coords[1]

            logger.info(f"Standardized columns: {', '.join(df.columns)}")

            logger.info(f"Standardized {len(standard_columns)} columns: {', '.join(standard_columns)}")

        return df

    def save_data(self, df, dataset_name: str):
        """
        Save standardized data to file.

        Args:
            df: Standardized DataFrame
            dataset_name: Name for output file
        """

        geopackage_path = self.output_dir / f"{dataset_name}_standardized.gpkg"
        gpd.GeoDataFrame.to_file(df, geopackage_path, driver="GPKG")

        # csv_path = self.output_dir / f"{dataset_name}_standardized.csv"
        # df.to_csv(csv_path, index=False)
        # logger.info(f"Saved CSV version to {csv_path}")

    def process_all_datasets(self):
        """Process all datasets from config file."""
        datasets = self.load_config()

        for dataset in datasets:
            dataset_name = dataset.get('name', 'unknown')
            logger.info(f"Processing dataset: {dataset_name}")

            try:
                # Download data
                response = self.retrieve_data(dataset['download_link'])

                # Parse data
                df = self.parse_data(response, dataset['file_type'])
                logger.info(f"Parsed {len(df)} records")

                # Standardize data
                df_standardized = self.standardize_data(df, dataset)

                # Save data
                self.save_data(df_standardized, dataset_name)

            except Exception as e:
                logger.error(f"Failed to process {dataset_name}: {e}")
                continue


def create_example_config():
    """Create an example configuration file."""
    config = [
        {
            "name": "Groningen",
            "data_owner": "Groningen (Gemeente)",
            "email_address": "opendata@groningen.nl",
            "update_frequency": "Monthly",
            "language": "Dutch",
            "primary_source": "https://data.groningen.nl/dataset/bomen-gemeente-groningen",
            "download_link": "https://maps.groningen.nl/geoserver/geo-data/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=geo-data:Bomen%20gemeente%20Groningen&maxFeatures=1000000&outputFormat=application/json&srsName=EPSG:4326&format_options=id_policy:reference_no=false",
            "file_type": "JSON",
            "crs": "EPSG:4326",
            "column_mapping": {
                "Municipality": "Groningen",
                "Lon": "properties.LON",
                "Lat": "properties.LAT",
                "Latin_name": "properties.LATIJNSE_NAAM",
                "Height": "properties.BOOMHOOGTE",
                "Year_of_planting": "properties.KIEMJAAR"
            }
        },
        {
            "name": "Dronten",
            "data_owner": "Dronten (Gemeente)",
            "email_address": "data@dronten.nl",
            "update_frequency": None,
            "language": "Dutch",
            "primary_source": "http://data.overheid.nl/dataset/bomenkaart-dronten",
            "download_link": "https://nedgeoservices.nedgraphicscs.nl/geoserver/ows?service=WFS&version=1.1.0&request=GetFeature&typeName=topp:O10002_Bomenkaart_OD&outputFormat=csv",
            "file_type": "CSV",
            "crs": "EPSG:4326",
            "column_mapping": {
                "Municipality": "Dronten",
                "Lon": "lon",
                "Lat": "lat",
                "Latin_name": "latijnse_naam",
                "Height": "hoogte",
                "Year_of_planting": "plantjaar"
            }
        },
        {
            "name": "Eindhoven",
            "data_owner": "Eindhoven (Gemeente)",
            "email_address": "data@eindhoven.nl",
            "update_frequency": "Quarterly",
            "language": "Dutch",
            "primary_source": "https://data.eindhoven.nl/datasets/bomen",
            "download_link": "https://data.eindhoven.nl/api/v2/catalog/datasets/bomen/exports/json",
            "file_type": "JSON",
            "crs": "EPSG:4326",
            "column_mapping": {
                "Municipality": "Eindhoven",
                "Lon": "geo_point_2d.lon",
                "Lat": "geo_point_2d.lat",
                "Latin_name": "boomsoort",
                "Height": "hoogte",
                "Year_of_planting": "plantjaar"
            }
        },

        {
            "name": "Nijmegen",
            "data_owner": "Nijmegen (Gemeente)",
            "email_address": "opendata@nijmegen.nl",
            "update_frequency": None,
            "language": "Dutch",
            "primary_source": "https://opendata.nijmegen.nl/dataset/bomen",
            "download_link": "https://services.nijmegen.nl/geoservices/extern_BOR_Groen/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=extern_BOR_Groen%3AGRN_BOMEN&outputFormat=csv",
            "file_type": "CSV",
            "crs": "EPSG:28992",
            "column_mapping": {
                "Municipality": "Nijmegen",
                "Lon": "GEOMETRIE",
                "Lat": "GEOMETRIE",
                "Latin_name": "BOOMSOORT",
                "Height": "HOOGTE_EXACT",
                "Year_of_planting": "PLANTJAAR"
            }
        }
    ]

    with open("datasets_config.json", "w", encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

    logger.info("Created example config file: datasets_config.json")


def add_dataset_programmatically(
        config_path: str,
        name: str,
        data_owner: str,
        download_link: str,
        file_type: str,
        lon_column: str,
        lat_column: str,
        latin_name_column: str,
        height_column: str,
        year_of_planting_column: str,
        email_address: str = "",
        update_frequency: str = None,
        language: str = "Dutch",
        primary_source: str = "",
        crs: str = "EPSG:4326",
) -> bool:
    """
    Add a dataset to config file programmatically.

    Args:
        config_path: Path to JSON config file
        name: Dataset name
        data_owner: Organization owning the data
        download_link: API URL to download data
        file_type: Type of file (JSON or CSV)
        lon_column: The name of the column containing the Longitude value,
        lat_column: The name of the column containing the Latitude value,
        latin_name_column: The name of the column containing the Latin name,
        height_column: The name of the column containing the height,
        year_of_planting_column: The name of the column containing the year of planting,
        email_address: Contact email
        update_frequency: How often data is updated
        language: Language of dataset
        primary_source: Primary source URL
        crs: Coordinate reference system

    Returns:
        True if successful, False otherwise
    """
    dataset = {
        "name": name,
        "data_owner": data_owner,
        "email_address": email_address,
        "update_frequency": update_frequency,
        "language": language,
        "primary_source": primary_source,
        "download_link": download_link,
        "file_type": file_type.upper(),
        "crs": crs,
        "column_mapping": {
            "Municipality": name,
            "Lon": lon_column,
            "Lat": lat_column,
            "Latin_name": latin_name_column,
            "Height": height_column,
            "Year_of_planting": year_of_planting_column
        }
    }

    # Load existing config or create new one
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except FileNotFoundError:
        config = []

    # Check for duplicates
    config = [d for d in config if d.get('name') != name]

    # Add new dataset
    config.append(dataset)

    # Save updated config
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

    logger.info(f"Successfully added dataset '{name}' to {config_path}")
    return True


def add_dataset_to_config(config_path: str = "datasets_config.json"):
    """
    Interactive function to add a new dataset to the config file.

    Args:
        config_path: Path to the JSON config file
    """
    print("\n=== Add New Dataset to Config ===\n")

    # Collect dataset information
    dataset = {
        "name": input("Dataset name (e.g., 'Amsterdam'): ").strip(),
        "data_owner": input("Data owner (e.g., 'Amsterdam (Gemeente)'): ").strip(),
        "email_address": input("Email address: ").strip(),
        "update_frequency": input("Update frequency (e.g., 'Monthly', 'Weekly', or leave empty): ").strip() or None,
        "language": input("Language (e.g., 'Dutch'): ").strip(),
        "primary_source": input("Primary source URL: ").strip(),
        "download_link": input("Download link (API URL): ").strip(),
        "file_type": input("File type (JSON or CSV): ").strip().upper(),
        "crs": input("CRS (e.g., 'EPSG:4326'): ").strip(),
        "column_mapping": {
          "Municipality": input("Municipality name (e.g., 'Amsterdam'): ").strip(),
          "Lon": input("The name of the column containing the Longitude value (e.g., 'LON, Y_coordinate', 'GEOM'): ").strip(),
          "Lat": input("The name of the column containing the Latitude value (e.g., 'LAT, X_coordinate', 'GEOM'): ").strip(),
          "Latin_name": input("The name of the column containing the Latin name (e.g., 'Latijnse_naam', 'boomsoort'): ").strip(),
          "Height": input("The name of the column containing the height (e.g., 'Hoogte', 'Boomhoogte'): ").strip(),
          "Year_of_planting": input("The name of the column containing the year of planting (e.g., 'Kiemjaar','Plantjaar'): ").strip()
        }
    }

    # Validate required fields
    if not all([dataset["name"], dataset["download_link"], dataset["file_type"]]):
        logger.error("Name, download_link, and file_type are required fields!")
        return False

    elif not all([dataset["column_mapping"]["Municipality"], dataset["column_mapping"]["Lon"],
                  dataset["column_mapping"]["Lat"], dataset["column_mapping"]["Latin_name"],
                  dataset["column_mapping"]["Height"], dataset["column_mapping"]["Year_of_planting"]]):
        logger.error("All the different columns need to be mapped properly")
        return False

    # Load existing config or create new one
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except FileNotFoundError:
        config = []
        logger.info(f"Config file not found. Creating new one at {config_path}")

    # Check for duplicate names
    if any(d.get('name') == dataset['name'] for d in config):
        overwrite = input(f"\nDataset '{dataset['name']}' already exists. Overwrite? (yes/no): ").lower()
        if overwrite == 'yes':
            config = [d for d in config if d.get('name') != dataset['name']]
        else:
            logger.info("Dataset not added.")
            return False

    # Add new dataset
    config.append(dataset)

    # Save updated config
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

    logger.info(f"Successfully added dataset '{dataset['name']}' to {config_path}")
    return True


def add_datasets_from_json(
        input_json_path: str,
        config_path: str = "datasets_config.json",
        overwrite_duplicates: bool = False
) -> int:
    """
    Add dataset(s) from a JSON file to the config file.

    Args:
        input_json_path: Path to JSON file containing dataset(s) to add
        config_path: Path to the main config file
        overwrite_duplicates: If True, overwrite existing datasets with same name

    Returns:
        Number of datasets added
    """
    # Load datasets from input file
    try:
        with open(input_json_path, 'r', encoding='utf-8') as f:
            new_datasets = json.load(f)
    except FileNotFoundError:
        logger.error(f"Input file not found: {input_json_path}")
        return 0
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in input file: {e}")
        return 0

    # Ensure new_datasets is a list
    if isinstance(new_datasets, dict):
        new_datasets = [new_datasets]
    elif not isinstance(new_datasets, list):
        logger.error("Input JSON must be a dictionary or list of dictionaries")
        return 0

    # Validate each dataset
    required_fields = ["name", "download_link", "file_type"]
    valid_datasets = []

    for i, dataset in enumerate(new_datasets):
        if not isinstance(dataset, dict):
            logger.warning(f"Skipping item {i}: not a dictionary")
            continue

        missing_fields = [field for field in required_fields if field not in dataset]
        if missing_fields:
            logger.warning(f"Skipping dataset {dataset.get('name', 'unknown')}: missing fields {missing_fields}")
            continue

        # Ensure file_type is uppercase
        dataset['file_type'] = dataset['file_type'].upper()
        valid_datasets.append(dataset)

    if not valid_datasets:
        logger.error("No valid datasets found in input file")
        return 0

    # Load existing config or create new one
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except FileNotFoundError:
        config = []
        logger.info(f"Config file not found. Creating new one at {config_path}")

    # Process each valid dataset
    added_count = 0
    existing_names = {d.get('name') for d in config}

    for dataset in valid_datasets:
        name = dataset['name']

        if name in existing_names:
            if overwrite_duplicates:
                config = [d for d in config if d.get('name') != name]
                config.append(dataset)
                logger.info(f"Overwrote existing dataset: {name}")
                added_count += 1
            else:
                logger.warning(f"Skipping duplicate dataset: {name} (use --overwrite to replace)")
        else:
            config.append(dataset)
            logger.info(f"Added new dataset: {name}")
            added_count += 1

    # Save updated config
    if added_count > 0:
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        logger.info(f"Successfully added {added_count} dataset(s) to {config_path}")

    return added_count


def list_datasets(config_path: str = "datasets_config.json"):
    """List all datasets in the config file."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        if not config:
            print("No datasets in config file.")
            return

        print(f"\n=== Datasets in {config_path} ===\n")
        for i, dataset in enumerate(config, 1):
            print(f"{i}. {dataset.get('name', 'Unknown')}")
            print(f"   Owner: {dataset.get('data_owner', 'N/A')}")
            print(f"   Type: {dataset.get('file_type', 'N/A')}")
            print(f"   Update: {dataset.get('update_frequency', 'N/A')}")
            print()
    except FileNotFoundError:
        print(f"Config file not found: {config_path}")
    except json.JSONDecodeError:
        print(f"Invalid JSON in config file: {config_path}")


def remove_dataset(name: str, config_path: str = "datasets_config.json") -> bool:
    """Remove a dataset from the config file by name."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except FileNotFoundError:
        logger.error(f"Config file not found: {config_path}")
        return False

    original_length = len(config)
    config = [d for d in config if d.get('name') != name]

    if len(config) == original_length:
        logger.warning(f"Dataset '{name}' not found in config")
        return False

    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

    logger.info(f"Removed dataset '{name}' from {config_path}")
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Dataset processor for downloading and standardizing datasets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
    Examples:
      # Process all datasets in config
      python retrieve_data.py process

      # Add datasets from a JSON file
      python retrieve_data.py add input_datasets.json

      # Add datasets and overwrite duplicates
      python retrieve_data.py add input_datasets.json --overwrite

      # Add single dataset interactively
      python retrieve_data.py add-interactive

      # List all datasets in config
      python retrieve_data.py list

      # Remove a dataset
      python retrieve_data.py remove "Amsterdam"

      # Create example config
      python retrieve_data.py create-example
            """
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # Process command
    process_parser = subparsers.add_parser('process', help='Process all datasets in config')
    process_parser.add_argument('--config', default='datasets_config.json', help='Path to config file')
    process_parser.add_argument('--output-dir', default='standardized_data', help='Output directory')

    # Add from JSON command
    add_parser = subparsers.add_parser('add', help='Add datasets from JSON file')
    add_parser.add_argument('input_file', help='Path to JSON file with dataset(s)')
    add_parser.add_argument('--config', default='datasets_config.json', help='Path to config file')
    add_parser.add_argument('--overwrite', action='store_true', help='Overwrite duplicate datasets')

    # Add interactive command
    interactive_parser = subparsers.add_parser('add-interactive', help='Add dataset interactively')
    interactive_parser.add_argument('--config', default='datasets_config.json', help='Path to config file')

    # List command
    list_parser = subparsers.add_parser('list', help='List all datasets in config')
    list_parser.add_argument('--config', default='datasets_config.json', help='Path to config file')

    # Remove command
    remove_parser = subparsers.add_parser('remove', help='Remove dataset from config')
    remove_parser.add_argument('name', help='Name of dataset to remove')
    remove_parser.add_argument('--config', default='datasets_config.json', help='Path to config file')

    # Create example command
    example_parser = subparsers.add_parser('create-example', help='Create example config file')

    args = parser.parse_args()

    if args.command == 'process':
        processor = DatasetDownloader(args.config, args.output_dir)
        processor.process_all_datasets()

    elif args.command == 'add':
        count = add_datasets_from_json(args.input_file, args.config, args.overwrite)
        if count > 0:
            print(f"\nSuccessfully added {count} dataset(s)")
        sys.exit(0 if count > 0 else 1)

    elif args.command == 'add-interactive':
        add_dataset_to_config(args.config)

    elif args.command == 'list':
        list_datasets(args.config)

    elif args.command == 'remove':
        success = remove_dataset(args.name, args.config)
        sys.exit(0 if success else 1)

    elif args.command == 'create-example':
        create_example_config()

    else:
        parser.print_help()


    # Create example config file
    create_example_config()

    # Process all datasets
    processor = DatasetDownloader("datasets_config.json")
    processor.process_all_datasets()