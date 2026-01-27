import argparse
import json
import os
import sys
from os.path import exists
import csv

from main import CONFIG_PATH

def clean(value):
        """Standardizes strings: 'None', empty, or whitespace become 'none'."""
        string = str(value).strip() if value else ""
        return string if string and string.lower() != "none" else "none"

def create_config_from_sheet(config_path):
    """Shortened and refactored creator for datasets_config.json."""
    map_path = r"../data/config/Tree-datasets(Column_mapping).csv"
    data_path = r"../data/config/Tree-datasets(Datasets).csv"

    # 1. Load Metadata from Datasets CSV
    metadata = {}
    with open(data_path, 'r', encoding='latin-1') as f:
        for r in csv.DictReader(f, delimiter=';'):
            if r.get('Name') and r.get('File_type') != 'ERROR':
                metadata[r['Name'].strip()] = r

    # 2. Process Mappings CSV
    config = []
    for name, info in metadata.items():
        # Search the Mapping CSV for a row that matches this dataset name
        mapping_row = None
        with open(map_path, 'r', encoding='latin-1') as f:
            map_reader = csv.reader(f, delimiter=';')
            next(map_reader)  # Skip header
            for row in map_reader:
                if row and row[0].strip() == name:
                    mapping_row = row
                    break
    
            entry = {
                "name": name.replace(" ", "_"),
                "file_type": info['File_type'].upper(),
                "metadata": {
                    "data_owner": info['Data_owner'],
                    "email_address": clean(info['Email_adress']),
                    "language": info['Language'],
                    "primary_source": info['Primary_source'],
                    "download_link": info['Download_link']
                },
                "column_mapping": {
                    "Latin_name": clean(mapping_row[3]) if mapping_row else None,
                    "Height": clean(mapping_row[13]) if mapping_row else None,
                    "Year_of_planting": clean(mapping_row[4]) if mapping_row else None,
                    "Trunk_diameter": clean(mapping_row[5]) if mapping_row else None
                }
            }

            if entry["file_type"] == "CSV":
                continue
                #TODO: implement lat/lon/geometry column handling

            config.append(entry)

    # 3. Save resulting configuration
    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    with open(config_path, "w", encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

    print(f"Successfully created {config_path} with {len(config)} datasets.")

def create_example_config():
    """Create an example configuration file."""
    config = [
        {
            "name": "The_Hague",
            "file_type": "SHP",
            "metadata": {
                "data_owner": "Gemeente 's-Gravenhage (Gemeente)",
                "email_address": "none",
                "language": "Dutch",
                "primary_source": "https://data.overheid.nl/dataset/313fa20b-7608-447d-8882-3ae2b989bc5d",
                "download_link": "https://ckan.dataplatform.nl/dataset/2487aed5-961b-4add-ab88-2400fa6901f6/resource/038fc5b4-c086-4de1-99a1-9284685e0e6d/download/bomen-shape.zip",
            },
            "column_mapping": {
                "Latin_name": "SRT_WETENS",
                "Trunk_diameter": "STAMDIAMET"
            }
        },
        {
            "name": "Hilversum",
            "file_type": "JSON",
            "metadata": {
                "data_owner": "Hilversum (Gemeente)",
                "email_address": "none",
                "language": "Dutch",
                "primary_source": "https://open-hilversum.hub.arcgis.com/datasets/Hilversum::bomen/about",
                "download_link": "https://services-eu1.arcgis.com/YcscSJ1XstO6iVhP/arcgis/rest/services/Bomen_8b628/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
            },
            "column_mapping": {
                "Latin_name": "SOORTNAAM",
                "Height": "BOOMHOOGTEKLASSEACTUEEL",
                "Year_of_planting": "JAARVANAANLEG",
                "Trunk_diameter": "STAMDIAMETERKLASSE"
            }
        },
        {
            "name": "Utrecht",
            "file_type": "JSON",
            "metadata": {
                "data_owner": "Utrecht (Gemeente)",
                "email_address": "none",
                "language": "Dutch",
                "primary_source": "https://data.overheid.nl/dataset/utrecht-bomenkaart-update-2024#panel-resources",
                "download_link": "https://arcgis.com/sharing/rest/content/items/7e2404cf7fba4bb087935f9cdb51f053/data",
            },
            "column_mapping": {
                "Latin_name": "Wetenschappelijke_naam",
                "Year_of_planting": "Plantjaar",
            }
        },
        {
            "name": "Delft",
            "file_type": "JSON",
            "metadata": {
                "data_owner": "Delft (Gemeente)",
                "email_address": "none",
                "language": "Dutch",
                "primary_source": "https://data.delft.nl/datasets/bomen-in-beheer-door-gemeente-delft-1/about",
                "download_link": "https://hub.arcgis.com/api/v3/datasets/d83a50486b384bfe8038c2d762f5e628_0/downloads/data?format=geojson&spatialRefId=4326&where=1=1",
            },
            "column_mapping": {
                "Latin_name": "BOOMSORTIMENT",
                "Height": "HOOGTE",
                "Year_of_planting": "AANLEGJAAR",
                "Trunk_diameter": "DIAMETER"
            }
        },
        {
            "name": "Amsterdam",
            "file_type": "JSON",
            "metadata": {
                "data_owner": "Amsterdam (Gemeente)",
                "email_address": "data-helpdesk@amsterdam.nl",
                "language": "Dutch",
                "primary_source": "https://api.data.amsterdam.nl/v1/bomen/v1/stamgegevens",
                "download_link": "https://api.data.amsterdam.nl/v1/bomen/v1/stamgegevens?_format=geojson",
            },
            "column_mapping": {
                "Latin_name": "soortnaam",
                "Height": "boomhoogteklasseActueel",
                "Year_of_planting": "jaarVanAanleg",
                "Trunk_diameter": "none"
            }
        },
        {
            "name": "Groningen",
            "file_type": "JSON",
            "metadata": {
                "data_owner": "Groningen (Gemeente)",
                "email_address": "opendata@groningen.nl",
                "language": "Dutch",
                "primary_source": "https://data.groningen.nl/dataset/bomen-gemeente-groningen",
                "download_link": "https://maps.groningen.nl/geoserver/geo-data/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=geo-data:Bomen%20gemeente%20Groningen&maxFeatures=1000000&outputFormat=application/json&srsName=EPSG:4326&format_options=id_policy:reference_no=false"
            },
            "column_mapping": {
                "Latin_name": "LATIJNSE_NAAM",
                "Height": "BOOMHOOGTE",
                "Year_of_planting": "KIEMJAAR",
                "Trunk_diameter": "none"
            }
        },
        {
            "name": "Dronten",
            "file_type": "CSV",
            "lat_column": "LAT",
            "lon_column": "LON",
            "CRS": "ESPG:4326",
            "metadata": {
                "data_owner": "Dronten (Gemeente)",
                "email_address": "data@dronten.nl",
                "language": "Dutch",
                "primary_source": "http://data.overheid.nl/dataset/bomenkaart-dronten",
                "download_link": "https://nedgeoservices.nedgraphicscs.nl/geoserver/ows?service=WFS&version=1.1.0&request=GetFeature&typeName=topp:O10002_Bomenkaart_OD&outputFormat=csv"
            },
            "column_mapping": {
                "Latin_name": "latijnse_naam",
                "Height": "hoogte",
                "Year_of_planting": "plantjaar",
                "Trunk_diameter": "none"
            }
        },
        {
            "name": "Eindhoven",
            "file_type": "PARQUET",
            "metadata": {
                "data_owner": "Eindhoven (Gemeente)",
                "email_address": "data@eindhoven.nl",
                "language": "Dutch",
                "primary_source": "https://data.eindhoven.nl/explore/dataset/bomen/information/?disjunctive.beheerder&disjunctive.boomsoort&disjunctive.hoogte&disjunctive.eigenaar&disjunctive.eindbeeld&disjunctive.boomsoort_nederlands&disjunctive.status_ter_indicatie",
                "download_link": "https://data.eindhoven.nl/api/explore/v2.1//catalog/datasets/bomen/exports/parquet"
            },
            "column_mapping": {
                "Latin_name": "boomsoort",
                "Height": "hoogte",
                "Year_of_planting": "plantjaar",
                "Trunk_diameter": "none"
            }
        },
        {
            "name": "Nijmegen",
            "file_type": "CSV",
            "geometry_column": "GEOMETRIE",
            "crs": "EPSG:28992",
            "metadata": {
                "data_owner": "Eindhoven (Gemeente)",
                "email_address": "opendata@nijmegen.nl",
                "language": "Dutch",
                "primary_source": "https://opendata.nijmegen.nl/dataset/bomen",
                "download_link": "https://services.nijmegen.nl/geoservices/extern_BOR_Groen/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=extern_BOR_Groen%3AGRN_BOMEN&outputFormat=csv"
            },
            "column_mapping": {
                "Latin_name": "BOOMSOORT",
                "Height": "HOOGTE_EXACT",
                "Year_of_planting": "PLANTJAAR",
                "Trunk_diameter": "STAMDIAMETER"
            }
        }
    ]

    #make sure CONFIG_PATH exists
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)

    with open(CONFIG_PATH, "w", encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

    print("Created example config file: datasets_config.json")


def add_dataset_programmatically(
        config_path: str,
        name: str,
        data_owner: str,
        download_link: str,
        file_type: str,
        lon_column: str,
        lat_column: str,
        geometry_column: str,
        latin_name_column: str,
        height_column: str,
        year_of_planting_column: str,
        Trunk_diameter_column: str,
        email_address: str = "",
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
        lon_column: The name of the column containing the Longitude value (In the case of a CSV),
        lat_column: The name of the column containing the Latitude value (In the case of a CSV),
        geometry_column: The name of the column containing the geometry (In the case of a CSV),
        crs: Coordinate reference system (In the case of a CSV)
        latin_name_column: The name of the column containing the Latin name,
        height_column: The name of the column containing the height,
        year_of_planting_column: The name of the column containing the year of planting,
        Trunk_diameter_column: The name of the column containing the trunk diameter,
        email_address: Contact email
        language: Language of dataset
        primary_source: Primary source URL

    Returns:
        True if successful, False otherwise
    """
    dataset = {
        "name": name,
        "file_type": file_type.upper(),
        "metadata": {
            "data_owner": data_owner,
            "email_address": email_address,
            "language": language,
            "primary_source": primary_source,
            "download_link": download_link,
        },
        "column_mapping": {
            "Latin_name": latin_name_column,
            "Height": height_column,
            "Year_of_planting": year_of_planting_column,
            "Trunk_diameter": Trunk_diameter_column
        }
    }

    if dataset["file_type"] == "CSV":
        dataset["crs"] = crs
        if geometry_column:
            dataset["geometry_column"] = geometry_column
        elif lat_column and lon_column:
            dataset["lat_column"] = lat_column
            dataset["lon_column"] = lon_column
        else:
            print("CSVs require either a geometry column or lat/lon column(s)")
            return False

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

    print(f"Successfully added dataset '{name}' to {config_path}")
    return True


def add_dataset_to_config(config_path: str = CONFIG_PATH):
    """
    Interactive function to add a new dataset to the config file.

    Args:
        config_path: Path to the JSON config file
    """
    print("\n=== Add New Dataset to Config ===\n")

    # Collect dataset information
    dataset = {
        "name": input("Dataset name (e.g., 'Amsterdam'): ").strip(),
        "file_type": input("File type (JSON or CSV): ").strip().upper(),
        "geometry_column": input(
            "The name of the column containing the geometry value, needed if you don't have lat/lon columns (e.g., 'GEOM', 'GEOMETRIE') (In the case of a CSV): ").strip(),
        "lon_column": input(
            "The name of the column containing the Longitude value, needed if you don't have a geometry column (e.g., 'LON, Y_coordinate') (In the case of a CSV): ").strip(),
        "lat_column": input(
            "The name of the column containing the Latitude value, needed if you don't have a geometry column (e.g., 'LAT, X_coordinate') (In the case of a CSV): ").strip(),
        "crs": input("CRS (e.g., 'EPSG:4326') (In the case of a CSV): ").strip(),
        "metadata": {
            "data_owner": input("Data owner (e.g., 'Amsterdam (Gemeente)'): ").strip(),
            "email_address": input("Email address: ").strip(),
            "language": input("Language (e.g., 'Dutch'): ").strip(),
            "primary_source": input("Primary source URL: ").strip(),
            "download_link": input("Download link (API URL): ").strip(),
        },
        "column_mapping": {
            "Latin_name": input("The name of the column containing the Latin name (e.g., 'Latijnse_naam', 'boomsoort'): ").strip(),
            "Height": input("The name of the column containing the height (e.g., 'Hoogte', 'Boomhoogte'): ").strip(),
            "Year_of_planting": input("The name of the column containing the year of planting (e.g., 'Kiemjaar','Plantjaar'): ").strip(),
            "Trunk_diameter": input("The name of the column containing the trunk diameter (e.g., 'Stamdiameter', 'Diameter'): ").strip(),
        }
    }

    #Todo: fix all of this with the new config format

    # # Validate required fields
    # if not all([dataset["name"], dataset["download_link"], dataset["file_type"]]):
    #     print("Name, download_link, and file_type are required fields!")
    #     return False
    #
    # elif not all([dataset["column_mapping"]["Municipality"], dataset["column_mapping"]["Lon"],
    #               dataset["column_mapping"]["Lat"], dataset["column_mapping"]["Latin_name"],
    #               dataset["column_mapping"]["Height"], dataset["column_mapping"]["Year_of_planting"]]):
    #     print("All the different columns need to be mapped properly")
    #     return False
    #
    # # Load existing config or create new one
    # try:
    #     with open(config_path, 'r', encoding='utf-8') as f:
    #         config = json.load(f)
    # except FileNotFoundError:
    #     config = []
    #     print(f"Config file not found. Creating new one at {config_path}")
    #
    # # Check for duplicate names
    # if any(d.get('name') == dataset['name'] for d in config):
    #     overwrite = input(f"\nDataset '{dataset['name']}' already exists. Overwrite? (yes/no): ").lower()
    #     if overwrite == 'yes':
    #         config = [d for d in config if d.get('name') != dataset['name']]
    #     else:
    #         print("Dataset not added.")
    #         return False
    #
    # # Add new dataset
    # config.append(dataset)
    #
    # # Save updated config
    # with open(config_path, 'w', encoding='utf-8') as f:
    #     json.dump(config, f, indent=2, ensure_ascii=False)
    #
    # print(f"Successfully added dataset '{dataset['name']}' to {config_path}")
    # return True


def add_datasets_from_json(
        input_json_path: str,
        config_path: str = CONFIG_PATH,
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
        print(f"Input file not found: {input_json_path}")
        return 0
    except json.JSONDecodeError as e:
        print(f"Invalid JSON in input file: {e}")
        return 0

    # Ensure new_datasets is a list
    if isinstance(new_datasets, dict):
        new_datasets = [new_datasets]
    elif not isinstance(new_datasets, list):
        print("Input JSON must be a dictionary or list of dictionaries")
        return 0

    # Validate each dataset
    valid_datasets = []
    
    for i, dataset in enumerate(new_datasets):
        if not isinstance(dataset, dict):
            print(f"Skipping item {i}: not a dictionary")
            continue
    
        if not (dataset.get("name") and dataset.get("file_type") and dataset.get("metadata", {}).get("download_link")):
            print(f"Skipping dataset {dataset.get('name', 'unknown')}: missing fields")
            continue
    
        # Ensure file_type is uppercase
        dataset['file_type'] = dataset['file_type'].upper()
        valid_datasets.append(dataset)
    
    if not valid_datasets:
        print("No valid datasets found in input file")
        return 0
    
    # Load existing config or create new one
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except FileNotFoundError:
        config = []
        print(f"Config file not found. Creating new one at {config_path}")
    
    # Process each valid dataset
    added_count = 0
    existing_names = {d.get('name') for d in config}
    
    for dataset in valid_datasets:
        name = dataset['name']
    
        if name in existing_names:
            if overwrite_duplicates:
                config = [d for d in config if d.get('name') != name]
                config.append(dataset)
                print(f"Overwrote existing dataset: {name}")
                added_count += 1
            else:
                print(f"Skipping duplicate dataset: {name} (use --overwrite to replace)")
        else:
            config.append(dataset)
            print(f"Added new dataset: {name}")
            added_count += 1
    
    # Save updated config
    if added_count > 0:
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        print(f"Successfully added {added_count} dataset(s) to {config_path}")
    
    return added_count


def list_datasets(config_path):
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
            print(f"   Type: {dataset.get('file_type', 'N/A')}")
            print(f"   Owner: {dataset.get('metadata', {}).get('data_owner', 'N/A')}")
            print(f"   Email: {dataset.get('metadata', {}).get('email_address', 'N/A')}")
            print(f"   Language: {dataset.get('metadata', {}).get('language', 'N/A')}")
            print(f"   Primary Source: {dataset.get('metadata', {}).get('primary_source', 'N/A')}")
            print(f"   Download Link: {dataset.get('metadata', {}).get('download_link', 'N/A')}")
            print(f"   Column Mapping: {dataset.get('column_mapping', {})}")
            print()
            
    except FileNotFoundError:
        print(f"Config file not found: {config_path}")
    except json.JSONDecodeError:
        print(f"Invalid JSON in config file: {config_path}")


def remove_dataset(name: str, config_path) -> bool:
    """Remove a dataset from the config file by name."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"Config file not found: {config_path}")
        return False
    
    original_length = len(config)
    config = [d for d in config if d.get('name') != name]
    
    if len(config) == original_length:
        print(f"Dataset '{name}' not found in config")
        return False
    
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    
    print(f"Removed dataset '{name}' from {config_path}")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Dataset processor for downloading and standardizing datasets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
    Examples:
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

    # Add from JSON command
    add_parser = subparsers.add_parser('add', help='Add datasets from JSON file')
    add_parser.add_argument('input_file', help='Path to JSON file with dataset(s)')
    add_parser.add_argument('--config', default=CONFIG_PATH, help='Path to config file')
    add_parser.add_argument('--overwrite', action='store_true', help='Overwrite duplicate datasets')

    # Add interactive command
    interactive_parser = subparsers.add_parser('add-interactive', help='Add dataset interactively')
    interactive_parser.add_argument('--config', default=CONFIG_PATH, help='Path to config file')

    # List command
    list_parser = subparsers.add_parser('list', help='List all datasets in config')
    list_parser.add_argument('--config', default=CONFIG_PATH, help='Path to config file')

    # Remove command
    remove_parser = subparsers.add_parser('remove', help='Remove dataset from config')
    remove_parser.add_argument('--name', help='Name of dataset to remove')
    remove_parser.add_argument('--config', default=CONFIG_PATH, help='Path to config file')

    # Create example command
    example_parser = subparsers.add_parser('create-example', help='Create example config file')

    # Read sheet command
    sheet_parser = subparsers.add_parser('read-sheet', help='Create config from CSV sheets')
    sheet_parser.add_argument('--config', default=CONFIG_PATH, help='Path to config file')

    args = parser.parse_args()

    if args.command == 'add':
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

    elif args.command == 'read-sheet':
        create_config_from_sheet(args.config)

    elif args.command == 'create-example':
        create_example_config()
    else:
        parser.print_help()
