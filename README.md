# Dutch Public Tree Dataset 🌳

This Python repository contains a Python pipeline for downloading, standardizing, and publishing geospatial tree datasets from various Dutch municipalities. The pipeline converts datasets to GeoParquet format, generates STAC metadata, and uploads it to cloud storage.

## Main Features
The main features of the repository are:

- **Multi-format Support**: Handles (GEO)JSON, CSV, GeoParquet, GeoPackage
- **Standardization**: Converts diverse datasets to a unified schema
- **GeoParquet Output**: Efficient columnar format with spatial indexing (Hilbert curve)
- **STAC Metadata**: Generates STAC Items and Collections for dataset discovery
- **Cloud Publishing**: Uploads to S3-compatible storage (Source Cooperative)
- **Validation**: Built-in GeoParquet validation
---

## Project Structure 📁

```
.
├── src/
│   ├── main.py                     # Main pipeline script
│   ├── config_setup.py             # Dataset configuration management
│   ├── retrieve_data.py            # Data retrieval module (DatasetDownloader)
│   ├── utils.py                    # Functions used by other .py files                   
├── data/
│   ├── config/
│   │   ├── datasets_config.json    # Dataset configuration file
│   │   └── dataset_template.json   # Template for new datasets
│   ├── nl_trees/                   # Output directory for converted files
│   │   ├── amsterdam/
│   │   │   ├── amsterdam.parquet
│   │   │   └── amsterdam.json      # STAC Item
│   │   └── collection.json         # STAC Collection
│   └── local/                      # Local file storage
```
---
## Overview

When maintaining the dataset, two files are important:

- **`main.py`** - Is the core pipeline for data conversion, validation, and publishing
- **`config_setup.py`** - Is used for editing, adding and/or removing datasets

---

## Installation ⚙️
Clone the repository and install dependencies:

```bash
git clone https://github.com/lucjonker/nl_trees_in_geoparquet
cd nl_trees_in_geoparquet
pip install -r requirements.txt
```

---

## Dataset Configuration - `config_setup.py` 

When starting a new dataset or when updating the current dataset, the first step is to use the Dataset configuration methods found in `config_setup.py`. This file provides function that make it possible to add, remove or edit the datasets that are defined in the `datasets_config.json` file. The datasets in this file will be processed and added using the functions in `main.py`, explained later in this README file.

The program requires certain information in order to function. These are: `name`, `file_type`, `download_link`, and `column_mapping`. The download link can also be a path to a local file. Other metadata is also requested to make the dataset more accessible and tracable for other users. The standard input JSON format is (see also`data\config\dataset_template.json`):
```json
{
  "name": "YourCity",
  "file_type": "File type",
  "metadata": {
      "data_owner": "City (Gemeente)",
      "email_address": "contact@city.nl",
      "language": "Dutch",
      "primary_source": "https://data.city.nl/bomen",
      "download_link": "https://api.city.nl/bomen"
  },
  "column_mapping": {
    "Latin_name": "original_species_column",
    "Height": "original_height_column",
    "Year_of_planting": "original_year_column",
    "Trunk_diameter": "original_diameter_column"
  }
}
```

The pipeline currently standardizes the following attributes:

| Standard Column | Description | Example Source Columns |
|----------------|-------------|----------------------|
| `Latin_name` | Scientific species name | `soortnaam_lat`, `species_latin`, `Latijnse_naam` |
| `Height` | Tree height | `boomhoogte`, `height_m`, `Hoogte` |
| `Year_of_planting` | Year planted | `plantjaar`, `plant_year`, `Kiemjaar` |
| `Trunk_diameter` | Trunk diameter | `stamdiameter`, `diameter_cm`, `Diameter` |

Set column to `"none"` if not available in source dataset. When desired, it is ofcourse possible to add extra attributes to the `column_mapping` list. It is required however to do this for all the datasets in the `datasets_config.json` file in order to function properly. It is therefore extra important to provide the relevant metadata in order to provide the possibility of other users to find and add new features.

---

### Commands

#### 1. Add Datasets from JSON

It is possible to create a JSON file containing multiple mapped datasets in order to bulk import the datasets into the configurations file.

```bash
# Add datasets from file
python config_setup.py add input_datasets.json

# Overwrite existing datasets with same name
python config_setup.py add input_datasets.json --overwrite

# Use custom config path
python config_setup.py add input_datasets.json --config path/to/config.json
```



#### 2. Add Dataset Interactively

It is also possible using python to interactively add a new dataset. This will guide you to the process of collecting information. Prompts are created asking the user to input all the relevant information. 

```bash
python config_setup.py add-interactive
```

#### 3. Add dataset programmatically
When preferred, it is also possible to call the `add_dataset_programmatically` function directly in Python and inputting the parameters into the function
```python
from config_setup import add_dataset_programmatically

add_dataset_programmatically(
    config_path="data/config/datasets_config.json",
    name="Rotterdam",
    data_owner="Rotterdam Municipality",
    download_link="https://api.rotterdam.nl/trees",
    file_type="JSON",
    lon_column="none",
    lat_column="none",
    geometry_column="geometry",
    latin_name_column="species_latin",
    height_column="height_m",
    year_of_planting_column="plant_year",
    Trunk_diameter_column="diameter_cm",
    email_address="opendata@rotterdam.nl",
    language="Dutch",
    primary_source="https://rotterdam.nl/opendata",
    crs="EPSG:4326"
)
```


#### 4. List All Datasets

To see which datasets are currently in the `datasets_config.json` file, you can run the following command:

```bash
python config_setup.py list

# Use custom config
python config_setup.py list --config path/to/config.json
```

#### 5. Remove a Dataset

To remove a dataset that is currently in the `datasets_config.json` file, you can run the following command:

```bash
python config_setup.py remove --name Amsterdam

# Use custom config
python config_setup.py remove --name Amsterdam --config path/to/config.json
```
---

## Data Processing Pipeline - `main.py` 
The main data processing pipeline uses the `datasets_config.json` file and either downloads the relevant data from configured URLs or reads local files. It checks if the file has multiple layers and combines them into one if so. The program then standardizes column names and geometry based on the mapping and reprojects the points to WGS84 (EPSG:4326). All invalid geometries are then removed, a spatial index is added (Hilbert Curve) and the file is transformed into a GeoParquet format and validated.


### Commands

#### 1. Convert Datasets to GeoParquet

This command converts all geospatial tree datasets found in `datasets_config.json` to a standardized GeoParquet format.

```bash
# Convert all datasets
python main.py convert

# Convert single dataset
python main.py convert --single_dataset Amsterdam

# Use custom config
python main.py convert --config path/to/config.json
```

#### 2. Upload to Cloud Storage

It is possible for the converted GeoParquet files to be uploaded to a S3-compatible storage. 

**Prerequisites:**
- AWS credentials exported in terminal
- Write access to target bucket

```bash
# Upload all datasets
python main.py upload --bucket s3://bucket-name/path/

# Upload single dataset
python main.py upload --single_dataset Amsterdam --bucket s3://bucket-name/path/
```



#### 3. Generate STAC Metadata

Creates STAC (SpatioTemporal Asset Catalog) metadata.

```bash
# Generate STAC without uploading
python main.py stac --bucket s3://bucket-name/path/

# Generate and upload STAC
python main.py stac --bucket s3://bucket-name/path/ --upload

# Generate for single dataset
python main.py stac --single_dataset Amsterdam --bucket s3://bucket-name/path/ --upload
```

**Output:**
- Individual STAC Items for each city (`{city}.json`)
- Root STAC Collection (`collection.json`)

### Configuration

Default paths in `main.py`:

```python
CONVERTED_DIRECTORY = "../data/nl_trees_2/"
CONFIG_PATH = "../data/config/datasets_config.json"
TEMPLATE_PATH = "../data/config/dataset_template.json"
DEFAULT_BUCKET = "s3://us-west-2.opendata.source.coop/roorda-tudelft/public-trees-in-nl/nl_trees_2"
CRS = 4326  # WGS84
```
---


## Workflow Example

### Complete Pipeline for New Dataset

```bash
# 1. Add dataset configuration
python config_setup.py add-interactive

# 2. Convert to GeoParquet
python main.py convert --single_dataset YourCity

# 3. Upload to S3
python main.py upload --single_dataset YourCity --bucket s3://your-bucket/path/

# 4. Generate and upload STAC metadata
python main.py stac --single_dataset YourCity --bucket s3://your-bucket/path/ --upload
```

### Bulk Processing

```bash
# 1. Prepare JSON with multiple datasets
cat > new_datasets.json << EOF
[
  {"name": "Utrecht", "file_type": "JSON", ...},
  {"name": "Eindhoven", "file_type": "CSV", ...}
]
EOF

# 2. Add all datasets
python config_setup.py add new_datasets.json --overwrite

# 3. Convert all
python main.py convert

# 4. Upload all
python main.py upload --bucket s3://your-bucket/path/

# 5. Generate STAC for all
python main.py stac --bucket s3://your-bucket/path/ --upload
```

---

## Output Format

### GeoParquet Files

Each dataset produces a GeoParquet file with:
- Standardized schema (`Latin_name`, `Height`, `Year_of_planting`, `Trunk_diameter`)
- WGS84 projection (EPSG:4326)
- Hilbert curve spatial indexing
- ZSTD compression (level 15)
- Valid geometries only

### STAC Metadata

**Item format** (`{city}.json`):
```json
{
  "type": "Feature",
  "stac_version": "1.0.0",
  "id": "amsterdam",
  "geometry": {...},
  "properties": {
    "datetime": "2026-01-28T00:00:00Z"
  },
  "assets": {
    "data": {
      "href": "https://data.source.coop/.../amsterdam.parquet",
      "type": "application/vnd.apache.parquet"
    }
  }
}
```

**Collection format** (`collection.json`):
- Links to all dataset items
- Includes total spatial and temporal extent of all files combined
- Descriptive metadata

---


## Error Handling

The pipeline includes error handling:

- Skips datasets that fail processing
- Logs errors for debugging
- Continues processing remaining datasets
- Validates output files
- Cleans up temporary files

Check logs for detailed error messages:
```
2026-01-28 10:00:00 - INFO - Processing dataset: Amsterdam
2026-01-28 10:00:05 - ERROR - Failed to process Rotterdam: Invalid geometry
```

---

## Limitations

- Requires internet connection for API downloads (unless using local files)
- S3 upload requires appropriate credentials
- Multi-layer files must have compatible schemas
- CSV files need explicit CRS specification

---

## Support

For issues or questions:
1. Check logs for error messages
2. Validate dataset configuration format
3. Verify data source URLs are accessible
4. Ensure required columns exist in source data

