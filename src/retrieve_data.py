import io
import json

import pandas as pd
import requests
import geopandas as gpd
import logging
from io import StringIO
from typing import Dict, Any  # , List

from pandas.io.sas.sas_constants import dataset_length, dataset_offset
from shapely import wkt


class DatasetDownloader:
    # Todo remove output path stuff
    def __init__(self, config_path: str, template_path: str, logger: logging.Logger):
        """
        Initialize the dataset downloader.

        Args:
            config_path: Path to JSON config file with dataset information
        """
        self.config = self.load_json(config_path)
        self.template = self.load_json(template_path)
        self.logger = logger

    def load_json(self, config_path):
        """Load dataset configuration from JSON file."""
        with open(config_path, 'r') as f:
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
            self.logger.info(f"Successfully retrieved data (Status: {response.status_code})")
            return response
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error retrieving data from {url}: {e}")
            raise

    def parse_data(self, response: requests.Response, dataset_info: Dict[str, Any]):
        """
        Parse response data based on file type.

        Args:
            response: Response object from API
            dataset_info: Dictionary with dataset metadata and column mappings

        Returns:
            DataFrame containing parsed data
        """
        file_type = dataset_info.get('file_type', None)

        if file_type.upper() == "CSV":
            content = StringIO(response.text)
            geometry_column = dataset_info.get('geometry_column', None)
            lat_column = dataset_info.get('lat_column', None)
            lon_column = dataset_info.get('lon_column', None)
            crs = dataset_info.get('crs', None)

            if geometry_column:
                df = pd.read_csv(content)
                df['geometry'] = df[geometry_column].apply(wkt.loads)
                gdf = gpd.GeoDataFrame(df, crs=crs)
                return gdf
            elif lat_column and lon_column:
                df = pd.read_csv(content)
                gdf = gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_xy(df[lon_column], df[lat_column]), crs=crs)
                return gdf
            else:
                self.logger.error(f"CSV file missing required metadata (lat/lon columns or geometrycolumn)")
                return None

        elif file_type.upper() == "PARQUET":
            content = io.BytesIO(response.content)
            gdf = gpd.read_parquet(content)
            return gdf
        else:
            content = io.BytesIO(response.content)
            gdf = gpd.read_file(content)

            return gdf

    def standardize_data(self, gdf: gpd.GeoDataFrame, dataset_info: Dict[str, Any]) -> gpd.GeoDataFrame:
        """
        Standardize dataset by renaming columns

        Args:
            gdf: Raw DataFrame
            dataset_info: Dictionary with dataset metadata and column mappings

        Returns:
            Standardized DataFrame with only mapped columns
        """
        # Get column mappings
        dataset_column_mapping = dataset_info.get('column_mapping', {})
        if not dataset_column_mapping:
            self.logger.error(f"No column mapping found for dataset {dataset_info}")
            return gdf
        # Get template column mapping
        template_column_mapping = self.template.get('column_mapping', {})
        if not template_column_mapping:
            self.logger.error(f"No template column mappings found")
            return gdf
        # Get dataset name
        dataset_name = dataset_info.get('name', None)
        if not dataset_name:
            self.logger.error(f"No dataset name found for dataset {dataset_info}")
            return gdf

        dataset_values = dataset_column_mapping.values()
        template_keys = template_column_mapping.keys()

        rename = {v: k for k, v in dataset_column_mapping.items()}
        drop = []

        metadata = dataset_info.get('metadata', {})

        # For each column
        for column in gdf.columns:
            # If said column is not in our column mapping
            if column not in dataset_values:
                # Make sure not to drop the active geometry column
                if column != gdf.geometry.name:
                    drop.append(column)

        standardized = gdf.rename(columns=rename)
        standardized.drop(columns=drop, inplace=True)

        # Add parquet metadata
        for key, value in metadata.items():
            standardized[key] = value

        # Add missing converted columns
        for key in template_keys:
            if key not in dataset_column_mapping.keys():
                self.logger.warning(f"No column mapping found for column {key}, inserting null...")
                standardized[key] = "N/A"

        # Override geometry column name
        if standardized.active_geometry_name != 'geometry':
            standardized.rename_geometry('geometry', inplace=True)

        return standardized
