import io
import json
import requests
import geopandas as gpd
import logging
from io import StringIO
from typing import Dict, Any  # , List


class DatasetDownloader:
    # Todo remove output path stuff
    def __init__(self, config_path: str, logger: logging.Logger):
        """
        Initialize the dataset downloader.

        Args:
            config_path: Path to JSON config file with dataset information
        """
        self.config = self.load_config(config_path)
        self.logger = logger

    def load_config(self, config_path) -> list:
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

    def parse_data(self, response: requests.Response, file_type: str):
        """
        Parse response data based on file type.

        Args:
            response: Response object from API
            file_type: Type of file (JSON, CSV, etc.)

        Returns:
            DataFrame containing parsed data
        """

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

        if file_type.upper() == "CSV":
            content = StringIO(response.text)
            gdf = gpd.read_file(content, driver="CSV")
            # print(gdf.head())

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
        # Get column mappings if they exist
        column_mapping = dataset_info.get('column_mapping', {})
        values = column_mapping.values()

        rename = {v: k for k, v in column_mapping.items()}
        drop = []

        # For each column
        for column in gdf.columns:
            # If said column is not in our column mapping
            if column not in values:
                # Make sure not to drop the active geometry column
                if column != gdf.geometry.name:
                    drop.append(column)

        #Todo: include metadata in returned geodataframe (data owner, email address, etc)
        standardized = gdf.rename(columns=rename)
        standardized.drop(columns=drop, inplace=True)

        return standardized
