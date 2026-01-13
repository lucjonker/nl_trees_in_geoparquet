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

        if file_type.upper() == "CSV":
            content = StringIO(response.text)
            gdf = gpd.read_file(content, driver="CSV")
            # print(gdf.head())

        else:
            content = io.BytesIO(response.content)
            gdf = gpd.read_file(content)

            return gdf

    # Todo: Fix standardize data to output valid geodataframes (is it still broken?)
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

        if column_mapping:
            # Create reverse mapping (original -> standard)
            rename_dict = {}
            for standard_name, original_name in column_mapping.items():
                if standard_name == "Municipality":
                    continue
                elif original_name in gdf.columns:
                    rename_dict[original_name] = standard_name
                else:
                    self.logger.warning(f"Column '{original_name}' not found in dataset. Skipping.")

            # Rename columns
            gdf = gdf.rename(columns=rename_dict)

            # Keep only the standardized columns that exist
            standard_columns = [col for col in column_mapping.keys() if col in gdf.columns]
            gdf = gdf[standard_columns]
            gdf['Municipality'] = column_mapping['Municipality']

            # If lat - lon came from the same column, they were merged and need to be split up again
            has_lat = "Lat" in gdf.columns
            has_lon = "Lon" in gdf.columns

            if not (has_lat and has_lon):
                # Look for a column containing POINT(lat lon)
                point_col = next(
                    (col for col in gdf.columns if gdf[col].astype(str).str.contains("POINT", na=False).any()),
                    None
                )

                if point_col is None:
                    raise ValueError("No Lat/Lon columns and no POINT column found.")

                self.logger.info(f"Splitting lat/lon from column '{point_col}'")

                # Extract coordinates
                coords = gdf[point_col].str.extract(
                    r"POINT\s*\(\s*([-0-9\.]+)\s+([-0-9\.]+)\s*\)"
                )

                gdf["Lon"] = coords[0]
                gdf["Lat"] = coords[1]

            self.logger.info(f"Standardized columns: {', '.join(gdf.columns)}")

            self.logger.info(f"Standardized {len(standard_columns)} columns: {', '.join(standard_columns)}")

        return gdf
