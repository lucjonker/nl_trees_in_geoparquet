import os
from os.path import isdir

import geoparquet_io as gpio
from geoparquet_io.cli.main import check_all
from geoparquet_io.core.validate import validate_geoparquet


import geopandas as gpd
from pathlib import Path

from shapely import wkt

SUPPORTED_TYPES = {
    ".geojson",
    ".json",
    ".shp",
    ".gpkg",
    ".gdb",
}

RAW_DIRECTORY = "../data/raw/"

CONVERTED_DIRECTORY = "../data/converted/"

CRS = "EPSG:28992"

def convert_files():
    for file in os.listdir(RAW_DIRECTORY):
        full_path = os.path.join(RAW_DIRECTORY, file)
        filename, file_extension = os.path.splitext(file)

        # If it is a directory, check inside for supported filetypes
        if isdir(full_path):
            for inner_file in os.listdir(full_path):
                inner_filename, inner_file_extension = os.path.splitext(inner_file)
                if inner_file_extension in SUPPORTED_TYPES:
                    full_path = os.path.join(full_path, inner_file)
                    file_extension = inner_file_extension
                    break

        # Check that the file extension is supported by geoparquet-io
        if file_extension in SUPPORTED_TYPES:
            print("Converting", file, "...")

            #todo: with csv or tsv files we need to know the geometry column smh
            # # Convert CSV with WKT geometry
            # table = gpio.convert('data.csv', wkt_column='geometry')
            # # Convert CSV with lat/lon columns
            # table = gpio.convert('data.csv', lat_column='latitude', lon_column='longitude')
            # table = gpio.convert(full_path, skip_invalid=True).add_bbox().sort_hilbert()
            # table.write(f'{CONVERTED_DIRECTORY}{filename}.parquet')

            gdf = gpd.read_file(full_path, use_arrow=True)
            # Reproject
            gdf = gdf.to_crs(28992)
            # Remove rows where geometry is None or Empty
            gdf = gdf[~(gdf.geometry.is_empty | gdf.geometry.isna())]
            # Write as geoparquet file
            gdf.to_parquet(f'{CONVERTED_DIRECTORY}{filename}.parquet')

        elif file_extension == ".csv":
            print("TRYNA WRITE A CSV FUCCCCC")
            df = gpd.read_file(full_path, use_arrow=True)
            df['geometry'] = df['GEOM'].apply(wkt.loads)
            gdf = gpd.GeoDataFrame(df, crs='epsg:28992')
            path = Path(f'{CONVERTED_DIRECTORY}{filename}.parquet')
            gdf.to_parquet(path
                           )
        # Move files that are already .parquet
        # elif file_extension == ".parquet":
        #     print(f"Already a parquet file: {file}, reprojecting and writing to converted...")
        #     gdf = gpd.read_parquet(full_path)
        #     # Reproject
        #     gdf = gdf.to_crs(28992)
        #     path = Path(f'{CONVERTED_DIRECTORY}{filename}.parquet')
        #     gdf.to_parquet(path)

        # Error if unsupported filetype
        else:
            print(f"the file {full_path} is not supported")

def add_space_filling_curve():
    for file in os.listdir(CONVERTED_DIRECTORY):
        print(f"Adding bbox and performing hilbert sorting for file:", file, "...")
        full_path = os.path.join(CONVERTED_DIRECTORY, file)

        table = gpio.read(full_path)
        table.add_bbox().sort_hilbert()
        table.write(full_path)

def validate():
    for file in os.listdir(CONVERTED_DIRECTORY):
        print(f"Performing validation for file:", file, "...")
        full_path = os.path.join(CONVERTED_DIRECTORY, file)

        validation_result = validate_geoparquet(full_path)
        print(f"PASSED = {validation_result.is_valid}")
        print(f"Passed {validation_result.passed_count}")
        print(f"Failed {validation_result.failed_count}")
        print(f"Warnings {validation_result.warning_count}")

def main():
    print("---- COMMENCING GEOPARQUET CONVERSION ----")
    convert_files()

    print("---- COMMENCING GEOPARQUET HILBERT SORTING ----")
    add_space_filling_curve()

    print("---- COMMENCING GEOPARQUET VALIDATION ----")
    validate()
    #
    # ###
    # # Todo: (assume data has been downloaded)
    # # Todo: go into raw data folder, for each file inside:
    # # Todo: convert file from current format to geoparquet
    # # Todo: write that to the converted directory
    # #

if __name__ == "__main__":
    # unzip_dir("../data/raw/")
    main()