import os
from idlelib.outwin import file_line_pats
from os.path import isdir
from re import fullmatch

import geoparquet_io as gpio
from geoparquet_io.core.add_bbox_column import add_bbox_column
from geoparquet_io.core.hilbert_order import hilbert_order
from geoparquet_io.core.partition_by_h3 import partition_by_h3
from src.utils import unzip_dir

SUPPORTED_TYPES = {
    ".geojson",
    ".json",
    ".shp",
    ".gpkg",
    ".gdb",
    ".csv",
    ".tsv"
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

            (gpio.convert(full_path)
             .add_bbox()
             .sort_hilbert()
             .write(f'{CONVERTED_DIRECTORY}{filename}.parquet'))

        elif file_extension == ".parquet":
            print("Already a parquet file:", file, ", moving to converted...")
            #todo: move file to converted folder, maybe run a hilbert sort too?

        # Error if unsupported filetype
        else:
            print(f"the file {full_path} is not supported")

def convert_crs():
    for file in os.listdir(CONVERTED_DIRECTORY):
        print(f"Hilbert ordering and reprojecting to {CRS} for file:", file, "...")

        full_path = os.path.join(CONVERTED_DIRECTORY, file)
        table = gpio.read(full_path)
        print(table.crs)
        (
            table.reproject(target_crs=CRS)
                .write(full_path)
         )

def main():
    print("---- COMMENCING GEOPARQUET CONVERSION ----")
    convert_files()
    print("---- COMMENCING GEOPARQUET ORDERING AND REPROJECTION ----")
    convert_crs()

    ###
    # Todo: (assume data has been downloaded)
    # Todo: go into raw data folder, for each file inside:
    # Todo: convert file from current format to geoparquet
    # Todo: write that to the converted directory
    #

if __name__ == "__main__":
    # unzip_dir("../data/raw/")
    main()