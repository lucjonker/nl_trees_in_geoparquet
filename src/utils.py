import os
import zipfile


def unzip_dir(directory_path):
    for filename in os.listdir(directory_path):
        if filename.endswith(".zip"):
            full_path = os.path.join(directory_path, filename)
            extract_to = full_path.replace('.zip', '')

            print(f"Extracting {filename} to {extract_to}...")

            with zipfile.ZipFile(full_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)