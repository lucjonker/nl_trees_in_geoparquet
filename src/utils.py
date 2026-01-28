import os
import zipfile
import boto3

def calculate_file_size(file_path):
    """Returns file size in Mb."""
    try:
        size_bytes = os.path.getsize(file_path)
        return round(size_bytes / (1024 * 1024), 2)
    except Exception:
        return 0

def compare_file_size(logger,dataset_name: str, raw_size_mb: float, dataset_path: str):
    final_size_mb = calculate_file_size(dataset_path)
    logger.info(f"Raw Size: {raw_size_mb} MB | Final Size: {final_size_mb} MB")

    with open("conversion_stats.csv", "a") as f:
        f.write(f"{dataset_name},{raw_size_mb},{final_size_mb}\n")

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


def main():
    # delete_s3_item("us-west-2.opendata.source.coop", "roorda-tudelft/public-trees-in-nl/geoparquet/")
    print("Hello world")

if __name__ == "__main__":
    main()