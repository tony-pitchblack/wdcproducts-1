import requests
from zipfile import ZipFile
from pathlib import Path
import os

DATASETS = [
    'http://data.dws.informatik.uni-mannheim.de/largescaleproductcorpus/data/wdc-products/data.zip'
]

import requests
from tqdm import tqdm

def download_datasets():
    for link in DATASETS:
        # obtain filename by splitting url and getting the last part
        file_name = link.split('/')[-1]

        print("Downloading file: %s" % file_name)

        # create response object with stream enabled
        r = requests.get(link, stream=True)
        total_size = int(r.headers.get("content-length", 0))

        # download started with a progress bar
        with open(f'../../{file_name}', 'wb') as f, tqdm(
            total=total_size, unit="B", unit_scale=True, desc=file_name
        ) as pbar:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))

        print("%s downloaded!\n" % file_name)

    print("All files downloaded!")
    return

def unzip_files():
    for link in DATASETS:
        file_name = link.split('/')[-1]
        # opening the zip file in READ mode
        with ZipFile(f'../../{file_name}', 'r') as zip:
            # printing all the contents of the zip file
            zip.printdir()

            # extracting all the files
            print('Extracting all the files now...')
            zip.extractall(path='../../')
            print('Done!')

def print_unzipped_sizes():
    for link in DATASETS:
        file_name = link.split('/')[-1]
        zip_path = f'../../{file_name}'
        if os.path.exists(zip_path):
            try:
                with ZipFile(zip_path, 'r') as zip:
                    total_size = sum(zinfo.file_size for zinfo in zip.infolist())
                    print(f"{file_name} estimated unzipped size: {total_size / (1024 * 1024):.2f} MB")
            except Exception as e:
                print(f"Could not read {file_name}: {e}")

import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download, inspect, and unzip dataset files.")
    parser.add_argument("--download", action="store_true", default=False, help="Download dataset files")
    parser.add_argument("--print_sizes", action="store_true", default=False, help="Print estimated unzipped file sizes")
    parser.add_argument("--unzip", action="store_true", default=False, help="Unzip dataset files")
    args = parser.parse_args()

    if not (args.download or args.print_sizes or args.unzip):
        print("⚠️  Warning: All options are disabled. Nothing will be executed.")

    Path('../../data/').mkdir(parents=True, exist_ok=True)

    if args.download:
        download_datasets()

    if args.print_sizes:
        print_unzipped_sizes()

    if args.unzip:
        unzip_files()
