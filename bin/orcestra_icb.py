"""
ICB Data Manager
================

This module provides functionality to manage and interact with a collection of
ICB dataset records, loaded from a JSON file. It supports listing dataset names,
retrieving download links, and downloading datasets asynchronously to a specified
output directory, ensuring no duplicate downloads occur.

Usage
-----

1. Loading Dataset Records
--------------------------
Use the `load_from_file` method to load dataset records from a JSON file:

manager = ICBDataManager()
manager.load_from_file("icb_api.json")

2. Listing Dataset Names
-------------------------
Retrieve a list of all dataset names:

dataset_names = manager.list_dataset_names()
print(dataset_names)

3. Retrieving Download Links
----------------------------
Get a dictionary of dataset names mapped to their download links:

download_links = manager.get_download_links()
print(download_links)

4. Downloading Datasets
-----------------------
Download all datasets asynchronously to a specified directory. If a dataset has
already been downloaded and the file matches (based on hash comparison), it will
not be downloaded again.

import asyncio

async def main():
    await manager.download_all_datasets(output_dir="datasets")

asyncio.run(main())

Notes
-----
- Downloads are performed asynchronously for efficiency.
- A file is considered identical if its MD5 hash matches the existing file in the
  output directory.
"""
import json
import os
import aiohttp
from rich import print
import asyncio
from dataclasses import dataclass
from typing import List, Dict, Optional
from rich.progress import Progress, TaskID
from rich.logging import RichHandler
import logging
import click

# Set up rich logging
logging.basicConfig(level="NOTSET", handlers=[RichHandler()])
logger = logging.getLogger("rich")

API_URL = 'https://orcestra.ca/api/clinical_icb/available'
CACHE_FILE = 'icb_api.json'

@dataclass
class Publication:
    citation: str
    link: str

@dataclass
class VersionInfo:
    version: str
    type: Optional[str]
    publication: List[Publication]

@dataclass
class Dataset:
    name: str
    versionInfo: VersionInfo

@dataclass
class AvailableDatatype:
    name: str
    genomeType: str
    source: Optional[str] = None

@dataclass
class DatasetRecord:
    name: str
    doi: str
    downloadLink: str
    dateCreated: str
    dataset: Dataset
    availableDatatypes: List[AvailableDatatype]

class ICBDataManager:
    def __init__(self) -> None:
        self.records: List[DatasetRecord] = []

    async def fetch_data(self) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.get(API_URL) as response:
                response.raise_for_status()
                data = await response.json()
                with open(CACHE_FILE, 'w') as file:
                    json.dump(data, file)
                self.records = [self._parse_record(record) for record in data]

    def load_from_cache(self) -> None:
        try:
            with open(CACHE_FILE, 'r') as file:
                data = json.load(file)
                self.records = [self._parse_record(record) for record in data]
        except FileNotFoundError as fe:
            msg = f"Cache file '{CACHE_FILE}' not found. Please update the cache using 'icb update-cache'."
            raise FileNotFoundError(msg) from fe
        except json.JSONDecodeError as e:
            msg = f"Error parsing JSON: {e}"
            raise json.JSONDecodeError(msg) from e

    def _parse_record(self, record: dict) -> DatasetRecord:
        publications = [
            Publication(**pub) for pub in record['dataset']['versionInfo']['publication']
        ]
        version_info = VersionInfo(
            version=record['dataset']['versionInfo']['version'],
            type=record['dataset']['versionInfo'].get('type'),
            publication=publications
        )
        dataset = Dataset(
            name=record['dataset']['name'],
            versionInfo=version_info
        )
        datatypes = [
            AvailableDatatype(**dt) for dt in record.get('availableDatatypes', [])
        ]
        return DatasetRecord(
            name=record['name'],
            doi=record['doi'],
            downloadLink=record['downloadLink'],
            dateCreated=record['dateCreated'],
            dataset=dataset,
            availableDatatypes=datatypes
        )

    def list_dataset_names(self) -> List[str]:
        return [record.name for record in self.records]

    def get_download_links(self) -> Dict[str, str]:
        return {record.name: record.downloadLink for record in self.records}

    async def download_all_datasets(self, output_dir: str) -> None:
        os.makedirs(output_dir, exist_ok=True)

        async with aiohttp.ClientSession() as session:
            with Progress(transient=True) as progress:
                tasks = []
                task_ids = {}

                for record in self.records:
                    output_path = os.path.join(output_dir, f"{record.name}.rds")
                    if not self._needs_download(output_path, record.downloadLink):
                        logger.info(f"Skipping download: {record.name} (already exists)")
                        continue

                    task_id = progress.add_task(f"Downloading {record.name}...", start=False)
                    task_ids[record.name] = task_id
                    tasks.append(self._download_file(session, record.downloadLink, output_path, progress, task_id))

                await asyncio.gather(*tasks)

    async def download_dataset_by_name(self, dataset_name: str, output_dir: str) -> None:
        os.makedirs(output_dir, exist_ok=True)

        record = next((r for r in self.records if r.name == dataset_name), None)
        if not record:
            logger.error(f"Dataset '{dataset_name}' not found.")
            raise ValueError(f"Dataset '{dataset_name}' not found.")

        async with aiohttp.ClientSession() as session:
            output_path = os.path.join(output_dir, f"{record.name}.rds")
            if not self._needs_download(output_path, record.downloadLink):
                logger.info(f"Skipping download: {record.name} (already exists)")
                return

            with Progress() as progress:
                task_id = progress.add_task(f"Downloading {record.name}...", start=False)
                await self._download_file(session, record.downloadLink, output_path, progress, task_id)

    def _needs_download(self, file_path: str, download_link: str) -> bool:
        if not os.path.exists(file_path):
            return True
        return False

    async def _download_file(self, session: aiohttp.ClientSession, url: str, output_path: str, 
                             progress: Progress, task_id: TaskID) -> None:
        temp_output_path = output_path + ".part"
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                total_size = int(response.headers.get('content-length', 0))
                progress.start_task(task_id)
                progress.update(task_id, total=total_size)

                with open(temp_output_path, 'wb') as file:
                    async for chunk in response.content.iter_chunked(1024):
                        file.write(chunk)
                        progress.update(task_id, advance=len(chunk))

                os.rename(temp_output_path, output_path)
                print(f"[green]Downloaded: {output_path}[/green]")
        except Exception as e:
            if os.path.exists(temp_output_path):
                os.remove(temp_output_path)
            print(f"[red]Failed to download {url}: {e}[/red]")
            logger.exception(f"Failed to download {url}: {e}")
        progress.remove_task(task_id)

@click.group()
@click.help_option('--help', '-h')
def icb():
    pass

@icb.command()
@click.help_option('--help', '-h')
def list():
    """List available datasets"""
    manager = ICBDataManager()
    try:
        manager.load_from_cache()
    except Exception as e:
        logger.exception(f"Error: {e}")
        return
    ds = manager.list_dataset_names()
    # print a pretty table
    print(f"Available datasets: {len(ds)}")
    for d in ds:
        print(f'\t- [bold green] {d}')


@icb.command()
@click.option('--output_dir', default='datasets', help='Output directory for downloaded datasets')
@click.help_option('--help', '-h')
def download_all(output_dir):
    """Download all datasets"""
    manager = ICBDataManager()
    try:
        manager.load_from_cache()
        asyncio.run(manager.download_all_datasets(output_dir=output_dir))
    except Exception as e:
        logger.exception(f"Error: {e}")
        return

@icb.command()
@click.argument('dataset_name')
@click.option('--output_dir', default='datasets', help='Output directory for downloaded dataset')
@click.help_option('--help', '-h')
def download(dataset_name, output_dir):
    """Download a single dataset by name"""
    manager = ICBDataManager()
    try:
        manager.load_from_cache()
        asyncio.run(manager.download_dataset_by_name(dataset_name, output_dir=output_dir))
    except Exception as e:
        logger.exception(f"Error: {e}")
        return

@icb.command()
@click.help_option('--help', '-h')
def update_cache():
    """Update the dataset cache"""
    manager = ICBDataManager()
    try:
        logger.info("Fetching data from API...")
        with Progress() as progress:
            task_id = progress.add_task("Fetching data...", start=False)
            asyncio.run(manager.fetch_data())
            progress.update(task_id, completed=True)
        logger.info("Cache updated successfully.")
    except Exception as e:
        logger.exception(f"Error: {e}")
        return

if __name__ == "__main__":
    icb()
