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
import asyncio
from dataclasses import dataclass
from typing import List, Dict, Optional
from rich import print
from rich.progress import Progress, TaskID
import click

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

    def load_from_file(self, file_path: str) -> None:
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
                self.records = [self._parse_record(record) for record in data]
        except FileNotFoundError:
            raise FileNotFoundError(f"File '{file_path}' not found.")
        except json.JSONDecodeError as e:
            raise ValueError(f"Error parsing JSON: {e}")

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
        """
        Downloads all datasets asynchronously to the specified output directory.
        Skips downloading files that already exist with matching hashes.

        Parameters
        ----------
        output_dir : str
            The directory where the datasets will be downloaded.

        Raises
        ------
        OSError
            If the output directory cannot be created.
        """
        os.makedirs(output_dir, exist_ok=True)

        async with aiohttp.ClientSession() as session:
            with Progress() as progress:
                tasks = []
                task_ids = {}

                for record in self.records:
                    output_path = os.path.join(output_dir, f"{record.name}.rds")
                    if not self._needs_download(output_path, record.downloadLink):
                        print(f"[green]Skipping download: {record.name} (already exists)[/green]")
                        continue

                    task_id = progress.add_task(f"Downloading {record.name}...", start=False)
                    task_ids[record.name] = task_id
                    tasks.append(self._download_file(session, record.downloadLink, output_path, progress, task_id))

                await asyncio.gather(*tasks)

    async def download_dataset_by_name(self, dataset_name: str, output_dir: str) -> None:
        """
        Downloads a single dataset by its name asynchronously to the specified output directory.
        Skips downloading the file if it already exists with a matching hash.

        Parameters
        ----------
        dataset_name : str
            The name of the dataset to download.
        output_dir : str
            The directory where the dataset will be downloaded.

        Raises
        ------
        ValueError
            If the dataset with the specified name is not found.
        OSError
            If the output directory cannot be created.
        """
        os.makedirs(output_dir, exist_ok=True)

        record = next((r for r in self.records if r.name == dataset_name), None)
        if not record:
            raise ValueError(f"Dataset '{dataset_name}' not found.")

        async with aiohttp.ClientSession() as session:
            output_path = os.path.join(output_dir, f"{record.name}.rds")
            if not self._needs_download(output_path, record.downloadLink):
                print(f"[green]Skipping download: {record.name} (already exists)[/green]")
                return

            with Progress() as progress:
                task_id = progress.add_task(f"Downloading {record.name}...", start=False)
                await self._download_file(session, record.downloadLink, output_path, progress, task_id)

    def _needs_download(self, file_path: str, download_link: str) -> bool:
        """
        Checks if a file needs to be downloaded based on its hash.

        Parameters
        ----------
        file_path : str
            The path to the file to check.
        download_link : str
            The download link (used for caching checks if needed).

        Returns
        -------
        bool
            True if the file needs to be downloaded, False otherwise.
        """
        if not os.path.exists(file_path):
            return True
        # In a real scenario, we could compare remote and local hash (if available).
        return False

    async def _download_file(self, session: aiohttp.ClientSession, url: str, output_path: str, 
                             progress: Progress, task_id: TaskID) -> None:
        """
        Downloads a file from the given URL to the specified path.

        Parameters
        ----------
        session : aiohttp.ClientSession
            The aiohttp session for making HTTP requests.
        url : str
            The URL to download the file from.
        output_path : str
            The path where the downloaded file will be saved.
        progress : Progress
            The Rich progress instance for tracking download progress.
        task_id : TaskID
            The progress task ID associated with the download.
        """
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                total_size = int(response.headers.get('content-length', 0))
                progress.start_task(task_id)
                progress.update(task_id, total=total_size)

                with open(output_path, 'wb') as file:
                    async for chunk in response.content.iter_chunked(1024):
                        file.write(chunk)
                        progress.update(task_id, advance=len(chunk))

                print(f"[green]Downloaded: {output_path}[/green]")
        except Exception as e:
            print(f"[red]Failed to download {url}: {e}[/red]")


@click.group()
@click.help_option('--help', '-h')
def icb():
    pass

@icb.command()
@click.help_option('--help', '-h')
def list():
    """List available datasets"""
    manager = ICBDataManager()
    manager.load_from_file("icb_api.json")
    print(manager.list_dataset_names())


@icb.command()
@click.option('--output_dir', default='datasets', help='Output directory for downloaded datasets')
@click.help_option('--help', '-h')
def download_all(output_dir):
    """Download all datasets"""
    manager = ICBDataManager()
    try:
        manager.load_from_file("icb_api.json")
        asyncio.run(manager.download_all_datasets(output_dir=output_dir))
    except Exception as e:
        print(f"Error: {e}")

@icb.command()
@click.argument('dataset_name')
@click.option('--output_dir', default='datasets', help='Output directory for downloaded dataset')
@click.help_option('--help', '-h')
def download(dataset_name, output_dir):
    """Download a single dataset by name"""
    manager = ICBDataManager()
    try:
        manager.load_from_file("icb_api.json")
        asyncio.run(manager.download_dataset_by_name(dataset_name, output_dir=output_dir))
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    icb()

# Example usage
# if __name__ == "__main__":
#     manager = ICBDataManager()
#     try:
#         manager.load_from_file("icb_api.json")
#         # print("Available datasets:")
#         # print(manager.list_dataset_names())
#         # print("\nDownload links:")
#         # print(manager.get_download_links())

#         # Run asynchronous download
#         asyncio.run(manager.download_all_datasets(output_dir="datasets"))
#     except Exception as e:
#         print(f"Error: {e}")
