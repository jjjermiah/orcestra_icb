# Orcestra ICB Data Manager

This project provides a command-line interface (CLI) to manage and interact with a collection of ICB dataset records. The CLI allows you to list available datasets, download all datasets, or download a specific dataset by name.

## **Demo of downloading all ICB Dataset from Orcestra**

<img src="https://github.com/jjjermiah/orcestra_icb/blob/main/assets/Example_download.gif?raw=true" alt="Download" width="80%">

## Setup

Clone the repository:

```sh
git clone https://github.com/jjjermiah/orcestra_icb.git
```

Ensure you have `pixi` installed.

Then run:

```sh
pixi install
```

## Usage

### Listing Datasets

To list all available datasets, run:

```sh
pixi run icb list
```

### Downloading Datasets

To download all datasets, run:

```sh
pixi run icb download_all --output_dir <output_directory>
```

To download a specific dataset, run:

```sh
pixi run icb download <dataset_name> --output_dir <output_directory>

```
