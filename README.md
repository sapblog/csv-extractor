# CSV Extractor

## Overview

This tool uses features of the SAP HANA and Oracle databases to move tables from Oracle to SAP HANA as flat files, i.e., CSV.

## Installation

To installed the tool, database users and directories must be created on both the SAP HANA and Oracle directories.  The create_users.txt details the necessary users, grants, and directories for each database.

## Exporting a schema

## Future steps

## Flowchart of major elements
To better visualize the entire process, this flowchart highlights some of the major processing steps.

![CSV Exporter Flowchart](/images/Flow.png)

## File List
The files listed below are used mostly in the order they appear.  Please note that you will need to make adjustments to directories and databases schemas to match your environment.

### Getting started

| File | Description |
| ---- | ----------- |
| Flow.png | This is a detailed view of the major control elements of the process.|
| create-user.txt | Grants needed in the Oracle and HANA databases.  Also includes links to useful links.|
| create-tables.sql | Create the tables supporting the process.|

### Installing the software

| File | Description |
| ---- | ----------- |
CSV_EXPORTER.pls | Oracle package specification.
CSV_EXPORTER.plb | Oracle package body.
csv_exporter.sh | Linux script placed on the Oracle server to zip-n-ship the CSV output for a table.
csv_launch.sh | Linux script on the HANA server that is call by the csv_exporter.sh to start a HANA import.
csv_loader.sh | Linux script on the HANA server to perform the HANA import.

### Running tests

| File | Description |
| ---- | ----------- |
| sample-job.sql | Example of building and running an export of a schema. |
| stop-job.sql | Example of stopping a running job. |
| load-blob-from-oracle-dir.sql | Quick script to load a BLOB to test exporting to CSV. |
| build-data.sql | Example script to create 100 tables to export with 150K to 1M rows per table. |

