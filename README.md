# CSV Extractor

## Overview

## Installation

## Future steps

## File List
The files listed below are used mostly in the order they appear.  Please note that you will need to make adjustments to directories and databases schemas to match your environment.

Flow.png	This is a detailed view of the major control elements of the process.
create-user-grants.sql	Grants needed in the Oracle and HANA databases.  Also includes links to useful links.
create-tables.sql	Create the tables supporting the processes.
CSV_EXPORTER.pls	Oracle package specification.
CSV_EXPORTER.plb	Oracle package body.
run-job.sql	Example of building and running an export against a schema
stop-job.sql	Example of stopping a running job.
load-blob-from-oracle-dir.sql	Quick script to load a BLOB to test exporting to CSV.
build-data.sql	A script to create 100 tables to export with 150K to 1M rows per table.
csv_exporter.sh	Linux script placed on the Oracle server to zip-n-ship the CSV output for a table.
csv_launch.sh	Linux script on the HANA server that is call by the csv_exporter.sh to start a HANA import.
csv_loader.sh	Linux script on the HANA server to perform the HANA import.

