#!/bin/bash

##########################################################################
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# For more information, please refer to <http://unlicense.org/>
##########################################################################

# The following path must match the path specified when
# the extract job was created - THIS SHOULD NOT BE HARDCODED.

loaderPath=/usr/sap/HXE/HDB90/csv-loader
logFile=$loaderPath/csv_loader.log
exportPath=$loaderPath/exports

# This is the same guid used to create, zip and ship
# this table to the HANA server.

fileGUID=$1
timestamp=$(date)

echo "$timestamp - Start loading $fileGUID" >> $logFile

cd $loaderPath/exports

# The remote server provides 2 files for each table.  The zip
# is the actual CSV (zipped).  The SQL file has the HANA create
# table and import statements.
zipFile=$fileGUID.zip
sqlFile=$fileGUID.sql

# Durint this script, we'll extract the CSV to the following file
# and record logging information to the HDB file.  Note: the GUID for
# a table does not change between runs so the HDB file is the log
# for the most recent load of this table.
csvFile=$fileGUID.csv
hdbFile=$fileGUID.hdb

echo "$timestamp - Unziping $zipFile" >> $logFile

unzip $zipFile
rm -f $zipFile

timestamp=$(date)
echo "$timestamp - Unzip of $zipFile complete." >> $logFile

# The hardcoded password should be changed.
echo "$timestamp - Starting hdbsql for $sqlFile..." >> $logFile
hdbsql -i 90 -u csvadm -p Welcome01 -quiet -f -o $hdbFile -m -I $sqlFile

timestamp=$(date)
echo "$timestamp - Complete hdbsql for $sqlFile..." >> $logFile

echo "$timestamp - Removing $csvFile and $sqlFile." >> $logFile
rm -f $csvFile $sqlFile

echo "$timestamp - Complete load for $fileGUID." >> $logFile
