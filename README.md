# Release DB Copy

## Overview

This tool reads data from the Release internal database (Derby) and copies it to either an external database or to the filesystem as CSV files.

**Note: only CSV output supported at this time**

## Usage

### For CSV output

Copy the db-copy-1.0.jar file to the \<release home\>/archive directory.

Run the utlity with the following...

```bash
cd <release home>/archive
java -jar db-copy-1.0.jar -s jdbc:derby:db -t csv:<path to output directory>
```

Example...

```bash
cd /opt/xl-release-9.5.4-server/archive
java -jar db-copy-1.0.jar -s jdbc:derby:db -t csv:/tmp/export
```

After running the utility the /tmp/export directory will have a set of .csv files, one for each table.

## Development

To build and package the utility as an executable, run the following...

```bash
mvn clean package
```
