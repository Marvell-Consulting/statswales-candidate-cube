# Possible Data Cube Formate

This is a possible data cube format that the development team have been
experimenting with.  To get started you'll need to install DuckDB.

On a Mac:

```
brew install duckdb
```

For linux direct download from https://duckdb.org/docs/installation/?version=stable&environment=cli&platform=linux&download_method=direct&architecture=x86_64

For more information see: https://duckdb.org/docs/installation/?version=stable&environment=cli&platform=linux&download_method=direct&architecture=x86_64

## Getting started

To get started run the included SQL file directly against DuckDB to generate
a new cube from the included collection of CSV Files.  This will result in the
display of a Pivot table for SureStart data for FY 22.  You'll find a new DuckDB
file called `surestart.duckdb`.

```
duckdb surestart.duckdb < create_cube.sql
```

This will create the SureStart cube.  If you want to explore the cube you can then run:

```
duckdb surestart.duckdb
```

We also include `candidate-cube.duckdb` with in the archive which as already been generate
from the included SQL.


## The original data

The original data can be viewed at:

https://statswales.gov.wales/Catalogue/Health-and-Social-Care/NHS-Primary-and-Community-Activity/flying-start/numberofcontactsandaveragecontactsperchildreceivingflyingstartservices-by-staffgroup-localauthorithy

## The Pivot table code

```sql
PIVOT (SELECT
      year_codes.ViewName AS Year,
      reference_data_info.description AS Area,
      lookup_table.description AS Description,
      round(fact_table.Data, 2) AS Data,
      measure.name AS Name, reference_data.sort_order as RefSortOrder
  FROM
      year_codes, fact_table, reference_data, reference_data_info, lookup_table, measure
  WHERE
      year_codes.YearCode=fact_table.YearCode
      AND fact_table.AreaCode=reference_data.item_id
      AND reference_data.item_id=reference_data_info.item_id
      AND fact_table.Measure=measure.measure_id
      AND fact_table.RowRef=lookup_table.RowRef
      AND reference_data_info.lang='en-gb'
      AND lookup_table.lang='en-gb' AND measure.lang='en-gb'
      AND year_codes.ViewName='fy22'AND Area != 'Wales'
  ORDER BY
      lookup_table.SortOrder, RefSortOrder)
ON Description, name USING SUM(Data)
GROUP BY Area ORDER BY Area;
```

To include the totals remove `AND Area != 'wales'` from the PIVOT query.
To change the year change `AND year_codes.name='FY 22'` to be an `FY XX`
where `XX` any 2 digit year from 13 - 22.

## Useful DuckDB commands

To show all the tables in the database

```
.tables
```

To show the schema of a specific table

```
.schema <table_name>
```

