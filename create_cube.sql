-- CREATE FACT TABLE
CREATE TABLE fact_table AS FROM 'data/sure-start-data.csv';
-- Anything linking against the reference table needs to be a varchar
ALTER TABLE fact_table ALTER AreaCode TYPE VARCHAR;

-- ADD LOOK UP TABLES FOR Measure and RowRef
CREATE TABLE measure AS FROM 'user-lookup-tables/measure.csv';
CREATE TABLE lookup_table AS FROM 'user-lookup-tables/lookup_table.csv';

-- ADD LOOKUP TABLE FOR YearCode (This is a type of reference data)
CREATE TABLE year_codes AS FROM 'system-lookup-tables/year_codes.csv';

-- ADD LOOKUP TABLE FOR NOTE CODES
CREATE TABLE note_codes AS FROM 'system-lookup-tables/notes_codes.csv';

-- CREATE REFERENCE DATA TABLES
CREATE TABLE "categories" (
    "category" TEXT PRIMARY KEY
);

CREATE TABLE "category_keys" (
    "category_key" TEXT PRIMARY KEY,
    "category" TEXT NOT NULL,
    -- FOREIGN KEY("category") REFERENCES "categories"("category")
);

CREATE TABLE "reference_data" (
	"item_id"	TEXT NOT NULL,
	"version_no"	INTEGER NOT NULL,
	"sort_order"	INTEGER,
	"category_key"	TEXT NOT NULL,
	"validity_start"	TEXT NOT NULL,
	"validity_end"	TEXT,
	PRIMARY KEY("item_id","version_no","category_key"),
    -- FOREIGN KEY("category_key") REFERENCES "category_keys"("category_key")
);

CREATE TABLE "reference_data_info" (
    "item_id" TEXT NOT NULL,
    "version_no" INTEGER NOT NULL,
    "category_key" TEXT NOT NULL,
    "lang" TEXT NOT NULL,
    "description" TEXT NOT NULL,
    "notes" TEXT,
    PRIMARY KEY("item_id","version_no","category_key","lang"),
    -- FOREIGN KEY("item_id","version_no","category_key") REFERENCES "reference_data"("item_id","version_no","category_key")
);

CREATE TABLE "category_key_info" (
    "category_key" TEXT NOT NULL,
    "lang" TEXT NOT NULL,
    "description" TEXT NOT NULL,
	"notes" TEXT,
    PRIMARY KEY("category_key","lang"),
    -- FOREIGN KEY("category_key") REFERENCES "category_keys"("category_key")
);

CREATE TABLE "category_info" (
    "category" TEXT NOT NULL,
    "lang" TEXT NOT NULL,
    "description" TEXT NOT NULL,
	"notes" TEXT,
    PRIMARY KEY("category","lang"),
    -- FOREIGN KEY("category") REFERENCES "categories"("category")
);

CREATE TABLE "hierarchy" (
    "item_id" TEXT NOT NULL,
    "version_no" INTEGER NOT NULL,
    "category_key" TEXT NOT NULL,
    "parent_id" TEXT NOT NULL,
    "parent_version" INTEGER NOT NULL,
    "parent_category" TEXT NOT NULL,
    PRIMARY KEY("item_id","version_no","category_key","parent_id","parent_version","parent_category"),
    -- FOREIGN KEY("item_id","version_no","category_key") REFERENCES "reference_data"("item_id","version_no","category_key"),
    -- FOREIGN KEY("parent_id","parent_version","parent_category") REFERENCES "reference_data"("item_id","version_no","category_key")
);

COPY categories FROM 'reference-data/categories.csv';
COPY category_keys FROM 'reference-data/category_key.csv';
COPY reference_data FROM 'reference-data/reference_data.csv';
COPY reference_data_info FROM 'reference-data/reference_data_info.csv';
COPY category_key_info FROM 'reference-data/category_key_info.csv';
COPY category_info FROM 'reference-data/category_info.csv';
COPY hierarchy FROM 'reference-data/hierarchy.csv';

-- Clean up the reference data to remove entires we don't need
DELETE FROM reference_data WHERE item_id NOT IN (SELECT AreaCode FROM fact_table);
DELETE FROM reference_data_info WHERE item_id NOT IN (SELECT item_id FROM reference_data);
DELETE FROM category_keys WHERE category_key NOT IN (SELECT category_key FROM reference_data);
DELETE FROM category_Key_info WHERE category_key NOT IN (select category_key FROM category_keys);
DELETE FROM categories where category NOT IN (SELECT category FROM category_keys);
DELETE FROM category_info WHERE category NOT IN (SELECT category FROM categories);
DELETE FROM hierarchy WHERE item_id NOT IN (SELECT item_id FROM reference_data);

-- Create Views for the Data based on year
CREATE VIEW data_fy12 AS SELECT
    year_codes.ViewName AS Year,
    reference_data_info.description AS Area,
    lookup_table.description AS Description,
    round(fact_table.Data, 2) AS Data,
    note_codes.value AS Note,
    measure.name AS Name, reference_data.sort_order as RefSortOrder
    FROM
        fact_table
    LEFT JOIN year_codes ON year_codes.YearCode=fact_table.YearCode
    LEFT JOIN lookup_table ON fact_table.RowRef=lookup_table.RowRef AND lookup_table.lang='en-gb'
    LEFT JOIN reference_data ON fact_table.AreaCode=reference_data.item_id
    LEFT JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id AND reference_data_info.lang='en-gb'
    LEFT JOIN measure ON fact_table.Measure=measure.measure_id AND measure.lang='en-gb'
    LEFT JOIN note_codes ON fact_table.NoteCodes=note_codes.code AND note_codes.lang='en-gb'
    WHERE
        year_codes.ViewName='fy12'
        AND Area != 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;

CREATE VIEW total_fy12 AS SELECT
    year_codes.ViewName AS Year,
    reference_data_info.description AS Area,
    lookup_table.description AS Description,
    round(fact_table.Data, 2) AS Data,
    note_codes.value AS Note,
    measure.name AS Name, reference_data.sort_order as RefSortOrder
    FROM
        fact_table
    LEFT JOIN year_codes ON year_codes.YearCode=fact_table.YearCode
    LEFT JOIN lookup_table ON fact_table.RowRef=lookup_table.RowRef AND lookup_table.lang='en-gb'
    LEFT JOIN reference_data ON fact_table.AreaCode=reference_data.item_id
    LEFT JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id AND reference_data_info.lang='en-gb'
    LEFT JOIN measure ON fact_table.Measure=measure.measure_id AND measure.lang='en-gb'
    LEFT JOIN note_codes ON fact_table.NoteCodes=note_codes.code AND note_codes.lang='en-gb'
    WHERE
        year_codes.ViewName='fy12'
        AND Area = 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;

CREATE VIEW data_fy13 AS SELECT
    year_codes.ViewName AS Year,
    reference_data_info.description AS Area,
    lookup_table.description AS Description,
    round(fact_table.Data, 2) AS Data,
    note_codes.value AS Note,
    measure.name AS Name, reference_data.sort_order as RefSortOrder
    FROM
        fact_table
    LEFT JOIN year_codes ON year_codes.YearCode=fact_table.YearCode
    LEFT JOIN lookup_table ON fact_table.RowRef=lookup_table.RowRef AND lookup_table.lang='en-gb'
    LEFT JOIN reference_data ON fact_table.AreaCode=reference_data.item_id
    LEFT JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id AND reference_data_info.lang='en-gb'
    LEFT JOIN measure ON fact_table.Measure=measure.measure_id AND measure.lang='en-gb'
    LEFT JOIN note_codes ON fact_table.NoteCodes=note_codes.code AND note_codes.lang='en-gb'
    WHERE
        year_codes.ViewName='fy13'
        AND Area != 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;

CREATE VIEW total_fy13 AS SELECT
    year_codes.ViewName AS Year,
    reference_data_info.description AS Area,
    lookup_table.description AS Description,
    round(fact_table.Data, 2) AS Data,
    note_codes.value AS Note,
    measure.name AS Name, reference_data.sort_order as RefSortOrder
    FROM
        fact_table
    LEFT JOIN year_codes ON year_codes.YearCode=fact_table.YearCode
    LEFT JOIN lookup_table ON fact_table.RowRef=lookup_table.RowRef AND lookup_table.lang='en-gb'
    LEFT JOIN reference_data ON fact_table.AreaCode=reference_data.item_id
    LEFT JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id AND reference_data_info.lang='en-gb'
    LEFT JOIN measure ON fact_table.Measure=measure.measure_id AND measure.lang='en-gb'
    LEFT JOIN note_codes ON fact_table.NoteCodes=note_codes.code AND note_codes.lang='en-gb'
    WHERE
        year_codes.ViewName='fy13'
        AND Area = 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;

CREATE VIEW data_fy14 AS SELECT
    year_codes.ViewName AS Year,
    reference_data_info.description AS Area,
    lookup_table.description AS Description,
    round(fact_table.Data, 2) AS Data,
    note_codes.value AS Note,
    measure.name AS Name, reference_data.sort_order as RefSortOrder
    FROM
        fact_table
    LEFT JOIN year_codes ON year_codes.YearCode=fact_table.YearCode
    LEFT JOIN lookup_table ON fact_table.RowRef=lookup_table.RowRef AND lookup_table.lang='en-gb'
    LEFT JOIN reference_data ON fact_table.AreaCode=reference_data.item_id
    LEFT JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id AND reference_data_info.lang='en-gb'
    LEFT JOIN measure ON fact_table.Measure=measure.measure_id AND measure.lang='en-gb'
    LEFT JOIN note_codes ON fact_table.NoteCodes=note_codes.code AND note_codes.lang='en-gb'
    WHERE
        year_codes.ViewName='fy14'
        AND Area != 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;

CREATE VIEW total_fy14 AS SELECT
    year_codes.ViewName AS Year,
    reference_data_info.description AS Area,
    lookup_table.description AS Description,
    round(fact_table.Data, 2) AS Data,
    note_codes.value AS Note,
    measure.name AS Name, reference_data.sort_order as RefSortOrder
    FROM
        fact_table
    LEFT JOIN year_codes ON year_codes.YearCode=fact_table.YearCode
    LEFT JOIN lookup_table ON fact_table.RowRef=lookup_table.RowRef AND lookup_table.lang='en-gb'
    LEFT JOIN reference_data ON fact_table.AreaCode=reference_data.item_id
    LEFT JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id AND reference_data_info.lang='en-gb'
    LEFT JOIN measure ON fact_table.Measure=measure.measure_id AND measure.lang='en-gb'
    LEFT JOIN note_codes ON fact_table.NoteCodes=note_codes.code AND note_codes.lang='en-gb'
    WHERE
        year_codes.ViewName='fy14'
        AND Area = 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;

CREATE VIEW data_fy15 AS SELECT
    year_codes.ViewName AS Year,
    reference_data_info.description AS Area,
    lookup_table.description AS Description,
    round(fact_table.Data, 2) AS Data,
    note_codes.value AS Note,
    measure.name AS Name, reference_data.sort_order as RefSortOrder
    FROM
        fact_table
    LEFT JOIN year_codes ON year_codes.YearCode=fact_table.YearCode
    LEFT JOIN lookup_table ON fact_table.RowRef=lookup_table.RowRef AND lookup_table.lang='en-gb'
    LEFT JOIN reference_data ON fact_table.AreaCode=reference_data.item_id
    LEFT JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id AND reference_data_info.lang='en-gb'
    LEFT JOIN measure ON fact_table.Measure=measure.measure_id AND measure.lang='en-gb'
    LEFT JOIN note_codes ON fact_table.NoteCodes=note_codes.code AND note_codes.lang='en-gb'
    WHERE
        year_codes.ViewName='fy15'
        AND Area != 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;

CREATE VIEW total_fy15 AS SELECT
    year_codes.ViewName AS Year,
    reference_data_info.description AS Area,
    lookup_table.description AS Description,
    round(fact_table.Data, 2) AS Data,
    note_codes.value AS Note,
    measure.name AS Name, reference_data.sort_order as RefSortOrder
    FROM
        fact_table
    LEFT JOIN year_codes ON year_codes.YearCode=fact_table.YearCode
    LEFT JOIN lookup_table ON fact_table.RowRef=lookup_table.RowRef AND lookup_table.lang='en-gb'
    LEFT JOIN reference_data ON fact_table.AreaCode=reference_data.item_id
    LEFT JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id AND reference_data_info.lang='en-gb'
    LEFT JOIN measure ON fact_table.Measure=measure.measure_id AND measure.lang='en-gb'
    LEFT JOIN note_codes ON fact_table.NoteCodes=note_codes.code AND note_codes.lang='en-gb'
    WHERE
        year_codes.ViewName='fy15'
        AND Area = 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;

CREATE VIEW data_fy16 AS SELECT
    year_codes.ViewName AS Year,
    reference_data_info.description AS Area,
    lookup_table.description AS Description,
    round(fact_table.Data, 2) AS Data,
    note_codes.value AS Note,
    measure.name AS Name, reference_data.sort_order as RefSortOrder
    FROM
        fact_table
    LEFT JOIN year_codes ON year_codes.YearCode=fact_table.YearCode
    LEFT JOIN lookup_table ON fact_table.RowRef=lookup_table.RowRef AND lookup_table.lang='en-gb'
    LEFT JOIN reference_data ON fact_table.AreaCode=reference_data.item_id
    LEFT JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id AND reference_data_info.lang='en-gb'
    LEFT JOIN measure ON fact_table.Measure=measure.measure_id AND measure.lang='en-gb'
    LEFT JOIN note_codes ON fact_table.NoteCodes=note_codes.code AND note_codes.lang='en-gb'
    WHERE
        year_codes.ViewName='fy16'
        AND Area != 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;

CREATE VIEW total_fy16 AS SELECT
    year_codes.ViewName AS Year,
    reference_data_info.description AS Area,
    lookup_table.description AS Description,
    round(fact_table.Data, 2) AS Data,
    note_codes.value AS Note,
    measure.name AS Name, reference_data.sort_order as RefSortOrder
    FROM
        fact_table
    LEFT JOIN year_codes ON year_codes.YearCode=fact_table.YearCode
    LEFT JOIN lookup_table ON fact_table.RowRef=lookup_table.RowRef AND lookup_table.lang='en-gb'
    LEFT JOIN reference_data ON fact_table.AreaCode=reference_data.item_id
    LEFT JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id AND reference_data_info.lang='en-gb'
    LEFT JOIN measure ON fact_table.Measure=measure.measure_id AND measure.lang='en-gb'
    LEFT JOIN note_codes ON fact_table.NoteCodes=note_codes.code AND note_codes.lang='en-gb'
    WHERE
        year_codes.ViewName='fy16'
        AND Area = 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;

CREATE VIEW data_fy17 AS SELECT
    year_codes.ViewName AS Year,
    reference_data_info.description AS Area,
    lookup_table.description AS Description,
    round(fact_table.Data, 2) AS Data,
    note_codes.value AS Note,
    measure.name AS Name, reference_data.sort_order as RefSortOrder
    FROM
        fact_table
    LEFT JOIN year_codes ON year_codes.YearCode=fact_table.YearCode
    LEFT JOIN lookup_table ON fact_table.RowRef=lookup_table.RowRef AND lookup_table.lang='en-gb'
    LEFT JOIN reference_data ON fact_table.AreaCode=reference_data.item_id
    LEFT JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id AND reference_data_info.lang='en-gb'
    LEFT JOIN measure ON fact_table.Measure=measure.measure_id AND measure.lang='en-gb'
    LEFT JOIN note_codes ON fact_table.NoteCodes=note_codes.code AND note_codes.lang='en-gb'
    WHERE
        year_codes.ViewName='fy17'
        AND Area != 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;

CREATE VIEW total_fy17 AS SELECT
    year_codes.ViewName AS Year,
    reference_data_info.description AS Area,
    lookup_table.description AS Description,
    round(fact_table.Data, 2) AS Data,
    note_codes.value AS Note,
    measure.name AS Name, reference_data.sort_order as RefSortOrder
    FROM
        fact_table
    LEFT JOIN year_codes ON year_codes.YearCode=fact_table.YearCode
    LEFT JOIN lookup_table ON fact_table.RowRef=lookup_table.RowRef AND lookup_table.lang='en-gb'
    LEFT JOIN reference_data ON fact_table.AreaCode=reference_data.item_id
    LEFT JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id AND reference_data_info.lang='en-gb'
    LEFT JOIN measure ON fact_table.Measure=measure.measure_id AND measure.lang='en-gb'
    LEFT JOIN note_codes ON fact_table.NoteCodes=note_codes.code AND note_codes.lang='en-gb'
    WHERE
        year_codes.ViewName='fy17'
        AND Area = 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;

CREATE VIEW data_fy18 AS SELECT
    year_codes.ViewName AS Year,
    reference_data_info.description AS Area,
    lookup_table.description AS Description,
    round(fact_table.Data, 2) AS Data,
    note_codes.value AS Note,
    measure.name AS Name, reference_data.sort_order as RefSortOrder
    FROM
        fact_table
    LEFT JOIN year_codes ON year_codes.YearCode=fact_table.YearCode
    LEFT JOIN lookup_table ON fact_table.RowRef=lookup_table.RowRef AND lookup_table.lang='en-gb'
    LEFT JOIN reference_data ON fact_table.AreaCode=reference_data.item_id
    LEFT JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id AND reference_data_info.lang='en-gb'
    LEFT JOIN measure ON fact_table.Measure=measure.measure_id AND measure.lang='en-gb'
    LEFT JOIN note_codes ON fact_table.NoteCodes=note_codes.code AND note_codes.lang='en-gb'
    WHERE
        year_codes.ViewName='fy18'
        AND Area != 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;

CREATE VIEW total_fy18 AS SELECT
    year_codes.ViewName AS Year,
    reference_data_info.description AS Area,
    lookup_table.description AS Description,
    round(fact_table.Data, 2) AS Data,
    note_codes.value AS Note,
    measure.name AS Name, reference_data.sort_order as RefSortOrder
    FROM
        fact_table
    LEFT JOIN year_codes ON year_codes.YearCode=fact_table.YearCode
    LEFT JOIN lookup_table ON fact_table.RowRef=lookup_table.RowRef AND lookup_table.lang='en-gb'
    LEFT JOIN reference_data ON fact_table.AreaCode=reference_data.item_id
    LEFT JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id AND reference_data_info.lang='en-gb'
    LEFT JOIN measure ON fact_table.Measure=measure.measure_id AND measure.lang='en-gb'
    LEFT JOIN note_codes ON fact_table.NoteCodes=note_codes.code AND note_codes.lang='en-gb'
    WHERE
        year_codes.ViewName='fy18'
        AND Area = 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;

CREATE VIEW data_fy19 AS SELECT
    year_codes.ViewName AS Year,
    reference_data_info.description AS Area,
    lookup_table.description AS Description,
    round(fact_table.Data, 2) AS Data,
    note_codes.value AS Note,
    measure.name AS Name, reference_data.sort_order as RefSortOrder
    FROM
        fact_table
    LEFT JOIN year_codes ON year_codes.YearCode=fact_table.YearCode
    LEFT JOIN lookup_table ON fact_table.RowRef=lookup_table.RowRef AND lookup_table.lang='en-gb'
    LEFT JOIN reference_data ON fact_table.AreaCode=reference_data.item_id
    LEFT JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id AND reference_data_info.lang='en-gb'
    LEFT JOIN measure ON fact_table.Measure=measure.measure_id AND measure.lang='en-gb'
    LEFT JOIN note_codes ON fact_table.NoteCodes=note_codes.code AND note_codes.lang='en-gb'
    WHERE
        year_codes.ViewName='fy19'
        AND Area != 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;

CREATE VIEW total_fy19 AS SELECT
    year_codes.ViewName AS Year,
    reference_data_info.description AS Area,
    lookup_table.description AS Description,
    round(fact_table.Data, 2) AS Data,
    note_codes.value AS Note,
    measure.name AS Name, reference_data.sort_order as RefSortOrder
    FROM
        fact_table
    LEFT JOIN year_codes ON year_codes.YearCode=fact_table.YearCode
    LEFT JOIN lookup_table ON fact_table.RowRef=lookup_table.RowRef AND lookup_table.lang='en-gb'
    LEFT JOIN reference_data ON fact_table.AreaCode=reference_data.item_id
    LEFT JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id AND reference_data_info.lang='en-gb'
    LEFT JOIN measure ON fact_table.Measure=measure.measure_id AND measure.lang='en-gb'
    LEFT JOIN note_codes ON fact_table.NoteCodes=note_codes.code AND note_codes.lang='en-gb'
    WHERE
        year_codes.ViewName='fy19'
        AND Area = 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;

CREATE VIEW data_fy20 AS SELECT
    year_codes.ViewName AS Year,
    reference_data_info.description AS Area,
    lookup_table.description AS Description,
    round(fact_table.Data, 2) AS Data,
    note_codes.value AS Note,
    measure.name AS Name, reference_data.sort_order as RefSortOrder
    FROM
        fact_table
    LEFT JOIN year_codes ON year_codes.YearCode=fact_table.YearCode
    LEFT JOIN lookup_table ON fact_table.RowRef=lookup_table.RowRef AND lookup_table.lang='en-gb'
    LEFT JOIN reference_data ON fact_table.AreaCode=reference_data.item_id
    LEFT JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id AND reference_data_info.lang='en-gb'
    LEFT JOIN measure ON fact_table.Measure=measure.measure_id AND measure.lang='en-gb'
    LEFT JOIN note_codes ON fact_table.NoteCodes=note_codes.code AND note_codes.lang='en-gb'
    WHERE
        year_codes.ViewName='fy20'
        AND Area != 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;

CREATE VIEW total_fy20 AS SELECT
    year_codes.ViewName AS Year,
    reference_data_info.description AS Area,
    lookup_table.description AS Description,
    round(fact_table.Data, 2) AS Data,
    note_codes.value AS Note,
    measure.name AS Name, reference_data.sort_order as RefSortOrder
    FROM
        fact_table
    LEFT JOIN year_codes ON year_codes.YearCode=fact_table.YearCode
    LEFT JOIN lookup_table ON fact_table.RowRef=lookup_table.RowRef AND lookup_table.lang='en-gb'
    LEFT JOIN reference_data ON fact_table.AreaCode=reference_data.item_id
    LEFT JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id AND reference_data_info.lang='en-gb'
    LEFT JOIN measure ON fact_table.Measure=measure.measure_id AND measure.lang='en-gb'
    LEFT JOIN note_codes ON fact_table.NoteCodes=note_codes.code AND note_codes.lang='en-gb'
    WHERE
        year_codes.ViewName='fy20'
        AND Area = 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;

CREATE VIEW data_fy21 AS SELECT
    year_codes.ViewName AS Year,
    reference_data_info.description AS Area,
    lookup_table.description AS Description,
    round(fact_table.Data, 2) AS Data,
    note_codes.value AS Note,
    measure.name AS Name, reference_data.sort_order as RefSortOrder
    FROM
        fact_table
    LEFT JOIN year_codes ON year_codes.YearCode=fact_table.YearCode
    LEFT JOIN lookup_table ON fact_table.RowRef=lookup_table.RowRef AND lookup_table.lang='en-gb'
    LEFT JOIN reference_data ON fact_table.AreaCode=reference_data.item_id
    LEFT JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id AND reference_data_info.lang='en-gb'
    LEFT JOIN measure ON fact_table.Measure=measure.measure_id AND measure.lang='en-gb'
    LEFT JOIN note_codes ON fact_table.NoteCodes=note_codes.code AND note_codes.lang='en-gb'
    WHERE
        year_codes.ViewName='fy21'
        AND Area != 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;

CREATE VIEW total_fy21 AS SELECT
    year_codes.ViewName AS Year,
    reference_data_info.description AS Area,
    lookup_table.description AS Description,
    round(fact_table.Data, 2) AS Data,
    note_codes.value AS Note,
    measure.name AS Name, reference_data.sort_order as RefSortOrder
    FROM
        fact_table
    LEFT JOIN year_codes ON year_codes.YearCode=fact_table.YearCode
    LEFT JOIN lookup_table ON fact_table.RowRef=lookup_table.RowRef AND lookup_table.lang='en-gb'
    LEFT JOIN reference_data ON fact_table.AreaCode=reference_data.item_id
    LEFT JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id AND reference_data_info.lang='en-gb'
    LEFT JOIN measure ON fact_table.Measure=measure.measure_id AND measure.lang='en-gb'
    LEFT JOIN note_codes ON fact_table.NoteCodes=note_codes.code AND note_codes.lang='en-gb'
    WHERE
        year_codes.ViewName='fy21'
        AND Area = 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;

CREATE VIEW data_fy22 AS SELECT
    year_codes.ViewName AS Year,
    reference_data_info.description AS Area,
    lookup_table.description AS Description,
    round(fact_table.Data, 2) AS Data,
    note_codes.value AS Note,
    measure.name AS Name, reference_data.sort_order as RefSortOrder
    FROM
        fact_table
    LEFT JOIN year_codes ON year_codes.YearCode=fact_table.YearCode
    LEFT JOIN lookup_table ON fact_table.RowRef=lookup_table.RowRef AND lookup_table.lang='en-gb'
    LEFT JOIN reference_data ON fact_table.AreaCode=reference_data.item_id
    LEFT JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id AND reference_data_info.lang='en-gb'
    LEFT JOIN measure ON fact_table.Measure=measure.measure_id AND measure.lang='en-gb'
    LEFT JOIN note_codes ON fact_table.NoteCodes=note_codes.code AND note_codes.lang='en-gb'
    WHERE
        year_codes.ViewName='fy22'
        AND Area != 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;

CREATE VIEW total_fy22 AS SELECT
    year_codes.ViewName AS Year,
    reference_data_info.description AS Area,
    lookup_table.description AS Description,
    round(fact_table.Data, 2) AS Data,
    note_codes.value AS Note,
    measure.name AS Name, reference_data.sort_order as RefSortOrder
    FROM
        fact_table
    LEFT JOIN year_codes ON year_codes.YearCode=fact_table.YearCode
    LEFT JOIN lookup_table ON fact_table.RowRef=lookup_table.RowRef AND lookup_table.lang='en-gb'
    LEFT JOIN reference_data ON fact_table.AreaCode=reference_data.item_id
    LEFT JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id AND reference_data_info.lang='en-gb'
    LEFT JOIN measure ON fact_table.Measure=measure.measure_id AND measure.lang='en-gb'
    LEFT JOIN note_codes ON fact_table.NoteCodes=note_codes.code AND note_codes.lang='en-gb'
    WHERE
        year_codes.ViewName='fy22'
        AND Area = 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;

-- Pivot table for FY22
-- Can be compared against https://statswales.gov.wales/Catalogue/Health-and-Social-Care/NHS-Primary-and-Community-Activity/flying-start/numberofcontactsandaveragecontactsperchildreceivingflyingstartservices-by-staffgroup-localauthorithy
.mode table
SELECT 'Showing Pivot table for FY 22 Data' as Comment;

PIVOT data_fy22
    ON Description, name USING SUM(Data)
    GROUP BY Area ORDER BY Area;

SELECT 'Showing Pivot table for FY 22 Totals' as Comment;

PIVOT total_fy22
    ON Description, Note USING SUM(Data)
    GROUP BY Area;

PIVOT total_fy22
    ON Note USING SUM(Data)
    GROUP BY Description;
