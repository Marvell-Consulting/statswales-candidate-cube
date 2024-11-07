-- CREATE FACT TABLE
CREATE TABLE fact_table AS FROM 'data/wimd1901_data.csv';

-- ADD LOOK UP TABLES FOR Measure and RowRef
CREATE TABLE datatype_lookup AS FROM 'user-lookup-tables/wimd1901_DataType.csv';
CREATE TABLE dlookup AS FROM 'user-lookup-tables/wimd1901_Domain.csv';

-- ADD LOOKUP TABLE FOR YearCode (This is a type of reference data)
-- Don't add this here because there's no time data
-- CREATE TABLE year_codes AS FROM 'system-lookup-tables/year_codes.csv';

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

-- Fact Table:      │ fact │ value     │  Area_id   │ DataType_id │ Domain_id   │
-- datatype_lookup: │ item │ hierarchy │ sort_order │  lang       │ description │  notes  │
-- dlookup:         │ item │ hierarchy │ sort_order │  lang       │ description │  notes  │
CREATE VIEW data AS SELECT
    fact_table.fact as fact,
    fact_table.value as data,
    reference_data_info.description as area,
    datatype_lookup.description as type,
    dlookup.description as domain,
    fact_table.DataType_id as raw_type
FROM
    fact_table
LEFT JOIN reference_data ON fact_table.Area_id=reference_data.item_id
LEFT JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id AND reference_data_info.lang='en-gb'
LEFT JOIN datatype_lookup ON fact_table.DataType_id=datatype_lookup.item AND datatype_lookup.lang='en-gb'
LEFT JOIN dlookup ON fact_table.Domain_id=dlookup.item AND dlookup.lang='en-gb';
CREATE VIEW rank AS SELECT * FROM data WHERE raw_type='RANK';
CREATE VIEW dec AS SELECT * FROM data WHERE raw_type='DEC';
CREATE VIEW quin AS SELECT * FROM data where raw_type='QUIN';

PIVOT rank
on domain using SUM(data)
group by area;