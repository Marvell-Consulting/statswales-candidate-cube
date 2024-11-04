-- CREATE FACT TABLE
CREATE TABLE fact_table AS FROM 'tran0303_data.parquet';

-- Add lookup tables/dimensions
CREATE TABLE vehicle_types AS FROM 'lookup-tables/tran0303_TypeofVehicle.csv';
CREATE TABLE severity AS FROM 'lookup-tables/tran0303_Severity.csv';
CREATE TABLE road_type AS FROM 'lookup-tables/tran0303_Byhighwayagency.csv';
CREATE TABLE time_types AS FROM 'lookup-tables/tran0303_TimeofDay.csv';
CREATE TABLE areas AS FROM 'lookup-tables/tran0303_Area.csv'; -- <- This does not appear in our reference data

-- ADD LOOKUP TABLE FOR date information (This is a type of reference data)
CREATE TABLE day_codes AS FROM 'system-lookup-tables/tran0303_Day.csv';
CREATE TABLE date_codes AS FROM 'system-lookup-tables/tran0303_Date.csv';

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
DELETE FROM reference_data WHERE item_id NOT IN (SELECT DISTINCT Area_id FROM fact_table);
DELETE FROM reference_data_info WHERE item_id NOT IN (SELECT item_id FROM reference_data);
DELETE FROM category_keys WHERE category_key NOT IN (SELECT category_key FROM reference_data);
DELETE FROM category_Key_info WHERE category_key NOT IN (select category_key FROM category_keys);
DELETE FROM categories where category NOT IN (SELECT category FROM category_keys);
DELETE FROM category_info WHERE category NOT IN (SELECT category FROM categories);
DELETE FROM hierarchy WHERE item_id NOT IN (SELECT item_id FROM reference_data);

-- Create Views for the Data based on year

PIVOT (select
     areas.description AS area,
     day_codes.description AS day_of_week,
     date_codes.year AS year,
     date_codes.description as Quarter,
     vehicle_types.description AS vehicle,
     severity.description as severity_type,
     time_types.description as time_of_day,
     road_type.description as road_type,
     fact_table.value as data
 from fact_table
 left join areas on fact_table.area_id=areas.item and areas.lang='en-gb'
 left join day_codes on fact_table.day_id=day_codes.item AND day_codes.lang='en-gb'
 left join date_codes on fact_table.date_id=date_codes.item AND date_codes.lang='en-gb'
 left join vehicle_types on fact_table.TypeofVehicle_id=vehicle_types.item AND vehicle_types.lang='en-gb'
 left join severity on fact_table.Severity_id=severity.item AND severity.lang='en-gb'
 left join time_types on fact_table.TimeofDay_id=time_types.item AND time_types.lang='en-gb'
 left join road_type on fact_table.Byhighwayagency_id=road_type.item AND road_type.lang='en-gb'
 WHERE
     day_of_week='Total'
     AND quarter='Total'
     AND vehicle='All road users'
     AND time_of_day='Total'
     --AND severity_type='All severities'
     AND year=1996)
ON severity_type USING SUM(data)
GROUP BY Area
;
