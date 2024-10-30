sql_statements = "".join([f"""
CREATE VIEW data_fy{str(year)[-2:]} AS SELECT
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
        year_codes.ViewName='fy{str(year)[-2:]}'
        AND Area != 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;

CREATE VIEW total_fy{str(year)[-2:]} AS SELECT
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
        year_codes.ViewName='fy{str(year)[-2:]}'
        AND Area = 'Wales'
    ORDER BY
        lookup_table.SortOrder, RefSortOrder;
""" for year in range(2012, 2023)])

# Saving the generated script to a .sql file
file_path = "./fiscal_year_views.sql"
with open(file_path, "w") as file:
    file.write(sql_statements)
