-- Script: Extract_Parquet_Schema.sql
-- Author: Bhaskar Peri
-- Date: 24-DEC-2024
-- Description: Dynamically extracts schema information from Parquet files and stores it in a temporary table.

-- Drop existing temporary tables (if any)
IF OBJECT_ID('tempdb..#parquet_table_list') IS NOT NULL DROP TABLE #parquet_table_list;
IF OBJECT_ID('tempdb..#parquet_table_format') IS NOT NULL DROP TABLE #parquet_table_format;

-- Create temporary table to store table names
CREATE TABLE #parquet_table_list (
    parquet_table_name NVARCHAR(200)
);

-- Insert list of Parquet table names
INSERT INTO #parquet_table_list (parquet_table_name)
VALUES
    ('account'),
    ('contact'),
    ('email'),
    ('sharepointdocument'),
    ('systemuser'),
    ('team'),
    ('template'),
    ('transactioncurrency'),
    ('ukef_bankholiday'),
    ('ukef_calculationversion'),
    ('ukef_claim'),
    ('ukef_claimhistory'),
    ('ukef_claimpartybalance'),
    ('ukef_claimpayment'),
    ('ukef_claims_recovery'),
    ('ukef_cnumber'),
    ('ukef_country'),
    ('ukef_deal'),
    ('ukef_expense'),
    ('ukef_facility'),
    ('ukef_interestperiod'),
    ('ukef_obligation'),
    ('ukef_otheramount'),
    ('ukef_paymenttype'),
    ('ukef_product'),
    ('ukef_relateddocument'),
    ('ukef_ukefbankaccount'),
    ('ukef_ultimateobligor');

-- Create temporary table to store schema information
CREATE TABLE #parquet_table_format (
    parquet_table_name NVARCHAR(200),
    parquet_column_order INTEGER,
    parquet_column_name VARCHAR(200),
    parquet_column_type VARCHAR(200)
);

-- Declare variable for table name
DECLARE @table_name NVARCHAR(255);

-- Cursor to iterate over table names
DECLARE cur_table_name CURSOR FOR 
SELECT parquet_table_name FROM #parquet_table_list;

OPEN cur_table_name;
FETCH NEXT FROM cur_table_name INTO @table_name;

WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
        -- Define Parquet file path
        DECLARE @ParquetFilePath NVARCHAR(MAX) = CONCAT('deltalake/', @table_name, '_partitioned/PartitionId=*/*.snappy.parquet');
        
        -- Insert schema information into the temporary table
        INSERT INTO #parquet_table_format (parquet_table_name, parquet_column_order, parquet_column_name, parquet_column_type)
        SELECT
            @table_name,
            column_ordinal AS parquet_column_order,
            name AS parquet_column_name,
            system_type_name AS parquet_column_type
        FROM sys.dm_exec_describe_first_result_set(
            N'SELECT * FROM OPENROWSET(
                BULK ''' + @ParquetFilePath + ''',
                FORMAT = ''PARQUET'',
                DATA_SOURCE = ''ExternalConnection_DynamicsCE_ADL''
            ) AS [ParquetData]', NULL, 0
        );
    END TRY
    BEGIN CATCH
        PRINT 'Error processing table: ' + @table_name + ' - ' + ERROR_MESSAGE();
    END CATCH;

    FETCH NEXT FROM cur_table_name INTO @table_name;
END;

CLOSE cur_table_name;
DEALLOCATE cur_table_name;

-- Output the final schema information
SELECT * FROM #parquet_table_format;

-- clean-up
DROP TABLE #parquet_table_list;
DROP TABLE #parquet_table_format;
