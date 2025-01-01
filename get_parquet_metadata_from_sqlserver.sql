DROP TABLE #parquet_table_list
GO

CREATE TABLE #parquet_table_list
(
	parquet_table_name NVARCHAR(200)
)
GO
INSERT INTO #parquet_table_list
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
	('ukef_ultimateobligor')
GO

DROP TABLE #parquet_table_format
GO

CREATE TABLE #parquet_table_format
(
	parquet_table_name NVARCHAR(200),
	parquet_column_order INTEGER,
	parquet_column_name VARCHAR(200),
	parquet_column_type VARCHAR(200)
)

GO

DECLARE @table_name NVARCHAR(255)

DECLARE cur_table_name CURSOR FOR 
SELECT parquet_table_name 
FROM #parquet_table_list

OPEN cur_table_name
FETCH NEXT FROM cur_table_name INTO @table_name

WHILE @@FETCH_STATUS = 0
BEGIN
	DECLARE @ParquetFilePath NVARCHAR(MAX) = 'deltalake/'+ @table_name +'_partitioned/PartitionId=*/*.snappy.parquet'
	INSERT INTO #parquet_table_format
	(
		parquet_table_name, parquet_column_order, parquet_column_name, parquet_column_type
	)
	SELECT
		@table_name,
		column_ordinal as ColOrder,
		name as ColName,
		system_type_name as dtype
	  FROM sys.dm_exec_describe_first_result_set
			(
				'SELECT * FROM OPENROWSET(
					BULK ''' + @ParquetFilePath + ''',
					FORMAT = ''PARQUET'',
					DATA_SOURCE = ''ExternalConnection_DynamicsCE_ADL''
					) AS [ParquetData]',
					NULL, 0
			);
	FETCH NEXT FROM cur_table_name INTO @table_name
	--print(@table_name
END

CLOSE cur_table_name
DEALLOCATE cur_table_name

GO

SELECT * FROM #parquet_table_format