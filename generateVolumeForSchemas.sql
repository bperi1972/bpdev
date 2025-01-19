-- Generate a single query to pull table data volumes for all schemas across all databases
-- Ensure you have sufficient privileges to query sys.tables and sys.dm_db_partition_stats in each database

DECLARE @SQL NVARCHAR(MAX);
SET @SQL = '';

-- Generate dynamic SQL for all databases
SELECT @SQL = @SQL +
'USE ' + QUOTENAME(name) + ';
INSERT INTO #AllTableData
SELECT
    DB_NAME() AS DatabaseName,
    s.name AS SchemaName,
    t.name AS TableName,
    p.[rows] AS NumRows, -- "rows" column replaced with "NumRows" to avoid reserved keyword issues
    SUM(a.total_pages) * 8 / 1024.0 AS DataSizeMB, -- Data size in MB
    SUM(a.used_pages) * 8 / 1024.0 AS UsedSpaceMB, -- Used space in MB
    SUM(a.total_pages - a.used_pages) * 8 / 1024.0 AS FreeSpaceMB -- Free space in MB
FROM
    sys.tables t
INNER JOIN
    sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN
    sys.indexes i ON t.object_id = i.object_id
INNER JOIN
    sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
INNER JOIN
    sys.allocation_units a ON p.partition_id = a.container_id
WHERE
    t.is_ms_shipped = 0 -- Exclude system tables
GROUP BY
    s.name, t.name, p.[rows];
'
FROM sys.databases
WHERE state = 0 -- Exclude offline databases
  AND name NOT IN ('master', 'tempdb', 'model', 'msdb'); -- Exclude system databases

-- Create a temporary table to store results from all databases
CREATE TABLE #AllTableData (
    DatabaseName NVARCHAR(128),
    SchemaName NVARCHAR(128),
    TableName NVARCHAR(128),
    NumRows BIGINT,
    DataSizeMB FLOAT,
    UsedSpaceMB FLOAT,
    FreeSpaceMB FLOAT
);

-- Execute the dynamic SQL
EXEC sp_executesql @SQL;

-- Query consolidated results
SELECT * FROM #AllTableData ORDER BY DatabaseName, NumRows DESC;

-- Drop the temporary table
DROP TABLE #AllTableData;

-- Once the query is executed, you can export the combined results to Excel by right-clicking in the Results Pane -> Save Results As -> CSV or Excel.
