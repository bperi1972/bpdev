"""
Script: External Table and View Script Generator
Author: Bhaskar Peri
Version: 1.0.0
Date: 26-DEC-2024

Description:
This script reads metadata from an Excel file and generates SQL scripts for creating external tables
and views dynamically. It supports configurable prefixes, suffixes, and output options for generating
individual or combined scripts.

Version History:
- 1.0.0: Initial version with dynamic script generation and JSON configuration support.

Considerations for Improvement
1. The metadata for parquet and default columns can be moved out of the code and passed as a parameter or a config file

"""
import pandas as pd
import re
import json
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("script_generator.log"),
        logging.StreamHandler()
    ]
)

# Function to generate Parquet metadata columns
def addParquetCreationMetadata(custom_columns=None):
    try:
        parquetMetadata = [
            '[Id] VARCHAR(50)',
            'SinkCreatedOn VARCHAR(50)',
            'SinkModifiedOn VARCHAR(50)',
            'versionnumber BigInt',
            'IsDelete VARCHAR(10)',
            'createdonpartition VARCHAR(50)',
            'uniquedscid VARCHAR(50)'
        ]

        if custom_columns:
            parquetMetadata.extend(custom_columns)

        return ',\n\t\t'.join(parquetMetadata)
    except Exception as e:
        logging.error(f"Error in addParquetCreationMetadata: {e}")
        raise


def addDefaultMetadata(custom_metadata=None):
    try:
        defaultMetadata = [
            'statecode INTEGER',
            'statuscode INTEGER',
            'createdby VARCHAR(50)',
            'createdby_entitytype VARCHAR(100)',
            'createdonbehalfby VARCHAR(50)',
            'createdonbehalfby_entitytype VARCHAR(100)',
            'modifiedby VARCHAR(50)',
            'modifiedby_entitytype VARCHAR(100)',
            'modifiedonbehalfby VARCHAR(50)',
            'modifiedonbehalfby_entitytype VARCHAR(100)',
            'organizationid VARCHAR(50)',
            'organizationid_entitytype VARCHAR(100)',
            'createdbyname VARCHAR(100)',
            'createdbyyominame VARCHAR(100)',
            'createdon VARCHAR(50)',
            'createdonbehalfbyname VARCHAR(100)',
            'createdonbehalfbyyominame VARCHAR(100)',
            'modifiedbyname VARCHAR(100)',
            'modifiedbyyominame VARCHAR(100)',
            'modifiedon VARCHAR(50)',
            'modifiedonbehalfbyname VARCHAR(100)',
            'modifiedonbehalfbyyominame VARCHAR(100)',
            'entityimage_timestamp VARCHAR(50)',
            'entityimage_url VARCHAR(200)',
            'entityimageid VARCHAR(50)',
            'importsequencenumber INTEGER',
            'overriddencreatedon VARCHAR(50)'
        ]

        if custom_metadata:
            defaultMetadata.extend(custom_metadata)

        return ',\n\t\t'.join(defaultMetadata)
    except Exception as e:
        logging.error(f"Error in addDefaultMetadata: {e}")
        raise

def defaultMetadataToExclusionList():
    try:
        exclusion_list = [
            'StateCode',
            'StatusCode',
            'CreatedBy',
            'CreatedBy_EntityType',
            'CreatedOnBehalfBy',
            'CreatedOnBehalfBy_EntityType',
            'ModifiedBy',
            'ModifiedBy_EntityType',
            'ModifiedOnBehalfBy',
            'ModifiedOnBehalfBy_EntityType',
            'OrganizationId',
            'OrganizationId_EntityType',
            'CreatedByName',
            'CreatedByYomiName',
            'CreatedOn',
            'CreatedOnBehalfByName',
            'CreatedOnBehalfByYominame',
            'ModifiedByName',
            'ModifiedByYomiName',
            'ModifiedOn',
            'ModifiedOnBehalfByName',
            'ModifiedOnBehalfByYomiName',
            'EntityImage_Timestamp',
            'EntityImage_Url',
            'EntityImageid',
            'ImportSequenceNumber',
            'OverriddenCreatedOn',
            'id',
            'SinkCreatedOn',
            'SinkModifiedOn',
            'VersionNumber',
            'isDelete',
            'CreatedOnPartition',
            'UniqueDscId'
        ]
    except Exception as e:
        logging.error(f"Error while populating the defaultMetadataExlusionList")
    return exclusion_list

def populateEntityColumnList(df, entityName, parquetMetadata, defaultMetadata):
    try:
        filtered_df = df[df['Entity Logical Name'] == entityName]
        specificColumnsList = [
            f"{row['Logical Name']} {row['Derived Data Type']}"
            for _, row in filtered_df.iterrows()
        ]
        return specificColumnsList
    except Exception as e:
        logging.error(f"Error in populateEntityColumnList for entity {entityName}: {e}")


def createExternalTable(
    tableName,
    specificColumnsList=None,
    schemaName=None,
    dataSource=None,
    fileFormat=None,
    locationPrefix=None
    # schemaName="d365",
    # dataSource="ExternalConnection_DynamicsCE_ADL",
    # fileFormat="ParquetFileFormat",
    # locationPrefix="deltalake"
):
    try:
        parquet_file_location = f"{locationPrefix}/{tableName}_partitioned/PartitionId=*/*.snappy.parquet"

        parquetMetadata = addParquetCreationMetadata()
        defaultMetadata = addDefaultMetadata()

        if specificColumnsList:
            formattedSpecificColumns = ',\n\t\t'.join(specificColumnsList)
        else:
            formattedSpecificColumns = ''

        query = f"""
CREATE EXTERNAL TABLE {schemaName}.[{tableName}_raw] 
(
\t\t/** Parquet Creation Metadata **/

\t\t{parquetMetadata},

\t\t/** Data **/
\t\t/** Default Metadata **/

\t\t{defaultMetadata},

\t\t/** Entity Specific Metadata **/

\t\t{formattedSpecificColumns}
)
WITH (
    DATA_SOURCE = {dataSource},
    LOCATION = N'{parquet_file_location}',
    FILE_FORMAT = {fileFormat},
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
)

GO
"""

        return query
    except Exception as e:
        logging.error(f"Error in createExternalTable for table {tableName}: {e}")
        raise

def createViewOnExternalTable(tableName, schemaName="d365"):
    try:
        query = f"""
CREATE VIEW {schemaName}.{tableName} 
AS
SELECT * 
  FROM 
    (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY Id ORDER BY versionnumber DESC) as _row_id
          FROM {schemaName}.[{tableName}_raw]
    ) AS A
 WHERE A._row_id = 1
   AND A.IsDelete IS NULL

GO
"""
        return query
    except Exception as e:
        logging.error(f"Error in createViewOnExternalTable for table {tableName}: {e}")
        raise

def writeScripts(config, excelFilePath, parquetFilePath, allScriptsInOne=False):
    try:
        output_directory = config.get("outputDirectory", "./")

        # Ensure output directory exists
        os.makedirs(output_directory, exist_ok=True)

        # Populate the Metadata from the Sales Force Excel File
        df_excel = pd.read_excel(excelFilePath, sheet_name='Metadata')

        df = df_excel.loc[df_excel['Attribute Type'] != 'Virtual']
        df_parquet = pd.read_excel(parquetFilePath, sheet_name='Parquet_Metadata')
        df = pd.merge(df, df_parquet[["Entity Logical Name", "Logical Name", "Parquet Data Type"]], on=["Entity Logical Name", "Logical Name"], how='right')
        df_default_col_n_types = pd.read_excel(parquetFilePath, sheet_name='Default Metadata Cols with Type')

        df[["Derived Data Type", "Size", "Precision"]] = df.apply(extractDataType, axis=1)
        # df[["Parquet Data Type"]] = df.apply(extractParquetDataType, axis=1)

        parquetMetadata = addParquetCreationMetadata()
        defaultMetadata = addDefaultMetadata()


        # Filter default and parquet metadata columns
        df = df[~df["Logical Name"].str.lower().isin([col.split(' ', 1)[0].lower().strip() for col in defaultMetadataToExclusionList()])]

        combinedExternalScript = ""
        combinedViewScript = ""

        for table in config["tables"]:
            tableName = table["tableName"]
            specificColumnsList = populateEntityColumnList(df, tableName, parquetMetadata, defaultMetadata)

            externalTableScript = createExternalTable(
                tableName=tableName,
                specificColumnsList=specificColumnsList,
                schemaName=config["schemaName"],
                dataSource=config["dataSource"],
                fileFormat=config["fileFormat"],
                locationPrefix=config["locationPrefix"]
            )

            viewScript = createViewOnExternalTable(
                tableName=tableName,
                schemaName=config["schemaName"]
            )

            if allScriptsInOne:
                combinedExternalScript += f"\n{externalTableScript}\n"
                combinedViewScript += f"\n{viewScript}\n"
            else:
                externalTableFileName = os.path.join(
                    output_directory,
                    f"{config['tableScriptPrefix']}{tableName}{config['tableScriptSuffix']}.sql"
                )
                viewFileName = os.path.join(
                    output_directory,
                    f"{config['viewScriptPrefix']}{tableName}{config['viewScriptSuffix']}.sql"
                )

                with open(externalTableFileName, "w") as tableFile:
                    tableFile.write(externalTableScript)

                with open(viewFileName, "w") as viewFile:
                    viewFile.write(viewScript)

        if allScriptsInOne:
            combinedExternalFile = os.path.join(output_directory, config["combinedExternalTableScriptName"])
            combinedViewFile = os.path.join(output_directory, config["combinedViewScriptName"])

            with open(combinedExternalFile, "w") as combinedFile:
                combinedFile.write(combinedExternalScript)

            with open(combinedViewFile, "w") as combinedFile:
                combinedFile.write(combinedViewScript)
    except Exception as e:
        logging.error(f"Error in writeScripts: {e}")
        raise

def extractDataType(row):
    try:
        table_name = row["Entity Logical Name"]
        column_name = row["Logical Name"]
        column_type = row["Attribute Type"]
        parquet_column_data_type = row["Parquet Data Type"]
        additional_data = str(row["Additional data"])
        # print(f"The column name is {column_name} and the column_type is {column_type} and the additional data is {additional_data} and parquet data type is {parquet_column_data_type}")

        data_type = None
        size = None
        precision = None

        """ 
        BigInt - maps to bigint
        Choice - integer as it's an enumeration field - as per example provided.
            Currency - numeric
        Customer - lookup to a unique identifier - varchar(50)
        DateTime - varchar(50)
        Decimal - numeric
        Double - numeric
        EntityName  - varchar(50) - as per example provided.
        Lookup - varchar(50) - as per example provided.
        ManagedProperty - not on an entity of interest
            Multiline Text - nvarchar inline with metadata setting
        Owner - varchar(50) - as per example provided.
        PartyList - not on an entity of interest
        State - integer - as per example provided.
        Status- integer - as per example provided.
            Text - nvarchar inline with metadata setting.
        Two options - varchar(5)
        Uniqueidentifier - varchar(50)
        Virtual - ignore as per earlier email
        Whole number - integer 
        """

        if str(parquet_column_data_type) in ('bit'):
            data_type = 'INTEGER'
        elif column_type in ('BigInt', 'bigint'):
            data_type = "BIGINT"            
        elif str(parquet_column_data_type).upper() in ('VARCHAR(8000)') and column_type not in ["Uniqueidentifier", "DateTime", "Text", "Multiline Text"]:
            data_type = 'VARCHAR(100)'
        elif str(parquet_column_data_type).upper() in ('FLOAT') or column_type in ["Double"]:
            data_type = 'FLOAT'            
        elif column_type in ["Choice", "State", "Status", "ManagedProperty", "Whole number"]:
            data_type = "INTEGER"
        elif column_type == "Currency":
            match = re.search(r"Precision:\s*(\d+)", additional_data)
            if match:
                precision = int(match.group(1))
            data_type = f"DECIMAL(38,{precision})"
        elif column_type in ["Decimal"]:
            match = re.search(r"Precision:\s*(\d+)", additional_data)
            if match:
                precision = int(match.group(1))
            data_type = f"DECIMAL(38,{precision})"
        elif column_type in ["Customer", "EntityName", "Lookup", "Owner", "Uniqueidentifier", "DateTime"]:
            data_type = "VARCHAR(50)"
        elif column_type == "Multiline Text":
            match = re.search(r"Max length:\s*(\d+)", additional_data)
            if match:
                size = int(match.group(1))
            if size > 8000:
                data_type = "VARCHAR(MAX)"
            else:
                data_type = f"NVARCHAR({size})" if size else "VARCHAR(50)"
        elif column_type == "PartyList":
            data_type = "VARCHAR(100)"                
        elif column_type == "Two Options":
            data_type = "VARCHAR(5)"
        elif column_type == "Text":
            match = re.search(r"Max length:\s*(\d+)", additional_data)
            if match:
                size = int(match.group(1))
            data_type = f"NVARCHAR({size})" if size else "VARCHAR(50)"
        elif column_type == "Virtual":
            data_type = "VARCHAR(50)"
        else:
            data_type = "VARCHAR(50)"
        print(f"The column name is {column_name} and the column_type is {data_type} and the size is {size} and parquet data type is {parquet_column_data_type}")
        return pd.Series([data_type, size, precision])
    except Exception as e:
        logging.error(f"Error in extractDataType for row {row}: {e}")
        raise


def main():
    try:
        with open(r'C:\Users\BPeri\Downloads\gitcode\bpdev\SalesForce_Script_Creation_Config.json', "r") as configFile:
            config = json.load(configFile)

        writeScripts(config, excelFilePath=config["excelFilePath"], parquetFilePath=config["parquetFilePath"], allScriptsInOne=config.get("allScriptsInOne", False))
    except Exception as e:
        logging.critical(f"Critical error in main: {e}")
        raise

if __name__ == "__main__":
    main()
