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

def populateEntityColumnList(df, entityName, parquetMetadata, defaultMetadata, df_default_col_n_types, df_parquet):
    try:
        filtered_df = df[df['Entity Logical Name'] == entityName]
        specificColumnsList = [
            f"{row['Logical Name']} {row['Derived Data Type']}"
            for _, row in filtered_df.iterrows()
        ]
        #print(f"The columns in the Entity list are {specificColumnsList}")
        return specificColumnsList
    except Exception as e:
        logging.error(f"Error in populateEntityColumnList for entity {entityName}: {e}")

def populateNonSynapseDefaultColumnList(df, entityName, df_default_col_n_types, df_parquet):
    try:
        filtered_df = df_parquet[df_parquet['Entity Logical Name'] == entityName][['Logical Name']]
        merged_df = pd.merge(df_default_col_n_types[["Logical Name", "Default Data Type"]], filtered_df, on="Logical Name", how='left', indicator= True)
        merged_filtered_df = merged_df[merged_df['_merge'] == 'both']
        applicableDefaultColumnList = [
            f"{row['Logical Name']} {row['Default Data Type']}"
            for _, row in merged_filtered_df.iterrows()
        ]
        return applicableDefaultColumnList
    except Exception as e:
        logging.error(f"Error in populateNonSynapseDefaultColumnList for entity {entityName}: {e}")


def populateSynapseDefaultColumnList(df):
    try:
        SynapseDefaultColumnList = [
            f"{row['Logical Name']} {row['Default Data Type']}"
            for _, row in df.iterrows()
        ]
        return SynapseDefaultColumnList
    except Exception as e:
        logging.error(f"Error in SynapseDefaultColumnList generation")

def createExternalTable(
    tableName,
    specificColumnsList=None,
    nonSynapseDefaultColumnList=None,
    synapseDefaultColumnList=None,
    schemaName=None,
    dataSource=None,
    fileFormat=None,
    locationPrefix=None
):
    try:
        parquet_file_location = f"{locationPrefix}/{tableName}_partitioned/PartitionId=*/*.snappy.parquet"

        parquetMetadata = addParquetCreationMetadata()
        #defaultMetadata = addDefaultMetadata()

        if specificColumnsList:
            formattedSpecificColumns = ',\n\t\t'.join(specificColumnsList)
        else:
            formattedSpecificColumns = ''

        if synapseDefaultColumnList:
            formattedSynapseDefaultColumnList = ',\n\t\t'.join(synapseDefaultColumnList)
        else:
            formattedSynapseDefaultColumnList = ''

        if nonSynapseDefaultColumnList:
            formattedNonSynapseDefaultColumnList = ',\n\t\t'.join(nonSynapseDefaultColumnList)
        else:
            formattedNonSynapseDefaultColumnList = ''
        

        query = f"""
CREATE EXTERNAL TABLE {schemaName}.[{tableName}_raw] 
(
\t\t/** Parquet Creation Metadata **/

\t\t{formattedSynapseDefaultColumnList},

\t\t/** Data **/
\t\t/** Default Metadata **/

\t\t{formattedNonSynapseDefaultColumnList},

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

def createViewOnExternalTable(tableName, allColumnsList, schemaName="d365"):
    try:
        allColumns = [column.split()[0] for column in allColumnsList]
        if allColumns:
            formattedAllColumns = ',\n\t\t'.join(allColumns)
            formattedAllColumnInner  = ',\n\t\t\t\t'.join(allColumns)
        else:
            formattedAllColumns = ''
            formattedAllColumnsInner = ''
        
        print(f"All columns selected are {formattedAllColumns}")
        query = f"""
CREATE VIEW {schemaName}.{tableName} 
AS
SELECT 
\t\t{formattedAllColumns} 
  FROM 
    (
        SELECT  {formattedAllColumnInner},
\t\t\t\tROW_NUMBER() OVER (PARTITION BY Id ORDER BY versionnumber DESC) as _row_id
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

def writeScripts(config, 
                 DynamicsCEExcelFilePath, DynamicsCEMetadataSheetName,
                 ParquetExcelFilePath, ParquetMetadataSheetName,
                 DefaultMetadataExcelFilePath, SynapseDefaultMetadataSheetName, NonSynapseDefaultMetadataSheetName,
                 allScriptsInOne=False):
    try:
        output_directory = config.get("outputDirectory", "./")

        # Ensure output directory exists
        os.makedirs(output_directory, exist_ok=True)

        # Populate the Metadata from the Sales Force Excel File
        df_excel = pd.read_excel(DynamicsCEExcelFilePath, sheet_name=DynamicsCEMetadataSheetName)

        df = df_excel.loc[df_excel['Attribute Type'] != 'Virtual']
        df_parquet = pd.read_excel(ParquetExcelFilePath, sheet_name=ParquetMetadataSheetName)
        df = pd.merge(df, df_parquet[["Entity Logical Name", "Logical Name", "Parquet Data Type"]], on=["Entity Logical Name", "Logical Name"], how='right')
        df_non_synapse_default_col_n_types = pd.read_excel(DefaultMetadataExcelFilePath, sheet_name=NonSynapseDefaultMetadataSheetName)
        df_synapse_default_col_n_types = pd.read_excel(DefaultMetadataExcelFilePath, sheet_name=SynapseDefaultMetadataSheetName)
        df[["Derived Data Type", "Size", "Precision"]] = df.apply(extractDataType, axis=1)

        parquetMetadata = addParquetCreationMetadata()
        defaultMetadata = addDefaultMetadata()


        # Filter default and parquet metadata columns
        df = df[~df["Logical Name"].str.lower().isin([col.split(' ', 1)[0].lower().strip() for col in defaultMetadataToExclusionList()])]

        combinedExternalScript = ""
        combinedViewScript = ""

        for table in config["tables"]:
            tableName = table["tableName"]
            specificColumnsList = populateEntityColumnList(df, tableName, parquetMetadata, defaultMetadata, df_non_synapse_default_col_n_types, df_parquet)
            nonSynapseDefaultColumnList = populateNonSynapseDefaultColumnList(df, tableName, df_non_synapse_default_col_n_types, df_parquet)
            synapseDefaultColumnList = populateSynapseDefaultColumnList(df_synapse_default_col_n_types)
            allColumnsList = synapseDefaultColumnList + nonSynapseDefaultColumnList + specificColumnsList
            externalTableScript = createExternalTable(
                tableName=tableName,
                specificColumnsList=specificColumnsList,
                nonSynapseDefaultColumnList=nonSynapseDefaultColumnList,
                synapseDefaultColumnList=synapseDefaultColumnList,
                schemaName=config["schemaName"],
                dataSource=config["dataSource"],
                fileFormat=config["fileFormat"],
                locationPrefix=config["locationPrefix"]
            )

            viewScript = createViewOnExternalTable(
                tableName=tableName,
                allColumnsList=allColumnsList,
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
        column_type = row["Attribute Type"]
        parquet_column_data_type = row["Parquet Data Type"]
        additional_data = str(row["Additional data"])

        data_type = None
        size = None
        precision = None

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
            data_type = f"NVARCHAR({size})" if size else "NVARCHAR(50)"
        elif column_type == "Virtual":
            data_type = "VARCHAR(50)"
        else:
            data_type = "VARCHAR(50)"
        
        return pd.Series([data_type, size, precision])
    except Exception as e:
        logging.error(f"Error in extractDataType for row {row}: {e}")
        raise


def main():
    try:
        with open(r'C:\Users\BPeri\Downloads\gitcode\bpdev\SalesForce_Script_Creation_Config.json', "r") as configFile:
            config = json.load(configFile)

        writeScripts(config, 
                     DynamicsCEExcelFilePath=config["DynamicsCEExcelFilePath"],
                     DynamicsCEMetadataSheetName=config["DynamicsCEMetadataSheetName"], 
                     ParquetExcelFilePath=config["ParquetExcelFilePath"], 
                     ParquetMetadataSheetName=config["ParquetMetadataSheetName"], 
                     DefaultMetadataExcelFilePath=config["DefaultMetadataExcelFilePath"], 
                     SynapseDefaultMetadataSheetName=config["SynapseDefaultMetadataSheetName"], 
                     NonSynapseDefaultMetadataSheetName=config["NonSynapseDefaultMetadataSheetName"], 
                     allScriptsInOne=config.get("allScriptsInOne", False))
    except Exception as e:
        logging.critical(f"Critical error in main: {e}")
        raise

if __name__ == "__main__":
    main()
