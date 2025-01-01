import pandas as pd
import json
import os
import logging

# configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
        logging.FileHandler("script_generator.log"),
        logging.StreamHandler()
    ]
)

# Columns in SF but not in Parquet
# Columns in Default Metadata but not in Parquet
# Columns in Parquet but not in SF
# Datatype Mis-Match between SF and Parquet
def colsInSFButNotInParquet(sf_data, pq_data, exceptionFilePath):
    df_colsInSFButNotInParquet = pd.merge(sf_data, pq_data[["Entity Logical Name", "Logical Name", "Parquet Column Id"]], on=["Entity Logical Name", "Logical Name"], how="left")
    df_to_out = df_colsInSFButNotInParquet[["Entity Logical Name", "Logical Name", "Parquet Column Id"]]
    filtered_df_to_out = df_to_out[df_to_out['Parquet Column Id'].isna()]

    with pd.ExcelWriter(exceptionFilePath, engine='openpyxl', mode='w') as writer:
        filtered_df_to_out = df_to_out[df_to_out['Parquet Column Id'].isna()]
        filtered_df_to_out.to_excel(writer, index=False, sheet_name="In SF Not in PQ")

def colsInSFButNotInParquetExcludingVirtualColumns(sf_data, pq_data, exceptionFilePath):
    sf_data_fil = sf_data.loc[sf_data['Attribute Type'] != 'Virtual']
    df_colsInSFButNotInParquet = pd.merge(sf_data_fil, pq_data[["Entity Logical Name", "Logical Name", "Parquet Column Id"]], on=["Entity Logical Name", "Logical Name"], how="left")
    df_to_out = df_colsInSFButNotInParquet[["Entity Logical Name", "Logical Name", "Parquet Column Id"]]

    with pd.ExcelWriter(exceptionFilePath, engine='openpyxl', mode='a') as writer:
        filtered_df_to_out = df_to_out[df_to_out['Parquet Column Id'].isna()]
        filtered_df_to_out.to_excel(writer, index=False, sheet_name="In SF Not in PQ Ex Virtual")
    
def colsInDefaultButNotInParquet(defaultMetadata, pq_data, exceptionFilePath):
    tableList = pq_data['Entity Logical Name'].unique()

    missing_columns_report = []
    for table in tableList:
        pq_columns = pq_data[pq_data['Entity Logical Name'] == table]['Logical Name'].str.lower().tolist()

        def_columns = defaultMetadata['Logical Name'].str.lower().tolist()

        missing_columns = set(def_columns) - set(pq_columns)

        for col in missing_columns:
            missing_columns_report.append({'Entity Logical Name': table, "Logical Name": col})
        df_to_excel = pd.DataFrame(missing_columns_report)

    with pd.ExcelWriter(exceptionFilePath, engine='openpyxl', mode='a') as writer:
        df_to_excel.to_excel(writer, index=False, sheet_name='Missing Columns in Parquet')
    
def colsInParquetButNotInSalesforce(sf_data, pq_data, exceptionFilePath):
    None

def datatypeMisMatchSFParquet(sf_data, pq_data, exceptionFilePath):
    None

def main():
    # global variables
    # sf_excel_file = 'SF_Excel.xlsx'
    # pq_excel_file = 'PQ_Excel.xlsx'
    # output_folder = 'output'

    # read the Salesforce excel file
    logging.info('Reading the excel files')
    sf_df = pd.DataFrame()
    pq_df = pd.DataFrame()
    defCols_df = pd.DataFrame()

    try:
        with open(r'C:\Users\BPeri\Downloads\gitcode\bpdev\SalesForce_Script_Creation_Config.json', "r") as configFile:
            config = json.load(configFile)
        
        try:
            sf_df = pd.read_excel(config["excelFilePath"], sheet_name='Metadata')
        except Exception as e_sf:
            logging.critical(f"Error {e_sf} occurred while accessing the file {config["excelFilePath"]}")
            raise

        try:
            pq_df = pd.read_excel(config["parquetFilePath"], sheet_name='Parquet_Metadata')
        except Exception as e_pq:
            logging.critical(f"Error {e_pq} occurred while accessing the file {config["parquetFilePath"]}")
            raise

        try:
            defCols_df = pd.read_excel(config["parquetFilePath"], sheet_name='Default Metadata')
        except Exception as e_pq_def:
            logging.critical(f"Error {e_pq_def} occurred while accessing the file {config["parquetFilePath"]}")
            raise

        colsInSFButNotInParquet(sf_df, pq_df, config["exceptionFilePath"])
        colsInSFButNotInParquetExcludingVirtualColumns(sf_df, pq_df, config["exceptionFilePath"])
        colsInDefaultButNotInParquet(defCols_df, pq_df, config["exceptionFilePath"])
    except Exception as e:
        logging.critical(f"Critical error in main: {e}")
        raise

if __name__ == "__main__":
    main()