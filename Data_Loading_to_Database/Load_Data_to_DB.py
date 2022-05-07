import glob
import os

import pandas as pd
import pyodbc
from fast_to_sql import fast_to_sql as fts



def load_data_to_db():
    ### source csv files
    idw_bal_csv = r'C:\InventoryBalanceData\INV_merged_files\IDW_Balance_Enquiry.csv'
    idw_cost_csv = r'C:\InventoryBalanceData\INV_merged_files\IDW_Cost_Enquiry.csv'
    idw_gross_req = r'C:\InventoryBalanceData\INV_merged_files\IDW_Gross_Forecast.csv'

    vmi_folder = "C:\\InventoryBalanceData\\"
    vmi_rothenburg = glob.glob(os.path.join(vmi_folder, "VMI_toyota_rothenburg*"))

    sap_ibp_folder = "C:\\InventoryBalanceData\\SAP IBP Tables\\"
    sap_ibp_supp_code = glob.glob(os.path.join(sap_ibp_folder, "SAP_IBP_Supplier_Code*"))
    sap_ibp_supp_name = glob.glob(os.path.join(sap_ibp_folder, "SAP_IBP_Supplier_Description*"))
    sap_ibp_supp_idco = glob.glob(os.path.join(sap_ibp_folder, "SAP_IBP_IDCO*"))
    sap_ibp_cost = glob.glob(os.path.join(sap_ibp_folder, "SAP_IBP_Cost*"))

    sap_ibp_suppliers = glob.glob(os.path.join(sap_ibp_folder, "SAP_IBP_Suppliers*"))

    # fora = r'C:\Users\BurikMyk\OneDrive - Electrolux\Documents\FORA_ZS_202102.xlsx'
    # fora_df = pd.read_excel(fora)
    # cmdt = r'C:\Users\BurikMyk\OneDrive - Electrolux\Documents\CMDT.xlsx'
    # cmdt_df = pd.read_excel(cmdt)

    arrivals_report = glob.glob(os.path.join(sap_ibp_folder, "Arrivals_report*"))
    dcde_report = glob.glob(os.path.join(sap_ibp_folder, "DCDE3*"))

    # Created a pyodbc connection to db electrolux
    conn = pyodbc.connect('DRIVER={SQL Server};'
                          'SERVER=DT-SupplyPlanning-EU.biz.electrolux.com;Trusted_Connection=no;'
                          'Database=SupplyPlanning_Test;'
                          'UID=supply_admin;'
                          'PWD={i)Z\#6&;~fwJNZ7e}')

    ### source csv files opened as dataframes
    df_idw_bal = pd.read_csv(idw_bal_csv,  low_memory=False)
    df_vmi_rtg = pd.read_csv(vmi_rothenburg[0],  low_memory=False)
    df_arrivals_report = pd.read_csv(arrivals_report[0],  low_memory=False)
    df_idw_cost = pd.read_csv(idw_cost_csv, low_memory=False)
    df_sap_ibp_supp_code = pd.read_csv(sap_ibp_supp_code[0], low_memory=False)
    df_sap_ibp_supp_name = pd.read_csv(sap_ibp_supp_name[0], low_memory=False)
    df_sap_ibp_supp_idco = pd.read_csv(sap_ibp_supp_idco[0], low_memory=False)
    df_sap_ibp_cost = pd.read_csv(sap_ibp_cost[0], low_memory=False)
    df_sap_ibp_dcde = pd.read_csv(dcde_report[0], low_memory=False)
    df_sap_ibp_suppliers = pd.read_csv(sap_ibp_suppliers[0], low_memory=False)

    df_idw_gross_forecast = pd.read_csv(idw_gross_req, low_memory=False)

    cursor = conn.cursor()

    # Replace(or Create) and insert to temporary table data from IDW_Balance_enquiry.csv
    insert_db_temp_data_idw_bal = fts.fast_to_sql(df_idw_bal, "temp_data_IDW_balance", conn, if_exists="replace",
                                                  temp=False)
    print("df_idw_bal done")
    ### Replace(or Create) and insert to temporary table data from IDW_Cost_enquiry.csv
    insert_db_temp_data_idw_cost = fts.fast_to_sql(df_idw_cost, "temp_data_IDW_cost", conn, if_exists="replace", temp=False)
    print("df_idw_cost done")
    ### Replace(or Create) and insert to temporary table data from SAP_IBP_Supplier_Code*.csv
    insert_db_temp_data_sap_ibp_supplier_code = fts.fast_to_sql(df_sap_ibp_supp_code, "temp_data_SAP_Supplier_Code", conn,
                                                                if_exists="replace", temp=False)
    print("df_sap_ibp_supp_code done")
    ### Replace(or Create) and insert to temporary table data from SAP_IBP_Supplier_Description*.csv
    ### i dont know why it was called 'supplier name' whenever in sap file it's 'description' :|
    insert_db_temp_data_sap_ibp_supplier_name = fts.fast_to_sql(df_sap_ibp_supp_name, "temp_data_SAP_Supplier_Description",
                                                                conn, if_exists="replace", temp=False)
    print("df_sap_ibp_supp_name done")
    # ### Replace(or Create) and insert to temporary table data from SAP_IBP_IDCO*.csv
    insert_db_temp_data_sap_ibp_supplier_idco = fts.fast_to_sql(df_sap_ibp_supp_idco, "temp_data_SAP_Supplier_IDCO", conn,
                                                                if_exists="replace", temp=False)
    print("df_sap_ibp_supp_idco done")
    ### Replace(or Create) and insert to temporary table data from SAP_IBP_Cost*.csv
    insert_db_temp_data_sap_ibp_cost = fts.fast_to_sql(df_sap_ibp_cost, "temp_data_SAP_Cost", conn, if_exists="replace",
                                                       temp=False)
    print("df_sap_ibp_cost done")

    # # ## Replace(or Create) and insert to temporary table data from DDCDE3*.csv
    # insert_db_temp_data_dcde = fts.fast_to_sql(df_sap_ibp_dcde, "temp_data_DCDE_support_table", conn, if_exists="replace",
    #                                            temp=False)
    # print("df_sap_ibp_dcde done")

    ### Replace(or Create) and insert to temporary table data from VMI_Rothenburg*.csv
    insert_db_temp_data_vmi_roth = fts.fast_to_sql(df_vmi_rtg, "temp_data_VMI_Toyota_Rothenburg", conn, if_exists="replace",
                                                   temp=False)
    print("df_vmi_rtg done")
    insert_db_temp_data_arrivals_report = fts.fast_to_sql(df_arrivals_report, "temp_data_Arrivals_Report", conn,
                                                          if_exists="replace", temp=False)
    print("df_arrivals_report done")
    insert_db_temp_data_sap_ibp_suppliers = fts.fast_to_sql(df_sap_ibp_suppliers, "temp_data_SAP_Suppliers", conn,
                                                            if_exists="replace", temp=False)
    print("temp_data_SAP_Suppliers")
    insert_db_temp_gross_forecast = fts.fast_to_sql(df_idw_gross_forecast, "temp_data_gross_forecast", conn,
                                                    if_exists="replace", temp=False)
    print("temp_data_gross_forecast")

    # insert_db_temp_data_cmdt_report = fts.fast_to_sql(cmdt_df, "CMDT_Fixed_table", conn, if_exists = "replace", temp=False)
    # print("CMDT_Fixed_table")
    # insert_db_temp_data_fora = fts.fast_to_sql(fora_df, "temp_data_FORA_ZS", conn, if_exists = "replace", temp=False)
    # print("temp_data_FORA_ZS")


    ### for saving all manipulations always use .commit and .close
    conn.commit()
    cursor.close()
    conn.close()


# load_data_to_db()