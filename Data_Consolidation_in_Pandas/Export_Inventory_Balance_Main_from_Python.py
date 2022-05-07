import glob
import os

import numpy as np

import pandas as pd

pd.set_option('display.float_format', lambda x: '%.5f' % x)
pd.options.mode.chained_assignment = None
import time
from datetime import timedelta

#############################################################################################
### change SQL procedure on python script
#############################################################################################
start_time = time.monotonic()

def Export_Prepared_Inv_Balance_Main_Report():
    export_db = r"C:\InventoryBalanceData\Prepared_Reps"

    idw_bal_csv = r'C:\InventoryBalanceData\INV_merged_files\IDW_Balance_Enquiry.csv'
    idw_cost_csv = r'C:\InventoryBalanceData\INV_merged_files\IDW_Cost_Enquiry.csv'
    idw_gross_req = r'C:\InventoryBalanceData\INV_merged_files\IDW_Gross_Forecast.csv'

    cat_fixed_table = r'C:\InventoryBalanceData\Fixed_tables\Cat_Fixed_table.xlsx'
    cmdt_fixed_table = r'C:\InventoryBalanceData\Fixed_tables\CMDT_Fixed_table.xlsx'
    indop_fixed_table = r'C:\InventoryBalanceData\Fixed_tables\Indop_Fixed_table.xlsx'
    fora_fixed_table = r'C:\InventoryBalanceData\Fixed_tables\FORA_ZS.xlsx'


    vmi_folder = "C:\\InventoryBalanceData\\"
    vmi_rothenburg = glob.glob(os.path.join(vmi_folder, "VMI_toyota_rothenburg*"))

    sap_ibp_folder = "C:\\InventoryBalanceData\\SAP IBP Tables\\"
    sap_ibp_supp_code = glob.glob(os.path.join(sap_ibp_folder, "SAP_IBP_Supplier_Code*"))
    sap_ibp_supp_name = glob.glob(os.path.join(sap_ibp_folder, "SAP_IBP_Supplier_Description*"))
    sap_ibp_supp_idco = glob.glob(os.path.join(sap_ibp_folder, "SAP_IBP_IDCO*"))
    sap_ibp_cost = glob.glob(os.path.join(sap_ibp_folder, "SAP_IBP_Cost*"))

    sap_ibp_suppliers = glob.glob(os.path.join(sap_ibp_folder, "SAP_IBP_Suppliers*"))

    arrivals_report = glob.glob(os.path.join(sap_ibp_folder, "Arrivals_report*"))
    dcde_report = glob.glob(os.path.join(sap_ibp_folder, "DCDE3*"))


    ### source csv files opened as dataframes
    df_idw_bal = pd.read_csv(idw_bal_csv,  low_memory=False, usecols = ['#1', '#2', 'Plant', 'ANC', 'Description', 'Bal_Cat',
                                                                       'BC', 'PMS Supplier', 'Local_Supp', 'Suppname', 'IDCO',
                                                                       'STK_EUR', 'INV_date'])
    df_vmi_rtg = pd.read_csv(vmi_rothenburg[0],  low_memory=False)
    df_arrivals_report = pd.read_csv(arrivals_report[0],  low_memory=False)
    df_idw_cost = pd.read_csv(idw_cost_csv, low_memory=False)
    df_idw_cost = df_idw_cost.dropna(how='all')
    df_sap_ibp_supp_code = pd.read_csv(sap_ibp_supp_code[0], low_memory=False)
    df_sap_ibp_supp_name = pd.read_csv(sap_ibp_supp_name[0], low_memory=False)
    df_sap_ibp_supp_idco = pd.read_csv(sap_ibp_supp_idco[0], low_memory=False)
    df_sap_ibp_cost = pd.read_csv(sap_ibp_cost[0], low_memory=False)
    df_sap_ibp_dcde = pd.read_csv(dcde_report[0], low_memory=False)

    df_cat_table = pd.read_excel(cat_fixed_table)
    df_cmdt_table = pd.read_excel(cmdt_fixed_table)
    df_indop_table = pd.read_excel(indop_fixed_table)
    df_fora_table = pd.read_excel(fora_fixed_table)

    df_sap_ibp_suppliers = pd.read_csv(sap_ibp_suppliers[0], low_memory=False)

    ### idw inventory set appropriate names of columns
    df_idw_bal.rename(columns={'Local_Supp': 'Supplier_ID'}, inplace=True)
    df_idw_bal.rename(columns={'Suppname': 'Supplier_Name'}, inplace=True)
    df_idw_bal.rename(columns={'INV_date': 'Qty'}, inplace=True)
    df_idw_bal['Source_Data'] = 'IDW'

    ### vmi set appropriate names of columns
    df_vmi_rtg['Source_Data'] = 'VMI'
    df_vmi_rtg['#2'] = ''
    df_vmi_rtg['Plant'] = 'DGT'
    df_vmi_rtg.rename(columns={'Part No': 'ANC'}, inplace=True)
    df_vmi_rtg['Description'] = ''
    df_vmi_rtg['Bal_Cat'] = 'VMI Consignment'
    df_vmi_rtg['BC'] = ''
    df_vmi_rtg['PMS Supplier'] = ''
    df_vmi_rtg.rename(columns={'Supplier ID': 'Supplier_ID'}, inplace=True)
    df_vmi_rtg['Supplier_Name'] = ''
    df_vmi_rtg['IDCO'] = ''
    df_vmi_rtg['STK_EUR'] = ''
    df_vmi_rtg.rename(columns={'Sum of VMI Store': 'Qty'}, inplace=True)

    # ### arrivals (or named by Grzesiek 'GIT') set appropriate names of columns
    # df_git_arrivals = df_arrivals_report[['#1','Plant','ANC','SupplierID', 'TTL']]
    # df_git_arrivals['Source_Data'] = 'GIT'
    # df_git_arrivals['#2'] = ''
    # df_git_arrivals['Description'] = ''
    # df_git_arrivals['Bal_Cat'] = 'GIT'
    # df_git_arrivals['BC'] = ''
    # df_git_arrivals['PMS Supplier'] = ''
    # df_git_arrivals.rename(columns={'SupplierID': 'Supplier_ID'}, inplace=True)
    # df_git_arrivals['Supplier_Name'] = ''
    # df_git_arrivals['IDCO'] = ''
    # df_git_arrivals['STK_EUR'] = ''
    # df_git_arrivals.rename(columns={'TTL': 'Qty'}, inplace=True)

    ### union all 3 reports together
    # united_three_reports = [df_idw_bal, df_vmi_rtg, df_git_arrivals]
    united_three_reports = [df_idw_bal, df_vmi_rtg]

    united_main_table = pd.concat(united_three_reports, ignore_index = 1)

    # united_main_table['Description'] = [x[1][united_main_table.columns.get_loc(" ")] if x[1][united_main_table.columns.get_loc("#1")] == x[1][united_main_table.columns.get_loc("#1")] for x in united_main_table.iterrows()]
    # united_main_table

    ### filling data for Description column (instead of look up in excel for 5 hours)
    idw_bal_join =  united_main_table[['#1','Description']].drop_duplicates()
    idw_bal_join.rename(columns={'Description': 'Description_IDW'}, inplace=True)
    main_table_description = pd.merge(united_main_table, idw_bal_join, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_description.drop(['Description','Description_IDW'], axis=1)
    main_table_description = main_table_description.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    idw_cost_join =  df_idw_cost[['#1','ANC_Description']].drop_duplicates()
    idw_cost_join.rename(columns={'ANC_Description': 'Description_Costs'}, inplace=True)
    main_table_description_costs = pd.merge(main_table_description, idw_cost_join, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_description_costs.drop(['Description','Description_IDW','Description_Costs'], axis=1)
    main_table_description_costs = main_table_description_costs.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    # main_table_description_costs['Description_Costs'] = main_table_description_costs['Description_Costs'].fillna('-')
    # main_table_description_costs[main_table_description_costs['Description_Costs'] == '-']
    main_table_description_costs['Description_Final'] = np.where(main_table_description_costs['Description'] != '', main_table_description_costs['Description'],
                                                                  np.where(main_table_description_costs['Description_IDW'] != '', main_table_description_costs['Description_IDW'],
                                                                 np.where(main_table_description_costs['Description_Costs'] != '', main_table_description_costs['Description_Costs'], '-')))
    del main_table_description_costs['Description_Costs'], main_table_description_costs['Description'], main_table_description_costs['Description_IDW']
    main_table_description_costs.rename(columns={'Description_Final': 'Description'}, inplace=True)
    main_table_description_costs = main_table_description_costs.reset_index(drop=True)

    ### filling data for BC column (instead of look up in excel for 7 hours)

    idw_bal_join =  main_table_description_costs[['#1','BC']].drop_duplicates()
    idw_bal_join.rename(columns={'BC': 'BC_IDW'}, inplace=True)
    main_table_description = pd.merge(main_table_description_costs, idw_bal_join, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_description.drop(['BC','BC_IDW'], axis=1)
    main_table_bc = main_table_description.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    idw_cost_join =  df_idw_cost[['#1','BC']].drop_duplicates()
    idw_cost_join.rename(columns={'BC': 'BC_Costs'}, inplace=True)
    main_table_bc_costs = pd.merge(main_table_bc, idw_cost_join, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_bc_costs.drop(['BC','BC_IDW','BC_Costs'], axis=1)
    main_table_bc_costs = main_table_bc_costs.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    main_table_bc_costs['BC_Final'] = np.where(main_table_bc_costs['BC'] != '', main_table_bc_costs['BC'],
                                                                  np.where(main_table_bc_costs['BC_IDW'] != '', main_table_bc_costs['BC_IDW'],
                                                                 np.where(main_table_bc_costs['BC_Costs'] != '', main_table_bc_costs['BC_Costs'], 10)))
    del main_table_bc_costs['BC_Costs'], main_table_bc_costs['BC'], main_table_bc_costs['BC_IDW']
    main_table_bc_costs.rename(columns={'BC_Final': 'BC'}, inplace=True)
    main_table_bc_costs = main_table_bc_costs.reset_index(drop=True)


    ### filling data for Supplier_ID column (instead of look up in excel for 10 hours)

    df_sap_ibp_supp_code_join = df_sap_ibp_supp_code[['#','Supplier Code']].drop_duplicates()
    df_sap_ibp_supp_code_join.rename(columns={'#': '#1'}, inplace=True)
    df_sap_ibp_supp_code_join.rename(columns={'Supplier Code': 'Supplier_ID_SAP'}, inplace=True)
    main_table_supp_code_sap = pd.merge(main_table_bc_costs, df_sap_ibp_supp_code_join, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_supp_code_sap.drop(['Supplier_ID','Supplier_ID_SAP'], axis=1)
    main_table_supp_id = main_table_supp_code_sap.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    idw_bal_join_id = main_table_supp_id[['#1','Supplier_ID']].drop_duplicates()
    idw_bal_join_id.rename(columns={'Supplier_ID': 'Supplier_ID_IDW'}, inplace=True)
    main_table_idw_id = pd.merge(main_table_supp_id, idw_bal_join_id, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_idw_id.drop(['Supplier_ID','Supplier_ID_IDW','Supplier_ID_SAP'], axis=1)
    main_table_idw_code = main_table_idw_id.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    df_vmi_rtg_code = df_vmi_rtg[['#1','Supplier_ID']].drop_duplicates()
    df_vmi_rtg_code.rename(columns={'Supplier_ID': 'Supplier_ID_VMI'}, inplace=True)
    main_table_supp_code_vmi = pd.merge(main_table_idw_code, df_vmi_rtg_code, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_supp_code_vmi.drop(['Supplier_ID','Supplier_ID_IDW','Supplier_ID_SAP', 'Supplier_ID_VMI'], axis=1)
    main_table_supp_code_vmi = main_table_supp_code_vmi.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    # df_git_arrivals_data = df_git_arrivals[['#1','Supplier_ID']].drop_duplicates()
    # df_git_arrivals_data.rename(columns={'Supplier_ID': 'Supplier_ID_GIT'}, inplace=True)
    # main_table_supp_code_git = pd.merge(main_table_supp_code_vmi, df_git_arrivals_data, left_on= '#1', right_on= '#1', how = 'left')
    # eliminated_table = main_table_supp_code_git.drop(['Supplier_ID','Supplier_ID_IDW','Supplier_ID_SAP', 'Supplier_ID_VMI', 'Supplier_ID_GIT'], axis=1)
    # main_table_supp_id_git = main_table_supp_code_git.drop_duplicates(subset = list(eliminated_table.columns), keep='first')

    main_table_supp_code_vmi['Supplier_ID_Final'] = np.where(main_table_supp_code_vmi['Supplier_ID_SAP'].notna(), main_table_supp_code_vmi['Supplier_ID_SAP'],
                                                             np.where(main_table_supp_code_vmi['Supplier_ID'].notna(), main_table_supp_code_vmi['Supplier_ID'],
                                                             np.where(main_table_supp_code_vmi['Supplier_ID_IDW'].notna(), main_table_supp_code_vmi['Supplier_ID_IDW'],
                                                             np.where(main_table_supp_code_vmi['Supplier_ID_VMI'].notna(), main_table_supp_code_vmi['Supplier_ID_VMI'],'-'))))
    del main_table_supp_code_vmi['Supplier_ID'], main_table_supp_code_vmi['Supplier_ID_SAP'], main_table_supp_code_vmi['Supplier_ID_IDW'], main_table_supp_code_vmi['Supplier_ID_VMI']
    main_table_supp_code_vmi.rename(columns={'Supplier_ID_Final': 'Supplier_ID'}, inplace=True)
    main_table_supp_code_vmi = main_table_supp_code_vmi.reset_index(drop=True)


    ### filling data for Supplier_Description column (instead of look up in excel for 12 hours)

    df_sap_ibp_supp_name_join = df_sap_ibp_supp_name[['#','Supplier Description']].drop_duplicates()
    df_sap_ibp_supp_name_join.rename(columns={'#': '#1'}, inplace=True)
    df_sap_ibp_supp_name_join.rename(columns={'Supplier Description': 'Supplier_Description_SAP'}, inplace=True)
    main_table_supp_descr_sap = pd.merge(main_table_supp_code_vmi, df_sap_ibp_supp_name_join, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_supp_descr_sap.drop(['Supplier_Name','Supplier_Description_SAP'], axis=1)
    main_table_supp_descr_sap = main_table_supp_descr_sap.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    df_sap_ibp_supp_idw_name_join = main_table_supp_descr_sap[['#1','Supplier_Name']].drop_duplicates()
    df_sap_ibp_supp_idw_name_join.rename(columns={'Supplier_Name': 'Supplier_Name_IDW'}, inplace=True)
    main_table_supp_descr_idw = pd.merge(main_table_supp_descr_sap, df_sap_ibp_supp_idw_name_join, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_supp_descr_idw.drop(['Supplier_Name', 'Supplier_Description_SAP', 'Supplier_Name_IDW'], axis=1)
    main_table_supp_name_idw = main_table_supp_descr_idw.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    main_table_supp_name_idw['Supplier_Name_Final'] = np.where(main_table_supp_name_idw['Supplier_Description_SAP'].notna(), main_table_supp_name_idw['Supplier_Description_SAP'],
                                                                   np.where(main_table_supp_name_idw['Supplier_Name_IDW'].notna(), main_table_supp_name_idw['Supplier_Name_IDW'],
                                                                   np.where(main_table_supp_name_idw['Supplier_Name'].notna(), main_table_supp_name_idw['Supplier_Name'],'-')))# main_table_description_costs[main_table_description_costs['Description'] == '']

    del main_table_supp_name_idw['Supplier_Description_SAP'], main_table_supp_name_idw['Supplier_Name_IDW'], main_table_supp_name_idw['Supplier_Name']
    main_table_supp_name_idw.rename(columns={'Supplier_Name_Final': 'Supplier_Name'}, inplace=True)
    main_table_supp_name_idw = main_table_supp_name_idw.reset_index(drop=True)

    ### filling data for IDCO column (instead of look up in excel for 15 hours)

    idw_idco_join =  main_table_supp_name_idw[['#1','IDCO']].drop_duplicates()
    idw_idco_join.rename(columns={'IDCO': 'IDCO_IDW'}, inplace=True)
    main_table_idco_idw = pd.merge(main_table_supp_name_idw, idw_idco_join, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_idco_idw.drop(['IDCO','IDCO_IDW'], axis=1)
    main_table_idco = main_table_idco_idw.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    idw_cost_join =  df_idw_cost[['#1','IDCO']].drop_duplicates()
    idw_cost_join.rename(columns={'IDCO': 'IDCO_Cost'}, inplace=True)
    main_table_idco_cost = pd.merge(main_table_idco, idw_cost_join, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_idco_cost.drop(['IDCO','IDCO_IDW', 'IDCO_Cost'], axis=1)
    main_table_idco_cost_data = main_table_idco_cost.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    df_sap_ibp_idco = df_sap_ibp_supp_idco[['Product ID', 'IDCO Description']].drop_duplicates()
    df_sap_ibp_idco.rename(columns={'IDCO Description': 'IDCO_SAP'}, inplace=True)
    df_sap_ibp_idco.rename(columns={'Product ID': 'ANC'}, inplace=True)
    main_table_idco_sap = pd.merge(main_table_idco_cost_data, df_sap_ibp_idco, left_on= 'ANC', right_on= 'ANC', how = 'left')
    eliminated_table = main_table_idco_sap.drop(['IDCO','IDCO_IDW', 'IDCO_Cost','IDCO_SAP'], axis=1)
    main_table_idco_sap_data = main_table_idco_sap.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    main_table_idco_sap_data['IDCO_Final'] = np.where(main_table_idco_sap_data['IDCO'].notna(), main_table_idco_sap_data['IDCO'],
                                                             np.where(main_table_idco_sap_data['IDCO_IDW'].notna(), main_table_idco_sap_data['IDCO_IDW'],
                                                             np.where(main_table_idco_sap_data['IDCO_Cost'].notna(), main_table_idco_sap_data['IDCO_Cost'],
                                                             np.where(main_table_idco_sap_data['IDCO_SAP'] !='', main_table_idco_sap_data['IDCO_SAP'], '-'))))
    del main_table_idco_sap_data['IDCO'], main_table_idco_sap_data['IDCO_IDW'], main_table_idco_sap_data['IDCO_Cost'] , main_table_idco_sap_data['IDCO_SAP']
    main_table_idco_sap_data.rename(columns={'IDCO_Final': 'IDCO'}, inplace=True)
    main_table_idco_sap_data = main_table_idco_sap_data.reset_index(drop=True)



    ### filling data for STK_EUR column (instead of look up in excel for 20 hours)

    df_idw_cost_stk = df_idw_cost[['#1','Conv_STK3_Calc']].drop_duplicates()
    df_idw_cost_stk.rename(columns={'Conv_STK3_Calc': 'STK3_Calc_Cost'}, inplace=True)
    main_table_stk_cost_data = pd.merge(main_table_idco_sap_data, df_idw_cost_stk, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_stk_cost_data.drop(['STK_EUR','STK3_Calc_Cost'], axis=1)
    main_table_stk = main_table_stk_cost_data.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    df_sap_dcde = df_sap_ibp_dcde[['#','STK3 EUR']].drop_duplicates()
    df_sap_dcde.rename(columns={'#': '#1'}, inplace=True)
    df_sap_dcde.rename(columns={'STK3 EUR': 'STK_EUR_DCDE'}, inplace=True)
    main_table_stk_dcde = pd.merge(main_table_stk, df_sap_dcde, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_stk_dcde.drop(['STK_EUR','STK3_Calc_Cost', 'STK_EUR_DCDE'], axis=1)
    main_table_stk = main_table_stk_dcde.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    df_sap_ibp_cost_data = df_sap_ibp_cost[['#','Standard Cost']].drop_duplicates()
    df_sap_ibp_cost_data.rename(columns={'#': '#1'}, inplace=True)
    df_sap_ibp_cost_data.rename(columns={'Standard Cost': 'STK_EUR_SAP'}, inplace=True)
    main_table_stk_sap = pd.merge(main_table_stk, df_sap_ibp_cost_data, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_stk_sap.drop(['STK_EUR','STK3_Calc_Cost', 'STK_EUR_DCDE', 'STK_EUR_SAP'], axis=1)
    main_table_stk_sap = main_table_stk_sap.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    main_table_stk_idw = main_table_stk_sap[['#1','STK_EUR']].drop_duplicates()
    main_table_stk_idw.rename(columns={'STK_EUR': 'STK_EUR_IDW'}, inplace=True)
    main_table_stk_idw = pd.merge(main_table_stk_sap, main_table_stk_idw, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_stk_idw.drop(['STK_EUR','STK3_Calc_Cost', 'STK_EUR_DCDE', 'STK_EUR_SAP','STK_EUR_IDW'], axis=1)
    main_table_stk_euro = main_table_stk_idw.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    main_table_stk_euro['STK_EUR_FINAL'] = np.where((main_table_stk_euro['STK3_Calc_Cost'] != '') | (main_table_stk_euro['STK_EUR_DCDE'].notna()), main_table_stk_euro['STK3_Calc_Cost'],
                                                                  np.where((main_table_stk_euro['STK_EUR_DCDE'] != '') | (main_table_stk_euro['STK_EUR_DCDE'].notna()), main_table_stk_euro['STK_EUR_DCDE'],
                                                                  np.where((main_table_stk_euro['STK_EUR_SAP'] != '') | (main_table_stk_euro['STK_EUR_SAP'].notna()), main_table_stk_euro['STK_EUR_SAP'],
                                                                  np.where((main_table_stk_euro['STK_EUR_IDW'] != '') | (main_table_stk_euro['STK_EUR_IDW'].notna()), main_table_stk_euro['STK_EUR_IDW'], 0))))

    del main_table_stk_euro['STK3_Calc_Cost'], main_table_stk_euro['STK_EUR_DCDE'], main_table_stk_euro['STK_EUR_SAP'], main_table_stk_euro['STK_EUR_IDW'], main_table_stk_euro['STK_EUR']
    main_table_stk_euro.rename(columns={'STK_EUR_FINAL': 'STK_EUR'}, inplace=True)
    main_table_stk_euro = main_table_stk_euro.reset_index(drop=True)


    ### filling data for Value column (instead of look up in excel for 23 hours) that is multiply STK Eur on Qty
    main_table_stk_euro['STK_EUR'] = main_table_stk_euro['STK_EUR'].astype(float)
    main_table_stk_euro['Value'] = main_table_stk_euro['STK_EUR'] * main_table_stk_euro['Qty']


    ### merging with cmdt indop and categories fixed tables

    main_table_indop = pd.merge(main_table_stk_euro, df_indop_table, left_on= 'Plant', right_on= 'Plant', how = 'left')
    main_table_cmdt_temp = pd.merge(main_table_indop, df_cmdt_table, left_on= 'IDCO', right_on= 'IDCO', how = 'left')
    eliminated_table = main_table_cmdt_temp.drop(['IDCO'], axis=1)
    main_table_cmdt = main_table_cmdt_temp.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    df_cat_table.rename(columns={'Bal Cat': 'Bal_Cat'}, inplace=True)
    main_table_cat = pd.merge(main_table_cmdt, df_cat_table, left_on= 'Bal_Cat', right_on= 'Bal_Cat', how = 'left')
    main_table_cat = main_table_cat.reset_index(drop=True)

    ### retreiving key #2 using concatinations and cases, and filling supplier description for GIT's

    main_table_cat['#2'] = np.where((main_table_cat['#2'] =='' ) & (main_table_cat['Source_Data'] =='GIT' ), main_table_cat['Supplier_ID']+ main_table_cat['Plant'], main_table_cat['#2'])
    df_sap_suppliers = df_sap_ibp_suppliers[['Supplier Code','Supplier Description']].drop_duplicates()
    df_sap_suppliers.rename(columns={'Supplier Code': 'Supplier_ID'}, inplace=True)
    df_sap_suppliers.rename(columns={'Supplier Description': 'Supplier_Name_Supplier'}, inplace=True)
    df_sap_suppliers_data = pd.merge(main_table_cat, df_sap_suppliers, left_on= 'Supplier_ID', right_on= 'Supplier_ID', how = 'left')
    eliminated_table = df_sap_suppliers_data.drop(['Supplier_Name','Supplier_Name_Supplier'], axis=1)
    df_sap_supplier = df_sap_suppliers_data.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    df_sap_supplier['Supplier_Name_Final'] = np.where(((df_sap_supplier['Supplier_Name'] =='') & (df_sap_supplier['Source_Data'] == 'GIT')), df_sap_supplier['Supplier_Name_Supplier'], df_sap_supplier['Supplier_Name'])
    del df_sap_supplier['Supplier_Name'],df_sap_supplier['Supplier_Name_Supplier']
    df_sap_supplier.rename(columns={'Supplier_Name_Final': 'Supplier_Name'}, inplace=True)
    df_sap_supplier = df_sap_supplier.reset_index(drop=True)


    ### filling missing supplier names for VMI

    df_sap_suppliers_vmi = df_sap_ibp_suppliers[['Supplier Code','Supplier Description']].drop_duplicates()
    df_sap_suppliers_vmi.rename(columns={'Supplier Code': 'Supplier_ID'}, inplace=True)
    df_sap_suppliers_vmi.rename(columns={'Supplier Description': 'Supplier_Name_Supplier'}, inplace=True)
    df_sap_supp_vmi = pd.merge(df_sap_supplier, df_sap_suppliers_vmi, left_on= 'Supplier_ID', right_on= 'Supplier_ID', how = 'left')
    eliminated_table = df_sap_supp_vmi.drop(['Supplier_Name','Supplier_Name_Supplier'], axis=1)
    df_sap_supplier_vmi_data = df_sap_supp_vmi.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    df_sap_supplier_vmi_data['Supplier_Name_Final'] = np.where(((df_sap_supplier_vmi_data['Supplier_Name'] =='') & (df_sap_supplier_vmi_data['Source_Data'] == 'VMI')), df_sap_supplier_vmi_data['Supplier_Name_Supplier'], df_sap_supplier_vmi_data['Supplier_Name'])
    del df_sap_supplier_vmi_data['Supplier_Name'],df_sap_supplier_vmi_data['Supplier_Name_Supplier']
    df_sap_supplier_vmi_data.rename(columns={'Supplier_Name_Final': 'Supplier_Name'}, inplace=True)
    df_sap_supplier_vmi_data = df_sap_supplier_vmi_data.reset_index(drop=True)



    ### filling ZS suppliers names from fixed table

    df_fora_table_data = df_fora_table[['Supplier_ID', 'Supplier_Name']]
    df_fora_table_data.rename(columns={'Supplier_Name': 'Supplier_Name_FORA'}, inplace=True)
    df_sap_supp_fora = pd.merge(df_sap_supplier_vmi_data, df_fora_table_data, left_on= 'Supplier_ID', right_on= 'Supplier_ID', how = 'left')
    eliminated_table = df_sap_supp_fora.drop(['Supplier_Name','Supplier_Name_FORA'], axis=1)
    df_sap_supplier_fora_data = df_sap_supp_fora.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    df_sap_supplier_fora_data['Supplier_Name_Final'] = np.where(((df_sap_supplier_fora_data['Supplier_Name'] =='') & (df_sap_supplier_fora_data['Source_Data'] == 'GIT') & (df_sap_supplier_fora_data['Plant'] == 'ZS')), df_sap_supplier_fora_data['Supplier_Name_FORA'], df_sap_supplier_fora_data['Supplier_Name'])
    del df_sap_supplier_fora_data['Supplier_Name'], df_sap_supplier_fora_data['Supplier_Name_FORA']
    df_sap_supplier_fora_data.rename(columns={'Supplier_Name_Final': 'Supplier_Name'}, inplace=True)
    df_sap_supplier_fora_data = df_sap_supplier_fora_data.reset_index(drop=True)



    ### filling hopefully the last time the supplier name using sap descriptions

    df_sap_suppliers_last = df_sap_ibp_suppliers[['Supplier Code','Supplier Description']].drop_duplicates()
    df_sap_suppliers_last.rename(columns={'Supplier Code': 'Supplier_ID'}, inplace=True)
    df_sap_suppliers_last.rename(columns={'Supplier Description': 'Supplier_Name_LAST'}, inplace=True)
    df_sap_supp_last = pd.merge(df_sap_supplier_fora_data, df_sap_suppliers_last, left_on= 'Supplier_ID', right_on= 'Supplier_ID', how = 'left')
    eliminated_table = df_sap_supp_last.drop(['Supplier_Name','Supplier_Name_LAST'], axis=1)
    df_sap_supplier_last_data = df_sap_supp_last.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    df_sap_supplier_last_data['Supplier_Name_Final'] = np.where(((df_sap_supplier_last_data['Supplier_ID'].notna()) & (df_sap_supplier_last_data['Supplier_Name'] == '-')), df_sap_supplier_last_data['Supplier_Name_LAST'], df_sap_supplier_last_data['Supplier_Name'])
    del df_sap_supplier_last_data['Supplier_Name'], df_sap_supplier_last_data['Supplier_Name_LAST']
    df_sap_supplier_last_data.rename(columns={'Supplier_Name_Final': 'Supplier_Name'}, inplace=True)
    df_sap_supplier_last_data = df_sap_supplier_last_data.reset_index(drop=True)


    ### fix of value (shouldnt be negative)
    df_sap_supplier_last_data['Value'] = np.where(df_sap_supplier_last_data['Value'] < 0, 0, df_sap_supplier_last_data['Value'])

    ##########################################################################################
    ################### FOR A WHILE IT NEEDN'T ###############################################
    # ### merging with arrivals quantity part
    # keys = df_arrivals_report.loc[:,['#1']]
    # weeks = df_arrivals_report.iloc[:,df_arrivals_report.columns.get_loc("wk0"):df_arrivals_report.columns.get_loc("TTL")+1]
    # combined_weeks = [keys, weeks]
    # combined_weeks = pd.concat(combined_weeks, axis = 1)
    # df_sap_supplier_last_data = pd.merge(df_sap_supplier_last_data, combined_weeks, left_on= '#1', right_on= '#1', how = 'left')
    # df_sap_supplier_last_data.rename(columns={'TTL': 'TTL_Arr_Qty'}, inplace=True)
    #
    #
    # ### replace on dash in CMDT and add multiples for arrivals
    # df_sap_supplier_last_data['CMDT'] = df_sap_supplier_last_data['CMDT'].fillna('-')
    # df_sap_supplier_last_data['wkmult0'] = df_sap_supplier_last_data['wk0'] * df_sap_supplier_last_data['STK_EUR']
    # df_sap_supplier_last_data['wkmult1'] = df_sap_supplier_last_data['wk1'] * df_sap_supplier_last_data['STK_EUR']
    # df_sap_supplier_last_data['wkmult2'] = df_sap_supplier_last_data['wk2'] * df_sap_supplier_last_data['STK_EUR']
    # df_sap_supplier_last_data['wkmult3'] = df_sap_supplier_last_data['wk3'] * df_sap_supplier_last_data['STK_EUR']
    # df_sap_supplier_last_data['wkmult4'] = df_sap_supplier_last_data['wk4'] * df_sap_supplier_last_data['STK_EUR']
    # df_sap_supplier_last_data['wkmult5'] = df_sap_supplier_last_data['wk5'] * df_sap_supplier_last_data['STK_EUR']
    # df_sap_supplier_last_data['wkmult6'] = df_sap_supplier_last_data['wk6'] * df_sap_supplier_last_data['STK_EUR']
    # df_sap_supplier_last_data['wkmult7'] = df_sap_supplier_last_data['wk7'] * df_sap_supplier_last_data['STK_EUR']
    # df_sap_supplier_last_data['wkmult8'] = df_sap_supplier_last_data['wk8'] * df_sap_supplier_last_data['STK_EUR']
    # df_sap_supplier_last_data['wkmult9'] = df_sap_supplier_last_data['wk9'] * df_sap_supplier_last_data['STK_EUR']
    # df_sap_supplier_last_data['wkmult10'] = df_sap_supplier_last_data['wk10'] * df_sap_supplier_last_data['STK_EUR']
    # df_sap_supplier_last_data['wkmult11'] = df_sap_supplier_last_data['wk11'] * df_sap_supplier_last_data['STK_EUR']
    # df_sap_supplier_last_data['wkmult12'] = df_sap_supplier_last_data['wk12'] * df_sap_supplier_last_data['STK_EUR']
    # df_sap_supplier_last_data['wkmult13'] = df_sap_supplier_last_data['wk13'] * df_sap_supplier_last_data['STK_EUR']
    # df_sap_supplier_last_data['TTL_Arr_Value'] = df_sap_supplier_last_data.iloc[:, df_sap_supplier_last_data.columns.get_loc("wkmult0"):df_sap_supplier_last_data.columns.get_loc("wkmult13")+1].sum(axis=1)



    ####### IMPROVEMENTS IN RAPORT AFTER CONSULTING WITH GRZESIEK
    #### ADD 10 do BC jak jest puste, i Description - jak pusty
    #### Blanks też zamienić na -
    #### z IDCO - **** i puste pola zamienić na '-'
    df_sap_supplier_last_data['BC'] = np.where(df_sap_supplier_last_data['BC'].isna(), 10, df_sap_supplier_last_data['BC'])
    df_sap_supplier_last_data = df_sap_supplier_last_data.reset_index(drop=True)
    df_sap_supplier_last_data['Description'] = np.where(df_sap_supplier_last_data['Description'].isna(), '-', df_sap_supplier_last_data['Description'])
    df_sap_supplier_last_data = df_sap_supplier_last_data.reset_index(drop=True)
    df_sap_supplier_last_data['IDCO'] = np.where((df_sap_supplier_last_data['IDCO'].isna()) | (df_sap_supplier_last_data['IDCO'] == '****'), '-', df_sap_supplier_last_data['IDCO'])
    df_sap_supplier_last_data = df_sap_supplier_last_data.reset_index(drop=True)
    df_sap_supplier_last_data['Supplier_Name'] = np.where((df_sap_supplier_last_data['Supplier_Name'].isna()) | (df_sap_supplier_last_data['Supplier_Name'] == '****'), '-', df_sap_supplier_last_data['Supplier_Name'])
    df_sap_supplier_last_data = df_sap_supplier_last_data.reset_index(drop=True)

    #### jezeli są blanki w Supplier_name to wyszukuje  w tablicy SAP IBP suplliers merge po supplier id

    ### filling hopefully the last time the supplier name using sap descriptions
    df_sap_suppliers_last = df_sap_ibp_suppliers[['Supplier Code', 'Supplier Description']].drop_duplicates()
    df_sap_suppliers_last.rename(columns={'Supplier Code': 'Supplier_ID'}, inplace=True)
    df_sap_suppliers_last.rename(columns={'Supplier Description': 'Supplier_Name_LAST'}, inplace=True)
    df_sap_supp_last = pd.merge(df_sap_supplier_last_data, df_sap_suppliers_last, left_on= 'Supplier_ID', right_on= 'Supplier_ID', how = 'left')
    eliminated_table = df_sap_supp_last.drop(['Supplier_Name','Supplier_Name_LAST'], axis=1)
    df_sap_supplier_last_data = df_sap_supp_last.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    df_sap_supplier_last_data['Supplier_Name_Final'] = np.where(df_sap_supplier_last_data['Supplier_Name'].isna(), df_sap_supplier_last_data['Supplier_Name_LAST'], df_sap_supplier_last_data['Supplier_Name'])
    del df_sap_supplier_last_data['Supplier_Name'], df_sap_supplier_last_data['Supplier_Name_LAST']
    df_sap_supplier_last_data.rename(columns={'Supplier_Name_Final': 'Supplier_Name'}, inplace=True)
    df_sap_supplier_last_data = df_sap_supplier_last_data.reset_index(drop=True)

    df_sap_supplier_last_data['Supplier_Name'] = np.where((df_sap_supplier_last_data['Supplier_Name'] =='-') & ((df_sap_supplier_last_data['PMS Supplier'] != '') | (df_sap_supplier_last_data['PMS Supplier'].isna())), df_sap_supplier_last_data['PMS Supplier'], df_sap_supplier_last_data['Supplier_Name'])

    df_sap_supplier_last_data['Supplier_Name'] = np.where(df_sap_supplier_last_data['Supplier_Name'].isna(), '-', df_sap_supplier_last_data['Supplier_Name'])
    df_sap_supplier_last_data = df_sap_supplier_last_data[df_sap_supplier_last_data['Source_Data'] != 'GIT']
    df_sap_supplier_last_data['PMS Supplier'] = df_sap_supplier_last_data['PMS Supplier'].replace('', '-')
    df_sap_supplier_last_data['PMS Supplier'] = df_sap_supplier_last_data['PMS Supplier'].fillna('-')
    df_sap_supplier_last_data['STK_EUR'] = df_sap_supplier_last_data['STK_EUR'].fillna(0)
    print(df_sap_supplier_last_data)
    ### jezeli  zwq45są nadal blanki to wziąć z PMS Supplier
    ### jezeli STK EUR jest blanks to zamieniamy na 0

    exportDF = df_sap_supplier_last_data.to_csv(export_db + '\\InvBalance_' + time.strftime('%Y%m%d') + '.csv', encoding='utf-8',index=False)
    end_time = time.monotonic()
    print("Time of raport calculation is - ", timedelta(seconds=end_time - start_time))



# Export_Prepared_Inv_Balance_Main_Report()
# main_table_description_costs['counter'] = np.where(main_table_description_costs['Description_IDW']==main_table_description_costs['Description_Costs'],'yes','no')
# main_table_description_costs[main_table_description_costs['counter']=='no']
# main_table_description[main_table_description['Description'] == main_table_description['Description2']]
# main_table_description_costs[main_table_description_costs['#1'] == 'A13924101ZS']


Export_Prepared_Inv_Balance_Main_Report()