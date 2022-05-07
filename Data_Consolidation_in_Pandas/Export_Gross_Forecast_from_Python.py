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
def Export_Gross_Main_Report():
    start_time = time.monotonic()
    export_db = r"C:\InventoryBalanceData\Prepared_Reps"

    idw_gross_req = r'C:\InventoryBalanceData\INV_merged_files\IDW_Gross_Forecast.csv'
    idw_cost_csv = r'C:\InventoryBalanceData\INV_merged_files\IDW_Cost_Enquiry.csv'

    sap_ibp_folder = "C:\\InventoryBalanceData\\SAP IBP Tables\\"
    sap_ibp_supp_code = glob.glob(os.path.join(sap_ibp_folder, "SAP_IBP_Supplier_Code*"))
    sap_ibp_supp_name = glob.glob(os.path.join(sap_ibp_folder, "SAP_IBP_Supplier_Description*"))
    sap_ibp_cost = glob.glob(os.path.join(sap_ibp_folder, "SAP_IBP_Cost*"))
    sap_ibp_supp_idco = glob.glob(os.path.join(sap_ibp_folder, "SAP_IBP_IDCO*"))


    df_idw_cost = pd.read_csv(idw_cost_csv, low_memory=False)
    df_gross = pd.read_csv(idw_gross_req, low_memory=False)
    df_gross = df_gross[df_gross['#1']!='nannan']

    df_idw_cost = df_idw_cost.dropna(how='all')
    dcde_report = glob.glob(os.path.join(sap_ibp_folder, "DCDE3*"))
    df_sap_ibp_cost = pd.read_csv(sap_ibp_cost[0], low_memory=False)
    df_sap_ibp_supp_idco = pd.read_csv(sap_ibp_supp_idco[0], low_memory=False)
    df_sap_ibp_supp_code = pd.read_csv(sap_ibp_supp_code[0], low_memory=False)
    df_sap_ibp_supp_name = pd.read_csv(sap_ibp_supp_name[0], low_memory=False)
    df_sap_ibp_dcde = pd.read_csv(dcde_report[0], low_memory=False)


    ### started filling columns

    df_sap_ibp_supp_code_join = df_sap_ibp_supp_code[['#','Supplier Code']].drop_duplicates()
    df_sap_ibp_supp_code_join.rename(columns={'#': '#1'}, inplace=True)
    df_sap_ibp_supp_code_join.rename(columns={'Supplier Code': 'Supplier_ID_SAP'}, inplace=True)
    main_table_supp_code_sap = pd.merge(df_gross, df_sap_ibp_supp_code_join, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_supp_code_sap.drop(['Supplier','Supplier_ID_SAP'], axis=1)
    main_table_supp_id = main_table_supp_code_sap.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    idw_gross_join_id = df_gross[['#1','Supplier']].drop_duplicates()
    idw_gross_join_id.rename(columns={'Supplier': 'Supplier_GROSS'}, inplace=True)
    main_table_gross_id = pd.merge(main_table_supp_id, idw_gross_join_id, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_gross_id.drop(['Supplier','Supplier_ID_SAP', 'Supplier_GROSS'], axis=1)
    main_table_gross_code = main_table_gross_id.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    main_table_gross_code['Supplier_Final'] = np.where(main_table_gross_code['Supplier_ID_SAP'].notna(), main_table_gross_code['Supplier_ID_SAP'],
                                                                  np.where(main_table_gross_code['Supplier_GROSS'].notna(), main_table_gross_code['Supplier_GROSS'], '-'))

    del main_table_gross_code['Supplier_ID_SAP'], main_table_gross_code['Supplier_GROSS'],main_table_gross_code['Supplier']
    main_table_gross_code.rename(columns={'Supplier_Final': 'Supplier'}, inplace=True)


    #### filling supp description
    df_sap_ibp_supp_name_join = df_sap_ibp_supp_name[['#','Supplier Description']].drop_duplicates()
    df_sap_ibp_supp_name_join.rename(columns={'#': '#1'}, inplace=True)
    df_sap_ibp_supp_name_join.rename(columns={'Supplier Description': 'Supplier_Description_SAP'}, inplace=True)
    main_table_supp_descr_sap = pd.merge(main_table_gross_code, df_sap_ibp_supp_name_join, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_supp_descr_sap.drop(['Local Supplier','Supplier_Description_SAP'], axis=1)
    main_table_supp_descr_sap = main_table_supp_descr_sap.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    idw_gross_join_id = df_gross[['#1','Local Supplier']].drop_duplicates()
    idw_gross_join_id.rename(columns={'Local Supplier': 'Local_Supplier_GROSS'}, inplace=True)
    main_table_gross_id = pd.merge(main_table_supp_descr_sap, idw_gross_join_id, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_gross_id.drop(['Local Supplier','Local_Supplier_GROSS', 'Supplier_Description_SAP'], axis=1)
    main_table_gross_name = main_table_gross_id.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    main_table_gross_name['Local_Final'] = np.where(main_table_gross_name['Supplier_Description_SAP'].notna(), main_table_gross_name['Supplier_Description_SAP'],
                                                                  np.where(main_table_gross_name['Local_Supplier_GROSS'].notna(), main_table_gross_name['Local_Supplier_GROSS'], '-'))
    del main_table_gross_name['Local Supplier'], main_table_gross_name['Supplier_Description_SAP'], main_table_gross_name['Local_Supplier_GROSS']
    main_table_gross_name.rename(columns={'Local_Final': 'Local Supplier'}, inplace=True)


    #### filling DM or as it called STK
    df_idw_cost_stk = df_idw_cost[['#1','Conv_STK3_Calc']].drop_duplicates()
    df_idw_cost_stk.rename(columns={'Conv_STK3_Calc': 'STK3_Calc_Cost'}, inplace=True)
    main_table_stk_cost_data = pd.merge(main_table_gross_name, df_idw_cost_stk, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_stk_cost_data.drop(['DM','STK3_Calc_Cost'], axis=1)
    main_table_stk = main_table_stk_cost_data.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    df_sap_dcde = df_sap_ibp_dcde[['#','STK3 EUR']].drop_duplicates()
    df_sap_dcde.rename(columns={'#': '#1'}, inplace=True)
    df_sap_dcde.rename(columns={'STK3 EUR': 'STK_EUR_DCDE'}, inplace=True)
    main_table_stk_dcde = pd.merge(main_table_stk, df_sap_dcde, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_stk_dcde.drop(['DM','STK3_Calc_Cost', 'STK_EUR_DCDE'], axis=1)
    main_table_stk = main_table_stk_dcde.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    df_sap_ibp_cost_data = df_sap_ibp_cost[['#','Standard Cost']].drop_duplicates()
    df_sap_ibp_cost_data.rename(columns={'#': '#1'}, inplace=True)
    df_sap_ibp_cost_data.rename(columns={'Standard Cost': 'STK_EUR_SAP'}, inplace=True)
    main_table_stk_sap = pd.merge(main_table_stk, df_sap_ibp_cost_data, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_stk_sap.drop(['DM','STK3_Calc_Cost', 'STK_EUR_DCDE', 'STK_EUR_SAP'], axis=1)
    main_table_stk_sap = main_table_stk_sap.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    idw_gross_join_id = df_gross[['#1','DM']].drop_duplicates()
    idw_gross_join_id.rename(columns={'DM': 'DM_GROSS'}, inplace=True)
    main_table_gross_id = pd.merge(main_table_stk_sap, idw_gross_join_id, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_gross_id.drop(['DM','STK3_Calc_Cost', 'STK_EUR_DCDE', 'STK_EUR_SAP','DM_GROSS'], axis=1)
    main_table_stk_dm = main_table_gross_id.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    main_table_stk_dm['DM_FINAL'] = np.where(main_table_stk_dm['STK3_Calc_Cost'].notna(), main_table_stk_dm['STK3_Calc_Cost'],
                                                                  np.where(main_table_stk_dm['STK_EUR_DCDE'].notna(), main_table_stk_dm['STK_EUR_DCDE'],
                                                                  np.where(main_table_stk_dm['STK_EUR_SAP'].notna(), main_table_stk_dm['STK_EUR_SAP'],
                                                                  np.where(main_table_stk_dm['DM_GROSS'].notna(), main_table_stk_dm['DM_GROSS'], 0))))

    del main_table_stk_dm['STK3_Calc_Cost'], main_table_stk_dm['DM'], main_table_stk_dm['DM_GROSS'],  main_table_stk_dm['STK_EUR_DCDE'], main_table_stk_dm['STK_EUR_SAP']
    main_table_stk_dm.rename(columns={'DM_FINAL': 'DM'}, inplace=True)
    main_table_stk_dm


    #### filling column IDCO

    idw_gross_join_id = df_gross[['#1','IDCO']].drop_duplicates()
    idw_gross_join_id.rename(columns={'IDCO': 'IDCO_GROSS'}, inplace=True)
    main_table_stk_cost_data = pd.merge(main_table_stk_dm, idw_gross_join_id, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_stk_cost_data.drop(['IDCO','IDCO_GROSS'], axis=1)
    main_table_idco = main_table_stk_cost_data.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    df_idw_cost_idco = df_idw_cost[['#1','IDCO']].drop_duplicates()
    df_idw_cost_idco.rename(columns={'IDCO': 'IDCO_Cost'}, inplace=True)
    main_table_stk_cost_data = pd.merge(main_table_idco, df_idw_cost_idco, left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_stk_cost_data.drop(['IDCO','IDCO_GROSS','IDCO_Cost'], axis=1)
    main_table_idco = main_table_stk_cost_data.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    df_sap_ibp_idco = df_sap_ibp_supp_idco[['Product ID', 'IDCO Description']].drop_duplicates()
    df_sap_ibp_idco.rename(columns={'IDCO Description': 'IDCO_SAP'}, inplace=True)
    df_sap_ibp_idco.rename(columns={'Product ID': 'ANC'}, inplace=True)
    main_table_idco_sap = pd.merge(main_table_idco, df_sap_ibp_idco, left_on= 'ANC', right_on= 'ANC', how = 'left')
    eliminated_table = main_table_idco_sap.drop(['IDCO', 'IDCO_GROSS', 'IDCO_Cost', 'IDCO_SAP'], axis=1)
    main_table_idco_sap_data = main_table_idco_sap.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    main_table_idco_sap_data['IDCO_Final'] = np.where(main_table_idco_sap_data['IDCO'] !='', main_table_idco_sap_data['IDCO'],
                                                             np.where(main_table_idco_sap_data['IDCO_GROSS']!='', main_table_idco_sap_data['IDCO_GROSS'],
                                                             np.where(main_table_idco_sap_data['IDCO_Cost']!='', main_table_idco_sap_data['IDCO_Cost'],
                                                             np.where(main_table_idco_sap_data['IDCO_SAP']!='', main_table_idco_sap_data['IDCO_SAP'], '-'))))
    del main_table_idco_sap_data['IDCO'], main_table_idco_sap_data['IDCO_GROSS'], main_table_idco_sap_data['IDCO_Cost'],  main_table_idco_sap_data['IDCO_SAP']
    main_table_idco_sap_data.rename(columns={'IDCO_Final': 'IDCO'}, inplace=True)

    #### creating the key #2 for merging with other intresting columns

    # main_table_idco_sap_data['#2'] = main_table_idco_sap_data['Supplier'].str.split(',') + main_table_idco_sap_data['Plant']
    # main_table_idco_sap_data
    splitter_supps = main_table_idco_sap_data[main_table_idco_sap_data['Supplier'].str.contains(',')]
    splitter_supps['Supps'] = splitter_supps['Supplier'].str.split(',')
    sep_commas_spuppliers = splitter_supps.explode('Supps').reset_index(drop=True)
    cols = list(sep_commas_spuppliers.columns)
    sep_commas_suppliers = sep_commas_spuppliers[cols]
    sep_commas_suppliers['#2'] = sep_commas_suppliers['Supps'] + sep_commas_suppliers['Plant']
    sep_commas_suppliers = sep_commas_suppliers.groupby(['#1','ANC'])['#2'].apply(','.join).reset_index()
    main_table_n2_data = pd.merge(main_table_idco_sap_data, sep_commas_suppliers, left_on= ['#1','ANC'], right_on= ['#1','ANC'], how = 'left')
    main_table_n2_data['#2_one'] = main_table_n2_data['Supplier'] + main_table_n2_data['Plant']
    main_table_n2_data['#2_Final'] = np.where(main_table_n2_data['#2'].notna(), main_table_n2_data['#2'] , main_table_n2_data['#2_one'])
    del main_table_n2_data['#2_one'],main_table_n2_data['#2']
    main_table_n2_data.rename(columns={'#2_Final': '#2'}, inplace=True)
    main_table_n2_data


    #### filling suppliers names from sap description
    #### the most lastest step in this script - exploding #2 on separate values and merging this with sap file

    df_sap_ibp_supp_name_join = df_sap_ibp_supp_name[['#','Supplier Description']].drop_duplicates()
    df_sap_ibp_supp_name_join.rename(columns={'#': '#2'}, inplace=True)
    df_sap_ibp_supp_name_join.rename(columns={'Supplier Description': 'Local_Supplier_SAP'}, inplace=True)

    splitter_n2 = main_table_n2_data[main_table_n2_data['#2'].str.contains(',')]
    splitter_n2['#2'] = splitter_n2['#2'].str.split(',')
    sep_commas_spuppliers = splitter_n2.explode('#2').reset_index(drop=True)

    cols = list(sep_commas_spuppliers.columns)
    sep_commas_spuppliers = sep_commas_spuppliers[cols]
    sep_commas_spuppliers
    main_table_stk_cost_data = pd.merge(sep_commas_spuppliers, df_sap_ibp_supp_name_join, left_on= '#2', right_on= '#2', how = 'left')
    main_table_stk_cost_data = pd.merge(main_table_n2_data, main_table_stk_cost_data[['#1','Local_Supplier_SAP']], left_on= '#1', right_on= '#1', how = 'left')
    eliminated_table = main_table_stk_cost_data.drop(['Local_Supplier_SAP','Local Supplier'], axis=1)
    main_table_stk_cost_data = main_table_stk_cost_data.drop_duplicates(subset = list(eliminated_table.columns), keep='first')
    main_table_stk_cost_data['Local_Supplier_Final'] = np.where(main_table_stk_cost_data['Local_Supplier_SAP'].notna(), main_table_stk_cost_data['Local_Supplier_SAP'] , main_table_stk_cost_data['Local Supplier'])
    del main_table_stk_cost_data['Local_Supplier_SAP'],main_table_stk_cost_data['Local Supplier']
    main_table_stk_cost_data.rename(columns={'Local_Supplier_Final': 'Local_Supplier'}, inplace=True)

    exportDF = main_table_stk_cost_data.to_csv(export_db +  '\\Gross_Forecast_' + time.strftime('%Y%m%d') +'.csv', encoding='utf-8', index=False)

    end_time = time.monotonic()
    print("Time of raport calculation is - ", timedelta(seconds=end_time - start_time))


Export_Gross_Main_Report()
# df_sap_ibp_supp_name_join = df_sap_ibp_supp_name[['#','Supplier Description']].drop_duplicates()
# df_sap_ibp_supp_name_join
# splitter_supps
# .str.cat(splitter_supps['Plant'])
# new = data["Team"].copy()

# data["Name"]= data["Name"].str.cat(new, sep =", ")

# splitter_supps['Supps'] = splitter_supps['Supplier'].str.split(',')
# sep_commas_spuppliers = splitter_supps.explode('Supps').reset_index(drop=True)
# sep_commas_spuppliers

