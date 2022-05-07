import glob
import os

import numpy as np
import pandas as pd
# import xlrd
desired_width=320

pd.set_option('display.width', desired_width)

# np.set_option(linewidth=desired_width)

pd.set_option('display.max_columns',15)


# DestinationPath = "\\\\PLWS0125.biz.electrolux.com\\share$\\PLL-SP\\Public\\SUPPLY PLANNING\\DATA EXPORT IDW VMI FSDS"
DestinationPath = r"C:\\InventoryBalanceData"

# destCSV = "\\\\PLWS0125.biz.electrolux.com\\share$\\PLL-SP\\Public\\SUPPLY PLANNING\\DATA EXPORT IDW VMI FSDS\\Main Data"
destCSV = r'C:\\InventoryBalanceData\\'
pd.options.mode.chained_assignment = None  # default='warn'


def convert_csv_idw_vmi():
    listIDW = [f for f in glob.glob(DestinationPath + "\*.xlsx")]
    FilesForDelete = glob.glob(os.path.join(destCSV, "*.csv"))
    for f in FilesForDelete:
        os.remove(f)
        print(f + " - was deleted")
    print(listIDW)
    for i in range(0, len(listIDW)):
        # print(listIDW[i])
        pathname = listIDW[i]
        path_list = pathname.split(os.sep)
        print(path_list[-1][:-5])
        if i == 0:
            data_xls = pd.read_excel(listIDW[i], index_col=None, skiprows=0, engine = "xlrd")
            data_xls.to_csv(destCSV + path_list[-1][:-5] + '.csv', encoding='utf-8', index=False)
        elif i == 1:
            data_xls = pd.read_excel(listIDW[i], index_col=None, skiprows=0, engine = "xlrd")
            data_xls.to_csv(destCSV + path_list[-1][:-5] + '.csv', encoding='utf-8', index=False)
        elif i == 2:
            data_xls = pd.read_excel(listIDW[i], index_col=None, skiprows=0, engine = "xlrd")
            data_xls.to_csv(destCSV + path_list[-1][:-5] + '.csv', encoding='utf-8', index=False)
        elif i == 6:
            data_xls = pd.read_excel(listIDW[i], index_col=None, skiprows=0, engine = "xlrd")
            data_xls.to_csv(destCSV + path_list[-1][:-5] + '.csv', encoding='utf-8', index=False)
        elif i == 7:
            data_xls = pd.read_excel(listIDW[i], index_col=None, skiprows=7, engine = "xlrd")
            vmi_df = data_xls[data_xls['Ownership'] == 'Supplier']
            vmi_mask_stock = vmi_df['Available Stock at VMI Store'].astype(int) > 0
            vmi_df = vmi_df[vmi_mask_stock]
            vmi_df['#1'] = vmi_df['Part No'].astype(str) + 'DGT'
            cleanedData = vmi_df.iloc[:, [17, 0, 2, 3, 8]]
            copiedData = cleanedData.copy(deep=True)
            vmi = copiedData.astype(str)
            vmi['Sum of VMI Store'] = vmi['Available Stock at VMI Store'].astype(int)
            vmi_dubl = vmi.groupby('Part No').agg(
                {'#1': 'first', 'Supplier ID': ', '.join, 'Sum of VMI Store': 'sum'}).reset_index()
            vmi_dubl.to_csv(destCSV + path_list[-1][:-5] + '.csv', encoding='utf-8', index=False)
        else:
            data_xls = pd.read_excel(listIDW[i], index_col=None, engine = "xlrd")
            data_xls.to_csv(destCSV + path_list[-1][:-5] + '.csv', encoding='utf-8', index=False)


def is_non_zero_file(fpath):
    return os.path.isfile(fpath) and os.path.getsize(fpath) > 0


def merge_in_one_idw_files():
    # FilesForDelete = glob.glob(os.path.join(destCSV, "IDW_balance_enquiry.csv"))
    # for f in FilesForDelete:
    #     os.remove(f)
    #     print(f + " - was deleted")
    listFiles = [f for f in glob.glob(destCSV + "\*.csv")]
    dateOfDownload = str(listFiles[1][:-4]).split(" ")[1]
    ### merging idw balance enquiry in one csv file
    try:
        idw_balance1 = pd.read_csv(listFiles[0],  on_bad_lines='skip')
        idw_balance1.rename(columns={idw_balance1.columns[-1]: 'INV_date'}, inplace=True)

        idw_balance2 = pd.read_csv(listFiles[1],  on_bad_lines='skip')
        idw_balance2.rename(columns={idw_balance2.columns[-1]: 'INV_date'}, inplace=True)

        idw_balance3 = pd.read_csv(listFiles[2],  on_bad_lines='skip')
        idw_balance3.rename(columns={idw_balance3.columns[-1]: 'INV_date'}, inplace=True)
        merged_df_idw_balance = [idw_balance1, idw_balance2, idw_balance3]
        idw_balance = pd.concat(merged_df_idw_balance)
    except:
        idw_balance1 = pd.read_csv(listFiles[0],  on_bad_lines='skip')
        idw_balance1.rename(columns={idw_balance1.columns[-1]: 'INV_date'}, inplace=True)
        idw_balance2 = pd.read_csv(listFiles[1],  on_bad_lines='skip')
        idw_balance2.rename(columns={idw_balance2.columns[-1]: 'INV_date'}, inplace=True)
        merged_df_idw_balance = [idw_balance1, idw_balance2]
        idw_balance = pd.concat(merged_df_idw_balance)
    # print(idw_balance[idw_balance['ANC']=='357227570'])
    # idw_balance['#1'] = idw_balance['ANC'].astype(str) + idw_balance['Plant']
    # idw_balance['#2'] = idw_balance['Local Supp'].astype(str) + idw_balance['Plant']
    cleanedData = idw_balance.iloc[:, [idw_balance.columns.get_loc("Plant"),
                                       idw_balance.columns.get_loc("ANC"),
                                       idw_balance.columns.get_loc("Description"),
                                       idw_balance.columns.get_loc("Bal Cat"),
                                       idw_balance.columns.get_loc("BC"),
                                       idw_balance.columns.get_loc("Local Supp"),
                                       idw_balance.columns.get_loc("Suppname"),
                                       idw_balance.columns.get_loc("PMS Supplier"),
                                       idw_balance.columns.get_loc("IDCO"),
                                       idw_balance.columns.get_loc("STK_EUR"),
                                       idw_balance.columns.get_loc("INV_date")]]
    # print(cleanedData[cleanedData['ANC']=='357227570'])

    # cleanedData = cleanedData.loc[(cleanedData['BC'] == 10) | (cleanedData['BC'] == 30) | (cleanedData['BC'] == 50)]
    cleanedData = cleanedData[~cleanedData['Bal Cat'].isin(['VMI Total'])]
    cleanedData.rename(columns={'Bal Cat': 'Bal_Cat'}, inplace=True)
    cleanedData.rename(columns={'Local Supp': 'Local_Supp'}, inplace=True)
    cleanedData['Bal_Cat'] = cleanedData['Bal_Cat'].replace([np.NAN], 'Balance')
    cleanedData['#2'] = cleanedData['Local_Supp'].astype(str) + cleanedData['Plant']
    cleanedData['#1'] = cleanedData['ANC'].astype(str) + cleanedData['Plant']
    cleanedData = cleanedData.loc[(cleanedData['BC'] == 10) | (cleanedData['BC'] == 30) | (cleanedData['BC'] == 50)]
    cleanedData = cleanedData.drop_duplicates()
    cleanedData.to_csv(destCSV + 'INV_merged_files\\IDW_Balance_Enquiry.csv', index=False, encoding='utf-8')
    # cleanedData.to_csv(destCSV + 'INV_merged_files\\IDW_balance_enquiry.csv', encoding='utf-8', index=False, sep='|')
    # # #
    #### merging idw cost enquiry in one csv file
    idw_cost_enq1 = pd.read_csv(listFiles[3],  on_bad_lines='skip', low_memory=False)
    # print(idw_cost_enq1)
    idw_cost_enq2 = pd.read_csv(listFiles[4],  on_bad_lines='skip', low_memory=False)
    # print(idw_cost_enq2)
    idw_cost_enq3 = pd.read_csv(listFiles[5],  on_bad_lines='skip', low_memory=False)
    # print(idw_cost_enq3)
    merged_df_idw_cost_enq = [idw_cost_enq1, idw_cost_enq2, idw_cost_enq3]
    idw_cost_enq = pd.concat(merged_df_idw_cost_enq)
    idw_cost_enq['#1'] = idw_cost_enq['ANC'].astype(str) + idw_cost_enq['Plant']
    cleanedData = idw_cost_enq.iloc[:, [idw_cost_enq.columns.get_loc("#1"),
                                        idw_cost_enq.columns.get_loc("Plant"),
                                        idw_cost_enq.columns.get_loc("ANC"),
                                        idw_cost_enq.columns.get_loc("ANC Description"),
                                        idw_cost_enq.columns.get_loc("BC"),
                                        idw_cost_enq.columns.get_loc("IDCO"),
                                        idw_cost_enq.columns.get_loc("Conv_STK3 Calc")]]
    cleanedData.rename(columns={'ANC Description': 'ANC_Description'}, inplace=True)
    cleanedData.rename(columns={'Conv_STK3 Calc': 'Conv_STK3_Calc'}, inplace=True)
    cleanedData.to_csv(destCSV + 'INV_merged_files\\IDW_Cost_Enquiry.csv', encoding='utf-8', index=False)


merge_in_one_idw_files()
