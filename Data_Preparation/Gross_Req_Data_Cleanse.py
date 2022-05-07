import pandas as pd
import glob
import os

InventoryBalanceDataFolder = r'C:\InventoryBalanceData'
gross_req_forecast = r'C:\\InventoryBalanceData\\INV_merged_files\\'


def gross_forecast_convert_clean_up():
    inv_gross = glob.glob(os.path.join(InventoryBalanceDataFolder, "IDW_material_gross_forecast*"))
    inv_cost_enq = glob.glob(os.path.join(InventoryBalanceDataFolder + "\\INV_merged_files", "IDW_cost_enquiry*"))

    df_inv_gross = pd.read_csv(inv_gross[0],  low_memory=False)
    df_inv_cost_enq = pd.read_csv(inv_cost_enq[0],  low_memory=False)

    df_inv_gross_first_part = df_inv_gross.iloc[:, [0, 1, 2, 5, 8, 9, 11, 14, 16]]

    df_inv_gross_second_part = df_inv_gross.iloc[:, 25:80]
    i = 0
    for wk in df_inv_gross_second_part:
        df_inv_gross_second_part.rename(columns={wk: 'wk' + str(i)}, inplace=True)
        i += 1
    combined_dataframes = [df_inv_gross_first_part, df_inv_gross_second_part]
    combined_result = pd.concat(combined_dataframes, axis=1)
    combined_result["#1"] = combined_result['ANC'].astype(str) + combined_result['Plant'].astype(str)
    combined_result.to_csv(gross_req_forecast + 'IDW_Gross_Forecast.csv', encoding='utf-8', index=False)

