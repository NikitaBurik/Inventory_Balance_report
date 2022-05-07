import glob
import os

import numpy as np

import pandas as pd

pd.set_option('display.float_format', lambda x: '%.5f' % x)
pd.options.mode.chained_assignment = None
import time
from datetime import timedelta

#############################################################################################
### import danych z przygotowanych przez SQL'a plików - Inventory Balance i Gross Requirement
#############################################################################################
start_time = time.monotonic()

prep_reps = r'C:\InventoryBalanceData\Prepared_Reps'
calc_reps = r'C:\InventoryBalanceData\Calculated_Reps'
invCSV = glob.glob(os.path.join(prep_reps, "InvBalance_*"))
# invCSV = r'C:\for_report\InvBalance_20211203.csv'
requirementCSV = glob.glob(os.path.join(prep_reps, "Gross_Forecast_*"))
# requirementCSV = r'C:\for_report\Gross_Forecast_20211203.csv'
safetyStockXlsxFold = r'\\plws0125\share$\PLL-SP\Public\SUPPLY PLANNING\DATA EXPORT IDW VMI FSDS\SS'
safetyStockXlsx = glob.glob(os.path.join(safetyStockXlsxFold, "Safety_Stock_*"))

invDF = pd.read_csv(invCSV[-1],  dtype={'Value': np.float32})
grossDF = pd.read_csv(requirementCSV[-1])
safetyStockSSDF = pd.read_excel(safetyStockXlsx[0], skiprows=0)

### Wybór konkretnych kolumn, zaokrąglanie i zamiana pustych pól na zera
Inv_Bal_Cleaned = invDF[
    ['Indop', 'Plant', 'ANC', 'Description', 'Bal_Cat', 'Cat', 'BC', 'Supplier_ID', 'Supplier_Name', 'IDCO', 'CMDT',
     'STK_EUR', 'Value']]
Inv_Bal_Cleaned['Value'] = Inv_Bal_Cleaned['Value'].astype(float).round(decimals=2)
Inv_Bal_Cleaned['Value'] = Inv_Bal_Cleaned['Value'].fillna(0)
Inv_Bal_Cleaned['Value'] = Inv_Bal_Cleaned['Value'].map('{:.2f}'.format)
Inv_Bal_Cleaned['Value'] = Inv_Bal_Cleaned['Value'].astype(float)

### wybieramy konkretne kolumny i 54 tygodni, zaokrąglamy liczby
Gross_Cleaned = grossDF[['Plant', 'ANC', 'Description', 'BC', 'Supplier', 'Local_Supplier', 'DM', 'IDCO']]
weekHorizont = grossDF.iloc[:, 5:59]
weekHorizont = weekHorizont.fillna(0)  # or 0 or any value you want here
weekHorizont = weekHorizont.astype(int)
combined_dataframes = [Gross_Cleaned, weekHorizont]
gross_result = pd.concat(combined_dataframes, axis=1)

#############################################################################
############### Tabela przestawna z INV  Factory i Hub
#############################################################################
INV_FQ_HQ = pd.pivot_table(Inv_Bal_Cleaned, index=['ANC', 'Plant'], columns=['Cat'], values=['Value'], aggfunc=np.sum)
INV_FQ_HQ = INV_FQ_HQ.fillna(0)
INV_FQ_HQ = INV_FQ_HQ.round(decimals=1)
INV_FQ_HQ = INV_FQ_HQ.reset_index()
INV_FQ_HQ = INV_FQ_HQ.fillna(0)
df = pd.merge(Inv_Bal_Cleaned, INV_FQ_HQ, on=['ANC', 'Plant'], how='left')
# df[df.iloc[:,13]>0]

df.rename(columns={df.columns[13]: 'INV F Q'}, inplace=True)
df.rename(columns={df.columns[15]: 'INV H Q'}, inplace=True)
df = df.drop(df.columns[14], axis=1)  # drop a column

### Obliczenie INV Totalu Q i Volumenów
df["INV T Q"] = df["INV F Q"] + df["INV H Q"]
df["INV T Q"] = df["INV T Q"].astype(float)
df["INV F V"] = np.where(df["INV F Q"] < 0, 0, df["INV F Q"] * df["STK_EUR"])
df["INV H V"] = np.where(df["INV H Q"] < 0, 0, df["INV H Q"] * df["STK_EUR"])
df["INV F V"] = df["INV F V"].round(decimals=2)
df["INV H V"] = df["INV H V"].round(decimals=2)
df["INV T V"] = df["INV F V"] + df["INV H V"]

####################################################################################
############# Połączenie dwóch raportów po ANC i Plant
####################################################################################
dfnew = pd.merge(df, gross_result, on=['ANC', 'Plant'], how='left')
# dfnew = pd.merge(df, gross_result, left_on= ['ANC', 'Plant'], right_on= ['ANC', 'Plant'], how = 'left')
dfnew = dfnew.fillna(0)



############################################################################################
### it works the most najlepiej - the new time record - 3 seconds zamiast 7 minut!!!
### Funkcja do obliczenia Days Inventory Outstanding - pokazuje nam na ile dni mamy pokrycie
### naszego komponentu w odniesieniu od aktualnego Stock'u
############################################################################################
def calculation_of_DIO_F_H_Total(df_combined):
    df_combined['Value_F_H_Sum'] = df_combined['INV F Q'] + df_combined['INV H Q']
    df_combined['INV F Q'] = df_combined.iloc[:, df_combined.columns.get_loc('INV F Q')]
    df_combined['INV H Q'] = df_combined.iloc[:, df_combined.columns.get_loc('INV H Q')]
    df_54_weeks = df_combined.iloc[:, 25:79]
    df_weeks_transposed = df_54_weeks.transpose()
    df_weeks_summed = df_weeks_transposed.iloc[0:, :].cumsum()
    df_weeks_transposed_back = df_weeks_summed.transpose()
    dfinvfq = pd.merge(df_combined['Value_F_H_Sum'], df_weeks_transposed_back, left_index=True, right_index=True)
    dfinvfq['Value_F_H_Sum'] = dfinvfq['Value_F_H_Sum'].round(decimals=1)
    dfinvfq = dfinvfq.iloc[:, 0:55].astype(float)
    dfinvfq['DIO T'] = ""
    dfinvfq['Pos'] = ""
    for x in dfinvfq.itertuples():
        c = 0
        d = 0
        for j in range(1, 55):
            if x[1] < x[2]:
                c = 2
            elif x[1] > x[j] and x[1] < x[j + 1]:
                c = j + 1
        if x[c] == 0:
            d = 0
        else:
            d = x[1] * 7 / x[c]
        if d != 0:
            d += 7 * (c - 2)
        else:
            d = 0
        dfinvfq.at[x.Index, 'Pos'] = c - 1
        dfinvfq.at[x.Index, 'DIO T'] = d
    df_fh_q = pd.merge(df_combined[['INV F Q', 'INV H Q']], dfinvfq, left_index=True, right_index=True)
    df_fh_q['DIO T'] = [x[df_fh_q.columns.get_loc('DIO T') + 1] if x[df_fh_q.columns.get_loc('Pos') + 1] > 0 else 0 for x in df_fh_q.itertuples()]
    df_fh_q['DIO F'] = [0 if x[df_fh_q.columns.get_loc('Value_F_H_Sum') + 1] == 0 else (x[df_fh_q.columns.get_loc('INV F Q') + 1] / x[df_fh_q.columns.get_loc('Value_F_H_Sum') + 1]) * x[df_fh_q.columns.get_loc('DIO T') + 1] for x in df_fh_q.itertuples()]
    df_fh_q['DIO H'] = [0 if x[df_fh_q.columns.get_loc('Value_F_H_Sum') + 1] == 0 else (x[df_fh_q.columns.get_loc('INV H Q') + 1] / x[df_fh_q.columns.get_loc('Value_F_H_Sum') + 1]) * x[df_fh_q.columns.get_loc('DIO T') + 1] for x in df_fh_q.itertuples()]
    df_fh_q['MRP Q'] = [x[df_fh_q.columns.get_loc('wk53') + 1] for x in df_fh_q.itertuples()]
    return df_fh_q


df_dio_calc = calculation_of_DIO_F_H_Total(dfnew)


def merge_DIOs_with_TOTAL(df_dio, dfTOTAL):
    df_DIO = df_dio.iloc[:, 57:62]
    del df_DIO['Pos']
    df_DIO_merged = pd.merge(dfTOTAL, df_DIO, left_index=True, right_index=True)
    del df_DIO_merged['Value_F_H_Sum']
    df_DIO_merged['MRP V'] = [x[df_DIO_merged.columns.get_loc('STK_EUR')+1] * x[df_DIO_merged.columns.get_loc('MRP Q')+1] for x in df_DIO_merged.itertuples()]
    return df_DIO_merged


df_DIO_Total = merge_DIOs_with_TOTAL(df_dio_calc, dfnew)


def after_merge_DIO_with_TOTAL_clean(df_total):
    combined_result = pd.concat([df_DIO_Total.iloc[:, :df_DIO_Total.columns.get_loc("wk0")],
                                 df_DIO_Total.iloc[:, df_DIO_Total.columns.get_loc("DIO T"):]], axis=1)
    del combined_result['IDCO_y']
    del combined_result['Description_y']
    del combined_result['BC_y']
    del combined_result['Supplier']
    del combined_result['Local_Supplier']
    combined_result.rename(columns={'Description_x': 'Description'}, inplace=True)
    combined_result.rename(columns={'BC_x': 'BC'}, inplace=True)
    combined_result.rename(columns={'IDCO_x': 'IDCO'}, inplace=True)
    combined_result['DIO T'] = [
        0 if x[combined_result.columns.get_loc("DIO T") + 1] < 0 else x[combined_result.columns.get_loc("DIO T") + 1]
        for x in combined_result.itertuples()]
    return combined_result



df_dio_clean = after_merge_DIO_with_TOTAL_clean(df_DIO_Total)
df_dio_clean['DIS F'] = [
    x[df_dio_clean.columns.get_loc("INV F Q") + 1] / (x[df_dio_clean.columns.get_loc("MRP Q") + 1] / 360) if x[
                                                                                                                 df_dio_clean.columns.get_loc(
                                                                                                                     "MRP Q") + 1] > 0 else 0
    for x in df_dio_clean.itertuples()]
df_dio_clean['DIS H'] = [
    x[df_dio_clean.columns.get_loc("INV H Q") + 1] / (x[df_dio_clean.columns.get_loc("MRP Q") + 1] / 360) if x[
                                                                                                                 df_dio_clean.columns.get_loc(
                                                                                                                     "MRP Q") + 1] > 0 else 0
    for x in df_dio_clean.itertuples()]
df_dio_clean['DIS T'] = df_dio_clean['DIS F'] + df_dio_clean['DIS H']


### Funkcje do obliczenia DIO ważonych dla różnych CMDT, Plant, Supplier, Indop
def DIO_Weight_CMDT_Total(df_dio_clean):
    df_weight = df_dio_clean.groupby(["ANC", "CMDT"])['MRP V'].sum().reset_index()
    df_weight['MRP V'] = df_weight['MRP V'].round(5)
    dfnew_weight_dio = pd.merge(df_weight, df_dio_clean[['ANC', 'CMDT', 'DIO T']], on=['ANC', 'CMDT'], how='left')

    dfnew_weight_dio['MRP_Summ'] = dfnew_weight_dio.groupby(["CMDT"])["MRP V"].transform('sum')
    dfnew_weight_dio['DIO Average'] = dfnew_weight_dio.groupby(["CMDT"])["DIO T"].transform('mean')

    dfnew_weight_dio['W_CMDT_T'] = [
        x[1][dfnew_weight_dio.columns.get_loc("MRP V")] / x[1][dfnew_weight_dio.columns.get_loc("MRP_Summ")] if x[1][
                                                                                                                    dfnew_weight_dio.columns.get_loc(
                                                                                                                        "MRP_Summ")] > 0 else 0
        for x in dfnew_weight_dio.iterrows()]
    dfnew_weight_dio['DIO T x Weight'] = [
        x[1][dfnew_weight_dio.columns.get_loc("DIO T")] * x[1][dfnew_weight_dio.columns.get_loc("W_CMDT_T")] for x in
        dfnew_weight_dio.iterrows()]
    dict_average_weighted_dio_summ = dfnew_weight_dio.groupby("CMDT")["DIO T x Weight"].sum()
    dfnew_weight_dio['ACC_DIO_T_CMDT_T'] = dfnew_weight_dio['CMDT'].map(dict_average_weighted_dio_summ)
    del dfnew_weight_dio["DIO T x Weight"]
    return dfnew_weight_dio


def DIO_Weight_CMDT_Plant(df_dio_clean):
    df_weight = df_dio_clean.groupby(["ANC", "CMDT", "Plant"])['MRP V'].sum().reset_index()
    df_weight['MRP V'] = df_weight['MRP V'].round(5)
    dfnew_weight_dio = pd.merge(df_weight, df_dio_clean[['ANC', 'CMDT', 'Plant', 'DIO T']], on=['ANC', 'CMDT', 'Plant'],
                                how='left')
    # dfnew_weight_dio[(dfnew_weight_dio['CMDT']=='Steel') & (dfnew_weight_dio['Plant'] == 'UKE')]['MRP V'].sum()
    # summ_of_cmdt_plant_mrp = dfnew_weight_dio.groupby(["CMDT", "Plant"])["MRP V"].sum()

    dfnew_weight_dio['MRP_Summ'] = dfnew_weight_dio.groupby(["CMDT", "Plant"])["MRP V"].transform('sum')
    dfnew_weight_dio['DIO Average'] = dfnew_weight_dio.groupby(["CMDT", "Plant"])["DIO T"].transform('mean')

    dfnew_weight_dio['W_CMDT_P'] = [
        x[1][dfnew_weight_dio.columns.get_loc("MRP V")] / x[1][dfnew_weight_dio.columns.get_loc("MRP_Summ")] if x[1][
                                                                                                                    dfnew_weight_dio.columns.get_loc(
                                                                                                                        "MRP_Summ")] > 0 else 0
        for x in dfnew_weight_dio.iterrows()]
    dfnew_weight_dio['DIO T x Weight'] = [
        x[1][dfnew_weight_dio.columns.get_loc("DIO T")] * x[1][dfnew_weight_dio.columns.get_loc("W_CMDT_P")] for x in
        dfnew_weight_dio.iterrows()]
    dfnew_weight_dio['ACC_DIO_T_CMDT_P'] = dfnew_weight_dio.groupby(["CMDT", "Plant"])["DIO T x Weight"].transform(
        'sum')
    del dfnew_weight_dio["DIO T x Weight"]
    return dfnew_weight_dio


def DIO_Weight_Supplier(df_dio_clean):
    df_weight = df_dio_clean.groupby(["ANC", "Supplier_ID", "Plant"])['MRP V'].sum().reset_index()
    df_weight['MRP V'] = df_weight['MRP V'].round(5)
    dfnew_weight_dio = pd.merge(df_weight, df_dio_clean[['ANC', 'Supplier_ID', 'Plant', 'DIO T']],
                                on=['ANC', 'Supplier_ID', 'Plant'], how='left')
    # dfnew_weight_dio[(dfnew_weight_dio['CMDT']=='Steel') & (dfnew_weight_dio['Plant'] == 'UKE')]['MRP V'].sum()
    # summ_of_cmdt_plant_mrp = dfnew_weight_dio.groupby(["CMDT", "Plant"])["MRP V"].sum()

    dfnew_weight_dio['MRP_Summ'] = dfnew_weight_dio.groupby(["Supplier_ID", "Plant"])["MRP V"].transform('sum')
    dfnew_weight_dio['DIO Average'] = dfnew_weight_dio.groupby(["Supplier_ID", "Plant"])["DIO T"].transform('mean')

    dfnew_weight_dio['W_Supp_P'] = [
        x[1][dfnew_weight_dio.columns.get_loc("MRP V")] / x[1][dfnew_weight_dio.columns.get_loc("MRP_Summ")] if x[1][
                                                                                                                    dfnew_weight_dio.columns.get_loc(
                                                                                                                        "MRP_Summ")] > 0 else 0
        for x in dfnew_weight_dio.iterrows()]
    dfnew_weight_dio['DIO T x Weight'] = [
        x[1][dfnew_weight_dio.columns.get_loc("DIO T")] * x[1][dfnew_weight_dio.columns.get_loc("W_Supp_P")] for x in
        dfnew_weight_dio.iterrows()]
    dfnew_weight_dio['ACC_DIO_T_SUPP_P'] = dfnew_weight_dio.groupby(["Supplier_ID", "Plant"])[
        "DIO T x Weight"].transform('sum')
    del dfnew_weight_dio["DIO T x Weight"]
    return dfnew_weight_dio


def DIO_Weight_Plant(df_dio_clean):
    df_weight = df_dio_clean.groupby(["ANC", "Plant"])['MRP V'].sum().reset_index()
    df_weight['MRP V'] = df_weight['MRP V'].round(5)
    dfnew_weight_dio = pd.merge(df_weight, df_dio_clean[['ANC', 'Plant', 'DIO T']], on=['ANC', 'Plant'], how='left')

    dfnew_weight_dio['MRP_Summ'] = dfnew_weight_dio.groupby(["Plant"])["MRP V"].transform('sum')
    dfnew_weight_dio['DIO Average'] = dfnew_weight_dio.groupby(["Plant"])["DIO T"].transform('mean')

    dfnew_weight_dio['W_Plant'] = [
        x[1][dfnew_weight_dio.columns.get_loc("MRP V")] / x[1][dfnew_weight_dio.columns.get_loc("MRP_Summ")] if x[1][
                                                                                                                    dfnew_weight_dio.columns.get_loc(
                                                                                                                        "MRP_Summ")] > 0 else 0
        for x in dfnew_weight_dio.iterrows()]
    dfnew_weight_dio['DIO T x Weight'] = [
        x[1][dfnew_weight_dio.columns.get_loc("DIO T")] * x[1][dfnew_weight_dio.columns.get_loc("W_Plant")] for x in
        dfnew_weight_dio.iterrows()]
    dfnew_weight_dio['ACC_DIO_T_PLANT'] = dfnew_weight_dio.groupby(["Plant"])["DIO T x Weight"].transform('sum')
    del dfnew_weight_dio["DIO T x Weight"]
    return dfnew_weight_dio


def DIO_Weight_Indop(df_dio_clean):
    df_weight = df_dio_clean.groupby(["ANC", "Indop"])['MRP V'].sum().reset_index()
    df_weight['MRP V'] = df_weight['MRP V'].round(5)
    dfnew_weight_dio = pd.merge(df_weight, df_dio_clean[['ANC', 'Indop', 'DIO T']], on=['ANC', 'Indop'], how='left')

    dfnew_weight_dio['MRP_Summ'] = dfnew_weight_dio.groupby(["Indop"])["MRP V"].transform('sum')
    dfnew_weight_dio['DIO Average'] = dfnew_weight_dio.groupby(["Indop"])["DIO T"].transform('mean')

    dfnew_weight_dio['W_Indop'] = [
        x[1][dfnew_weight_dio.columns.get_loc("MRP V")] / x[1][dfnew_weight_dio.columns.get_loc("MRP_Summ")] if x[1][
                                                                                                                    dfnew_weight_dio.columns.get_loc(
                                                                                                                        "MRP_Summ")] > 0 else 0
        for x in dfnew_weight_dio.iterrows()]
    dfnew_weight_dio['DIO T x Weight'] = [
        x[1][dfnew_weight_dio.columns.get_loc("DIO T")] * x[1][dfnew_weight_dio.columns.get_loc("W_Indop")] for x in
        dfnew_weight_dio.iterrows()]

    dfnew_weight_dio['ACC_DIO_T_INDOP'] = dfnew_weight_dio.groupby(["Indop"])["DIO T x Weight"].transform('sum')
    del dfnew_weight_dio["DIO T x Weight"]
    return dfnew_weight_dio


df_weight_cmdt_t = DIO_Weight_CMDT_Total(df_dio_clean)
df_weight_cmdt_plant = DIO_Weight_CMDT_Plant(df_dio_clean)
df_weight_supplier = DIO_Weight_Supplier(df_dio_clean)
df_weight_plant = DIO_Weight_Plant(df_dio_clean)
df_weight_indop = DIO_Weight_Indop(df_dio_clean)

### Połączenie wag z główną tabelą
df_merged_cmdt_t = pd.merge(df_dio_clean, df_weight_cmdt_t['W_CMDT_T'], left_index=True, right_index=True)
df_merged_cmdt_plant = pd.merge(df_merged_cmdt_t, df_weight_cmdt_plant['W_CMDT_P'], left_index=True, right_index=True)
df_merged_supplier = pd.merge(df_merged_cmdt_plant, df_weight_supplier['W_Supp_P'], left_index=True, right_index=True)
df_merged_plant = pd.merge(df_merged_supplier, df_weight_plant['W_Plant'], left_index=True, right_index=True)
df_merged_indop = pd.merge(df_merged_plant, df_weight_indop['W_Indop'], left_index=True, right_index=True)

### Obliczenie DIO F i DIO H, z proporcji od DIO T dla każdego Weighted Average DIO
df_merged_acc_dio_t = pd.merge(df_merged_indop, df_weight_cmdt_t['ACC_DIO_T_CMDT_T'], left_index=True, right_index=True)
df_merged_acc_dio_t['ACC_DIO_F_CMDT_T'] = [(x[1][df_merged_acc_dio_t.columns.get_loc("ACC_DIO_T_CMDT_T")] * x[1][
    df_merged_acc_dio_t.columns.get_loc("DIO F")]) / x[1][df_merged_acc_dio_t.columns.get_loc("DIO T")] if x[1][
                                                                                                               df_merged_acc_dio_t.columns.get_loc(
                                                                                                                   "DIO T")] > 0 else 0
                                           for x in df_merged_acc_dio_t.iterrows()]
df_merged_acc_dio_t['ACC_DIO_H_CMDT_T'] = [(x[1][df_merged_acc_dio_t.columns.get_loc("ACC_DIO_T_CMDT_T")] * x[1][
    df_merged_acc_dio_t.columns.get_loc("DIO H")]) / x[1][df_merged_acc_dio_t.columns.get_loc("DIO T")] if x[1][
                                                                                                               df_merged_acc_dio_t.columns.get_loc(
                                                                                                                   "DIO T")] > 0 else 0
                                           for x in df_merged_acc_dio_t.iterrows()]

df_merged_acc_dio_cmdt_plant = pd.merge(df_merged_acc_dio_t, df_weight_cmdt_plant['ACC_DIO_T_CMDT_P'], left_index=True,
                                        right_index=True)
df_merged_acc_dio_cmdt_plant['ACC_DIO_F_CMDT_P'] = [(x[1][df_merged_acc_dio_cmdt_plant.columns.get_loc(
    "ACC_DIO_T_CMDT_P")] * x[1][df_merged_acc_dio_cmdt_plant.columns.get_loc("DIO F")]) / x[1][
                                                        df_merged_acc_dio_cmdt_plant.columns.get_loc("DIO T")] if x[1][
                                                                                                                      df_merged_acc_dio_cmdt_plant.columns.get_loc(
                                                                                                                          "DIO T")] > 0 else 0
                                                    for x in df_merged_acc_dio_cmdt_plant.iterrows()]
df_merged_acc_dio_cmdt_plant['ACC_DIO_H_CMDT_P'] = [(x[1][df_merged_acc_dio_cmdt_plant.columns.get_loc(
    "ACC_DIO_T_CMDT_P")] * x[1][df_merged_acc_dio_cmdt_plant.columns.get_loc("DIO H")]) / x[1][
                                                        df_merged_acc_dio_cmdt_plant.columns.get_loc("DIO T")] if x[1][
                                                                                                                      df_merged_acc_dio_cmdt_plant.columns.get_loc(
                                                                                                                          "DIO T")] > 0 else 0
                                                    for x in df_merged_acc_dio_cmdt_plant.iterrows()]

df_merged_acc_dio_supplier = pd.merge(df_merged_acc_dio_cmdt_plant, df_weight_supplier['ACC_DIO_T_SUPP_P'],
                                      left_index=True, right_index=True)
df_merged_acc_dio_supplier['ACC_DIO_F_SUPP_P'] = [(x[1][
                                                       df_merged_acc_dio_supplier.columns.get_loc("ACC_DIO_T_SUPP_P")] *
                                                   x[1][df_merged_acc_dio_supplier.columns.get_loc("DIO F")]) / x[1][
                                                      df_merged_acc_dio_supplier.columns.get_loc("DIO T")] if x[1][
                                                                                                                  df_merged_acc_dio_supplier.columns.get_loc(
                                                                                                                      "DIO T")] > 0 else 0
                                                  for x in df_merged_acc_dio_supplier.iterrows()]
df_merged_acc_dio_supplier['ACC_DIO_H_SUPP_P'] = [(x[1][
                                                       df_merged_acc_dio_supplier.columns.get_loc("ACC_DIO_T_SUPP_P")] *
                                                   x[1][df_merged_acc_dio_supplier.columns.get_loc("DIO H")]) / x[1][
                                                      df_merged_acc_dio_supplier.columns.get_loc("DIO T")] if x[1][
                                                                                                                  df_merged_acc_dio_supplier.columns.get_loc(
                                                                                                                      "DIO T")] > 0 else 0
                                                  for x in df_merged_acc_dio_supplier.iterrows()]

df_merged_acc_dio_plant = pd.merge(df_merged_acc_dio_supplier, df_weight_plant['ACC_DIO_T_PLANT'], left_index=True,
                                   right_index=True)
df_merged_acc_dio_plant['ACC_DIO_F_PLANT'] = [(x[1][df_merged_acc_dio_plant.columns.get_loc("ACC_DIO_T_PLANT")] * x[1][
    df_merged_acc_dio_plant.columns.get_loc("DIO F")]) / x[1][df_merged_acc_dio_plant.columns.get_loc("DIO T")] if x[1][
                                                                                                                       df_merged_acc_dio_plant.columns.get_loc(
                                                                                                                           "DIO T")] > 0 else 0
                                              for x in df_merged_acc_dio_plant.iterrows()]
df_merged_acc_dio_plant['ACC_DIO_H_PLANT'] = [(x[1][df_merged_acc_dio_plant.columns.get_loc("ACC_DIO_T_PLANT")] * x[1][
    df_merged_acc_dio_plant.columns.get_loc("DIO H")]) / x[1][df_merged_acc_dio_plant.columns.get_loc("DIO T")] if x[1][
                                                                                                                       df_merged_acc_dio_plant.columns.get_loc(
                                                                                                                           "DIO T")] > 0 else 0
                                              for x in df_merged_acc_dio_plant.iterrows()]

df_merged_acc_dio_indop = pd.merge(df_merged_acc_dio_plant, df_weight_indop['ACC_DIO_T_INDOP'], left_index=True,
                                   right_index=True)
df_merged_acc_dio_indop['ACC_DIO_F_INDOP'] = [(x[1][df_merged_acc_dio_indop.columns.get_loc("ACC_DIO_T_INDOP")] * x[1][
    df_merged_acc_dio_indop.columns.get_loc("DIO F")]) / x[1][df_merged_acc_dio_indop.columns.get_loc("DIO T")] if x[1][
                                                                                                                       df_merged_acc_dio_indop.columns.get_loc(
                                                                                                                           "DIO T")] > 0 else 0
                                              for x in df_merged_acc_dio_indop.iterrows()]
df_merged_acc_dio_indop['ACC_DIO_H_INDOP'] = [(x[1][df_merged_acc_dio_indop.columns.get_loc("ACC_DIO_T_INDOP")] * x[1][
    df_merged_acc_dio_indop.columns.get_loc("DIO H")]) / x[1][df_merged_acc_dio_indop.columns.get_loc("DIO T")] if x[1][
                                                                                                                       df_merged_acc_dio_indop.columns.get_loc(
                                                                                                                           "DIO T")] > 0 else 0
                                              for x in df_merged_acc_dio_indop.iterrows()]

df_merged_acc_dio_indop['PQ'] = ''
df_merged_acc_dio_indop['PQ_S'] = ''
df_merged_acc_dio_indop['INV T Q [OPT]'] = ''
df_merged_acc_dio_indop['INV T V [OPT]'] = ''
df_merged_acc_dio_indop['OPT DIO [ANC]'] = ''
df_merged_acc_dio_indop['OPT DIS [ANC]'] = ''
df_merged_acc_dio_indop['OPT DIO T [CMDT_T]'] = ''
df_merged_acc_dio_indop['Δ ACC vs OPT [CMDT_T]'] = ''
df_merged_acc_dio_indop['OPT DIO T [CMDT_P]'] = ''
df_merged_acc_dio_indop['Δ ACC vs OPT [CMDT_P]'] = ''
df_merged_acc_dio_indop['OPT DIO T [SUPP_P]'] = ''
df_merged_acc_dio_indop['Δ ACC vs OPT [SUPP_P]'] = ''
df_merged_acc_dio_indop['OPT DIO T [PQ_P]'] = ''
df_merged_acc_dio_indop['Δ ACC vs OPT [PQ_P]'] = ''
df_merged_acc_dio_indop['OPT DIO T [PQ_S_P]'] = ''
df_merged_acc_dio_indop['Δ ACC vs OPT [PQ_S_P]'] = ''
df_merged_acc_dio_indop['OPT DIO T [PLANT]'] = ''
df_merged_acc_dio_indop['Δ ACC vs OPT [PLANT]'] = ''
df_merged_acc_dio_indop['OPT DIO T [INDOP]'] = ''
df_merged_acc_dio_indop['Δ ACC vs OPT [INDOP]'] = ''



more = df_merged_acc_dio_indop[['INV F Q', 'INV H Q', 'INV T Q']]


def Categorization_ABC_MRP_V(df_merged_acc_dio_indop):
    df_weight_data = df_merged_acc_dio_indop.groupby(["Plant", "ANC", "Supplier_ID"])['MRP V'].sum().reset_index()
    df_weight_data.rename(columns={'MRP V': 'MRP V Summ'}, inplace=True)
    df_main = pd.merge(df_merged_acc_dio_indop, df_weight_data[["Plant", "ANC", "Supplier_ID", "MRP V Summ"]],
                       on=["Plant", "ANC", "Supplier_ID"], how='left')
    df_ABC = df_main[["Plant", "ANC", "Supplier_ID", "MRP V", "MRP V Summ"]]
    df_ABC['% Share'] = [x[1][3] / x[1][4] if x[1][4] > 0 else 0 for x in df_ABC.iterrows()]
    df_ABC['% Share [Cumulative]'] = df_ABC.groupby(["Plant", "ANC", "Supplier_ID", "MRP V Summ"])['% Share'].cumsum()
    df_ABC['CAT. MRP V [SUPP_P]'] = [
        'A' if x[1][6] > 0 and x[1][6] <= 0.8 else 'B' if x[1][6] > 0.8 and x[1][6] <= 0.95 else 'C' for x in
        df_ABC.iterrows()]
    return df_ABC


def Categorization_ABC_INV_V(df_merged_acc_dio_indop):
    df_weight_data = df_merged_acc_dio_indop.groupby(["Plant", "ANC", "Supplier_ID"])['Value'].sum().reset_index()
    df_weight_data.rename(columns={'Value': 'INV V Summ'}, inplace=True)
    df_main = pd.merge(df_merged_acc_dio_indop, df_weight_data[["Plant", "ANC", "Supplier_ID", "INV V Summ"]],
                       on=["Plant", "ANC", "Supplier_ID"], how='left')
    df_ABC = df_main[["Plant", "ANC", "Supplier_ID", "Value", "INV V Summ"]]
    df_ABC['% Share'] = [x[1][3] / x[1][4] if x[1][4] > 0 else 0 for x in df_ABC.iterrows()]
    df_ABC['% Share [Cumulative]'] = df_ABC.groupby(["Plant", "ANC", "Supplier_ID", "Value"])['% Share'].cumsum()
    df_ABC['CAT. INV V [SUPP_P]'] = [
        'A' if x[1][6] > 0 and x[1][6] <= 0.8 else 'B' if x[1][6] > 0.8 and x[1][6] <= 0.95 else 'C' for x in
        df_ABC.iterrows()]
    return df_ABC


df_merged_cat_abc_mrp = pd.merge(df_merged_acc_dio_indop,
                                 Categorization_ABC_MRP_V(df_merged_acc_dio_indop)['CAT. MRP V [SUPP_P]'],
                                 left_index=True, right_index=True)
df_merged_cat_abc_inv_t = pd.merge(df_merged_cat_abc_mrp,
                                   Categorization_ABC_INV_V(df_merged_acc_dio_indop)['CAT. INV V [SUPP_P]'],
                                   left_index=True, right_index=True)



### obliczenie iloczynu turnoveru z DM
def TurnOver(gross_result):
    gross_result_copy = gross_result.copy()
    gross_result_copy['TURNOVER'] = gross_result_copy.iloc[:, 8:62].sum(axis=1) * gross_result_copy['DM']
    return gross_result_copy


### funkcja do obliczania safety stock w gross requirement - 3 dzialania cases
gross_req_turnover = TurnOver(gross_result)


#### Ważne pytania do Grzeska:
#### !!! CO czy KTORY konkretnie z SS trzeba MNOŻYĆ NA 7/5 na końcu (jaki sens?)?
#### !!! w filmie brałeś SSD a nie SS jak w instrukcji dla case 2
#### !!! czy w przypadku case 1 mergować po Plant + Supplier? bo inaczej sie nie da i jest bez sensu
#### !!! czy plik z SS bedzie zawsze tak przygotowany i dostarczony tak jak ostatnio do instrukcji??


def Gross_Req_SS_Cases123(gross_req_turnover):
    gross_result_copy = gross_req_turnover.copy()
    SS_n1_cleaned_duplicates = safetyStockSSDF.copy()
    SS_n2_cleaned_duplicates = safetyStockSSDF.copy()

    SS_n1_cleaned_duplicates.drop_duplicates(subset='#.1',
                                             keep=False, inplace=True)
    SS_n2_cleaned_duplicates.drop_duplicates(subset='#.2',
                                             keep=False, inplace=True)

    gross_result_copy['#1'] = gross_result_copy['ANC'].astype('string') + gross_result_copy['Plant'].astype('string')

    gross_result_copy['#2'] = gross_result_copy['Supplier'].str.lstrip('0') + gross_result_copy['Plant']

    SS_n1_cleaned_duplicates.rename(columns={'#.1': '#1'}, inplace=True)
    SS_n1_cleaned_duplicates.rename(columns={"CID": 'Plant'}, inplace=True)
    SS_n1_cleaned_duplicates.rename(columns={"Product ID": 'ANC'}, inplace=True)
    SS_n2_cleaned_duplicates.rename(columns={'#.2': '#2'}, inplace=True)

    # # gross_result['#2'] = gross_result['Supplier'] + gross_result['Plant']
    case_2_ss_anc_merging = pd.merge(gross_result_copy, SS_n1_cleaned_duplicates[["Plant", "ANC", "SS"]],
                                     on=["Plant", "ANC"], how='left')
    case_2_ss_supp_merging = pd.merge(case_2_ss_anc_merging, SS_n2_cleaned_duplicates[["#2", "SS.1"]], on=["#2"],
                                      how='left')
    case_2_ss_supp_merging.rename(columns={'SS.1': 'SS 2 Case 2'}, inplace=True)
    # case_2_ss_supp_merging[case_2_ss_supp_merging['SS 2 Case 2'].notna()]
    # case_2_ss_cleaned = case_2_ss_supp_merging[case_2_ss_supp_merging['SS 2 Case 2'].notna()]

    ### case 1 - vlookup and explode commas
    case_1_SS = gross_result_copy[gross_result_copy['Supplier'].str.contains(',')]
    case_1_SS['Supplier'] = case_1_SS['Supplier'].str.split(',')
    sep_commas_spuppliers = case_1_SS.explode('Supplier').reset_index(drop=True)
    cols = list(sep_commas_spuppliers.columns)
    # cols.append(cols.pop(cols.index('CustomerName')))
    sep_commas_suppliers = sep_commas_spuppliers[cols]
    del sep_commas_suppliers['#2']
    sep_commas_suppliers['#2'] = sep_commas_suppliers['Supplier'].str.lstrip('0') + sep_commas_suppliers['Plant']

    case_1_ss_merging = pd.merge(sep_commas_suppliers, SS_n2_cleaned_duplicates[["#2", "SS.1"]], on=["#2"], how='left')
    case_1_ss_merging['SS.1'] = case_1_ss_merging['SS.1'].fillna(0)
    case_1_ss_merging['SS Case 1'] = case_1_ss_merging.groupby(["Plant", "ANC"])['SS.1'].transform('mean')
    # del case_1_ss_merging['SS.1'], case_1_ss_merging['#1'], case_1_ss_merging['#2']
    # del case_1_ss_merging ['Supplier'], case_1_ss_merging ['#2'], case_1_ss_merging ['SS.1']
    # case_1_ss_merging.drop_duplicates()
    case_1_ss = pd.merge(case_2_ss_supp_merging, case_1_ss_merging[["Plant", "ANC", "SS Case 1"]].drop_duplicates(),
                         on=["Plant", "ANC"], how='left')
    case_1_ss.rename(columns={'SS': 'SS 1 Case 2'}, inplace=True)
    case_1_ss['SS'] = [x[1][67] if pd.notnull(x[1][67]) else x[1][65] if pd.notnull(x[1][65]) else x[1][66] for x in
                       case_1_ss.iterrows()]
    case_1_ss['SS'] = case_1_ss['SS'] * 7 / 5
    del case_1_ss['SS 1 Case 2'], case_1_ss['SS 2 Case 2'], case_1_ss['SS Case 1'], case_1_ss['#1'], case_1_ss['#2']
    return case_1_ss


gross_req_cases = Gross_Req_SS_Cases123(gross_req_turnover)


#### the most interestingest and the latest part
#### calculation of quantity using days in safety stock that we calculated before, in Grzesiek's instructions
#### this part called "Rolling Calculation of Safety Stock Qty"
def Rolling_Calculations_of_SS_Qty(gross_req_cases):
    df_test = gross_req_cases.iloc[:, 8:64]
    df_54_weeks = df_test.iloc[:, 0:54]
    df_weeks_transposed = df_54_weeks.transpose()
    df_weeks_summed = df_weeks_transposed.iloc[0:, :].cumsum()
    df_weeks_transposed_back = df_weeks_summed.transpose()
    df_rest_part = pd.merge(df_test[['TURNOVER', 'SS']], df_weeks_transposed_back, left_index=True, right_index=True)
    # # df_test = df_test.reset_index()
    df_test['Pos'] = (df_test['SS'] / 7).fillna(0)
    df_test['Pos_rounded'] = df_test['Pos'].astype(int)
    df_test['Pos_decimal'] = df_test['Pos'] - df_test['Pos_rounded']
    df_test['EndPos'] = ""
    for x in df_test.itertuples():
        #     print(x[1][58])
        pos = x[58]
        #     print(x[pos])
        val = x[pos + 1]
        #     print(val)
        #     print(x.Index)
        #     summa = df_test.iloc[x.Index, 0: pos].sum()
        df_test.at[x.Index, 'EndPos'] = val

    df_rest_part_with_positions = pd.merge(df_test[['Pos', 'Pos_rounded', 'Pos_decimal', 'EndPos']], df_rest_part,
                                           left_index=True, right_index=True)

    df_rest_part_with_positions['EndPosCumulative'] = ""

    for x in df_rest_part_with_positions.itertuples():
        #     print(x[1][58])
        pos = x[2]
        #     print(x[pos])
        val = x[pos + 8]
        #     print(val)
        #     print(x.Index)
        #     summa = df_test.iloc[x.Index, 0: pos].sum()
        df_rest_part_with_positions.at[x.Index, 'EndPosCumulative'] = val
    #     df_test.at[x.Index ,'Summa'] = summa

    df_rest_part_with_positions['SS Q'] = (df_rest_part_with_positions['EndPosCumulative'] -
                                           df_rest_part_with_positions['EndPos']) + df_rest_part_with_positions[
                                              'EndPos'] * df_rest_part_with_positions['Pos_decimal']
    del df_rest_part_with_positions['Pos'], df_rest_part_with_positions['Pos_rounded'], df_rest_part_with_positions[
        'Pos_decimal'], df_rest_part_with_positions['EndPos'], df_rest_part_with_positions['EndPosCumulative']
    gross_req_with_ss_qty = pd.merge(gross_req_cases, df_rest_part_with_positions['SS Q'], left_index=True,
                                     right_index=True)
    return gross_req_with_ss_qty


rolling_SS_Qty = Rolling_Calculations_of_SS_Qty(gross_req_cases)


# Rolling_Calculations_of_SS_Qty(gross_req_cases)[Rolling_Calculations_of_SS_Qty(gross_req_cases)['SS']>0]

def Calc_SS_V(rolling_SS_Qty):
    rolling_SS_Qty['SS V'] = rolling_SS_Qty['SS Q'] * rolling_SS_Qty['DM']
    return rolling_SS_Qty


####
gross_req_calc_ss_v = Calc_SS_V(rolling_SS_Qty)


def Turnover_Weight_Plant(gross_req_calc_ss_v):
    df_weight = gross_req_calc_ss_v.groupby(["Plant"])["TURNOVER"].sum().reset_index()
    df_weight["TURNOVER_Summ"] = df_weight["TURNOVER"].round(5)
    dfnew_weight_dio = pd.merge(gross_req_calc_ss_v[['Plant', 'TURNOVER']], df_weight[["Plant", "TURNOVER_Summ"]],
                                on=['Plant'], how='left')
    dfnew_weight_dio['W_Plant'] = [
        x[1][dfnew_weight_dio.columns.get_loc("TURNOVER")] / x[1][dfnew_weight_dio.columns.get_loc("TURNOVER_Summ")] if
        x[1][dfnew_weight_dio.columns.get_loc("TURNOVER_Summ")] > 0 else 0 for x in dfnew_weight_dio.iterrows()]
    del df_weight["TURNOVER_Summ"]
    return dfnew_weight_dio


df_w_plant = Turnover_Weight_Plant(gross_req_calc_ss_v)
df_merged_w_plant_req = pd.merge(gross_req_calc_ss_v, df_w_plant['W_Plant'], left_index=True, right_index=True)


def Qty(df_merged_w_plant_req):
    df_merged_w_plant_req_copy = df_merged_w_plant_req.copy()
    df_merged_w_plant_req_copy['Qty'] = df_merged_w_plant_req_copy.iloc[:, 8:62].sum(axis=1)
    return df_merged_w_plant_req_copy


df_merged_w_plant_req_copy = Qty(df_merged_w_plant_req)


def Marks12(df_merged_w_plant_req_copy):
    df_merged_w_plant_req_copy["Mark1"] = np.where(df_merged_w_plant_req_copy['Qty'] > 500, "Y", "N")
    df_merged_w_plant_req_copy["Mark2"] = np.where(df_merged_w_plant_req_copy['SS'] > 0, "Y", "N")
    return df_merged_w_plant_req_copy


df_marks = Marks12(df_merged_w_plant_req_copy)

df_merged_with_ss_v = pd.merge(df_merged_cat_abc_inv_t, df_marks[["Plant", "ANC", "SS V"]].drop_duplicates(),
                               on=["Plant", "ANC"], how='left')
df_merged_with_ss_v["SS V"] = df_merged_with_ss_v["SS V"].fillna(0)


# df_merged_with_ss_v[df_merged_with_ss_v['SS Coverage % [Factory]']>0]

def NettInventoryValue(df_merged_with_ss_v):
    df_merged_with_ss_v["Nett INV Value [Factory]"] = np.where(
        df_merged_with_ss_v['SS V'] > df_merged_with_ss_v["INV F V"], df_merged_with_ss_v["INV F V"],
        df_merged_with_ss_v['SS V'])
    df_merged_with_ss_v["SS Coverage % [Factory]"] = np.where(df_merged_with_ss_v['SS V'] > 0, (
            (df_merged_with_ss_v["Nett INV Value [Factory]"] / df_merged_with_ss_v['SS V']) * 100), 0)
    df_merged_with_ss_v["Nett INV Value [HUB]"] = np.where(df_merged_with_ss_v['SS V'] > df_merged_with_ss_v["INV H V"],
                                                           df_merged_with_ss_v["INV H V"], df_merged_with_ss_v['SS V'])
    df_merged_with_ss_v["SS Coverage % [HUB]"] = np.where(df_merged_with_ss_v['SS V'] > 0, (
            (df_merged_with_ss_v["Nett INV Value [HUB]"] / df_merged_with_ss_v['SS V']) * 100), 0)
    df_merged_with_ss_v["INV T V"] = df_merged_with_ss_v["INV F V"] + df_merged_with_ss_v["INV H V"]
    df_merged_with_ss_v["Nett INV Value [Factory+HUB]"] = np.where(
        df_merged_with_ss_v['SS V'] > df_merged_with_ss_v["INV T V"], df_merged_with_ss_v["INV T V"],
        df_merged_with_ss_v['SS V'])
    df_merged_with_ss_v["SS Coverage % [Factory+HUB]"] = np.where(df_merged_with_ss_v['SS V'] > 0, (
            (df_merged_with_ss_v["Nett INV Value [Factory+HUB]"] / df_merged_with_ss_v['SS V']) * 100), 0)
    return df_merged_with_ss_v


inv_prep_report = NettInventoryValue(df_merged_with_ss_v)
inv_prep_report = inv_prep_report.drop_duplicates()
df_marks = df_marks.drop_duplicates()
df_marks.to_excel(calc_reps + '\\GROSS_REQ_prepared_report.xlsx')
inv_prep_report.to_excel(calc_reps + "\\INVENTORY_prepared_report.xlsx")

end_time = time.monotonic()
print("Time of raport calculation is - ", timedelta(seconds=end_time - start_time))
