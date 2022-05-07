import pandas as pd
import glob
import os

sourceXLSX = r"\\plws0125\share$\PLL-SP\Public\SUPPLY PLANNING\DATA EXPORT IDW VMI FSDS\Arrivals Report"
destCSV = "C:\\InventoryBalanceData\\SAP IBP Tables\\"


def conv_arrivals_report():
    listFiles = [f for f in glob.glob(sourceXLSX + "\*.xlsx")]
    for i in range(0, len(listFiles)):
        # print(listIDW[i])
        pathname = listFiles[0]
        path_list = pathname.split(os.sep)
        print(path_list[-1])
        data_xls = pd.read_excel(listFiles[i], engine = "xlrd")
        const_columns_firstpart = data_xls.iloc[:, 0:4]
        changing_columns_secondpart = data_xls.iloc[:, 4:18]
        last_column_ttl = data_xls.iloc[:, [18]]
        zeroweek = data_xls.iloc[:, [5]].keys()[0]
        i = 0
        for wk in changing_columns_secondpart:
            # print(wk)
            changing_columns_secondpart.rename(columns={wk: 'wk' + str(i)}, inplace=True)
            i += 1

        # print(zeroweek)
        combined_dataframes = [const_columns_firstpart, changing_columns_secondpart, last_column_ttl]
        combined_result = pd.concat(combined_dataframes, axis=1)
        combined_result.rename(columns={'#': '#1'}, inplace=True)
        print(combined_result)
        combined_result.to_csv(destCSV + 'Arrivals_report.csv', encoding='utf-8', index=False)

