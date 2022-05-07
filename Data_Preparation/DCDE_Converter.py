import pandas as pd
import glob
import os

sourceXLSX = r"\\plws0125\share$\PLL-SP\Public\SUPPLY PLANNING\DATA EXPORT IDW VMI FSDS\DCDE"
destCSV = "C:\\InventoryBalanceData\\SAP IBP Tables\\"


def DCDE_convert_csv():
    listIDW = [f for f in glob.glob(sourceXLSX + "\*.xlsx")]
    FilesForDelete = glob.glob(os.path.join(destCSV, "*.csv"))
    for f in FilesForDelete:
        os.remove(f)
        print(f + " - was deleted")

    for i in range(0, len(listIDW)):
        # print(listIDW[i])
        pathname = listIDW[i]
        path_list = pathname.split(os.sep)
        print(path_list[-1][:-5])
        data_xls = pd.read_excel(listIDW[i], index_col=None, engine = "xlrd")
        data_xls.to_csv(destCSV + path_list[-1][:-5] + '.csv', encoding='utf-8', index=False)

# DCDE_convert_csv()