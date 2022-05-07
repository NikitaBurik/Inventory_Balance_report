import glob
import os

import pandas as pd

sourceDCDE = r"\\plws0125\share$\PLL-SP\Public\SUPPLY PLANNING\DATA EXPORT IDW VMI FSDS\DCDE"
sourceXLSX = r"\\plws0125\share$\PLL-SP\Public\SUPPLY PLANNING\DATA EXPORT IDW VMI FSDS\SAP IBP"
destCSV = "C:\\InventoryBalanceData\\SAP IBP Tables\\"


def sap_ibp_convert_csv():
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

    ### especialy were added next lines for convert DCDE report into SAP csv folder
    srcDCDE = glob.glob(os.path.join(sourceDCDE, "DCDE3_ALL*"))
    pathname = srcDCDE[-1]
    path_list = pathname.split(os.sep)
    print(path_list[-1][:-5])
    data_xls = pd.read_excel(srcDCDE[-1], index_col=None, engine = "xlrd")
    data_xls.to_csv(destCSV + path_list[-1][:-5] + '.csv', encoding='utf-8', index=False)

