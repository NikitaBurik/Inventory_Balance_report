import glob

import pandas


folderpath = r"\\plws0125\share$\PLL-SP\Public\SUPPLY PLANNING\Control Tower 2.0\PO_UPDATE"

listFiles = [f for f in glob.glob(folderpath + "\\v2_PO_UPDATE_matka*.xlsm")]

print(listFiles)