import glob
import time




def DataChecker():
    source_local_data = r"C:\\InventoryBalanceData"
    source_local_sap = r"C:\InventoryBalanceData\SAP IBP Tables"
    source_local_inv = r"C:\InventoryBalanceData\INV_merged_files"

    list_idw = [f for f in glob.glob(source_local_data + "\*.csv")]
    list_sap = [f for f in glob.glob(source_local_sap + "\*.csv")]
    list_inv = [f for f in glob.glob(source_local_inv + "\*.csv")]

    time.sleep(3)
    if len(list_idw) == 8 and len(list_sap) == 7 and len(list_inv) == 3:
        print("Every file has been updated")
    else:
        print("Something is not completed")
        time.sleep(2)
        DataChecker()
