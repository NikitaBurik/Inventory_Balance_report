import Data_Preparation.IDW_VMI_convert_csv_clean_up as IDW_VMI
import Data_Preparation.Gross_Req_Data_Cleanse as IDW_GROSS
import Data_Preparation.DCDE_Converter as DCDE
import Data_Preparation.SAP_IBP_DCDE_convert_csv as SAP_IBP
import Data_Preparation.Arrivals_Report as Arrivals
import Data_Preparation.Data_Checker as CheckFiles
import Data_Loading_to_Database.Load_Data_to_DB as LoadDataBase
import Data_Consolidating_Clean_Join_SQL.Export_Inventory_Balance_Main_from_DB as INV_DB
import Data_Consolidating_Clean_Join_SQL.Export_Gross_Forecast_from_DB as FORECAST_DB


try:
    # IDW_VMI.convert_csv_idw_vmi()
    # IDW_VMI.merge_in_one_idw_files()
    # IDW_GROSS.gross_forecast_convert_clean_up()
    # SAP_IBP.sap_ibp_convert_csv()
    # Arrivals.conv_arrivals_report()
    # CheckFiles.DataChecker()
    # print("Loading data to database was started")
    # LoadDataBase.load_data_to_db()
    # print("Data was successfully loaded to database")
    print("Run procedures")
    INV_DB.run_procedure()
    FORECAST_DB.run_procedure()
    print("Procedures completed - check folder")
except:
    print("Something doesn't work")
