import glob
import os
import time

import pyodbc

import pandas as pd



def run_procedure():
    export_db = r"C:\InventoryBalanceData\Prepared_Reps"
    # Created a pyodbc connection to db electrolux
    conn = pyodbc.connect('DRIVER={SQL Server};'
                          'SERVER=DT-SupplyPlanning-EU.biz.electrolux.com;Trusted_Connection=no;'
                          'Database=SupplyPlanning_Test;'
                          'UID=supply_admin;'
                          'PWD={i)Z\#6&;~fwJNZ7e}')

    cursor = conn.cursor()

    sql_query = pd.read_sql_query("""
    WITH UNITED_MAIN_TABLE
    AS (
        -- first table idw balance
        SELECT [#1]
          ,'IDW' as [Source_Data]
          ,[#2]
          ,[Plant]
          ,[ANC]
          ,[Description]
          ,[Bal_Cat]
          ,[BC]
          ,[PMS_Supplier]
          ,[Local_Supp] as [Supplier_ID]
          ,[Suppname] as [Supplier_Name]
          ,[IDCO]
          ,[STK_EUR]
          ,[INV_date] as [Qty]
        FROM [SupplyPlanning_Test].[dbo].[temp_data_IDW_balance] as [IDW_Bal]
        UNION ALL
        -- second table vmi rothenburg
        SELECT [VMI].[#1]
          ,'VMI' as [Source_Data]
          ,NULL as [#2]
          ,'DGT' as [Plant]
          ,[Part_No] as [ANC]
          ,NULL as [Description]
          ,'VMI Consignment' as [Bal_Cat]
          ,NULL as [BC]
          ,NULL as [PMS_Supplier]
          ,[Supplier_ID]
          ,NULL as [Supplier_Name]
          ,NULL as [IDCO]
          ,NULL as [STK_EUR]
          ,[Sum_of_VMI_Store] as [Qty]
        FROM [SupplyPlanning_Test].[dbo].[temp_data_VMI_Toyota_Rothenburg] as [VMI]
        UNION ALL
        -- third table git arrivals
        SELECT [GIT].[#1]
          ,'GIT' as [Source_Data]
          ,NULL as [#2]
          ,[GIT].[Plant]
          ,[GIT].[ANC]
          ,NULL as [Description]
          ,'GIT' as [Bal_Cat]
          ,NULL as [BC]
          ,NULL as [PMS_Supplier]
          ,[GIT].[SupplierID] as [Supplier_ID]
          ,NULL as [Supplier_Name]
          ,NULL as [IDCO]
          ,NULL as [STK_EUR]
          ,[GIT].[TTL] as [Qty]
    FROM [SupplyPlanning_Test].[dbo].[temp_data_Arrivals_Report] as [GIT]
    ),
    FILLED_UNITED_TABLE AS
    ( -- this table should be filled new and actual data from other tables
      SELECT
       [UNITED_MAIN_TABLE].[#1]
      ,[UNITED_MAIN_TABLE].[Source_Data]
      ,[UNITED_MAIN_TABLE].[#2]
      ,[UNITED_MAIN_TABLE].[Plant]
      ,[UNITED_MAIN_TABLE].[ANC]
      ,[UNITED_MAIN_TABLE].[PMS_Supplier]
      ,CASE WHEN [IDW_Joins].[Description] IS NOT NULL THEN [IDW_Joins].[Description]
            WHEN [IDW_Costs].[ANC_Description] IS NOT NULL THEN [IDW_Costs].[ANC_Description]
            ELSE '-'
            END AS [Description]
      ,[UNITED_MAIN_TABLE].[Bal_Cat]
      ,CASE WHEN [IDW_Joins].[BC] IS NOT NULL THEN [IDW_Joins].[BC]
            WHEN [IDW_Costs].[BC] IS NOT NULL THEN [IDW_Costs].[BC]
            ELSE 10
            END AS [BC]
      ,CASE WHEN [SAP_Code].[Supplier_Code] IS NOT NULL THEN [SAP_Code].[Supplier_Code]
            WHEN [IDW_Joins].[Local_Supp] IS NOT NULL THEN [IDW_Joins].[Local_Supp]
            WHEN [VMI_Joins].[Supplier_ID] IS NOT NULL THEN [VMI_Joins].[Supplier_ID]
            ELSE '-'
            END AS [Supplier_ID]
      ,CASE WHEN [SAP_Name].[Supplier_Description] IS NOT NULL THEN [SAP_Name].[Supplier_Description]
            WHEN [IDW_Joins].[Suppname] IS NOT NULL THEN [IDW_Joins].[Suppname]
            ELSE '-'
            END AS [Supplier_Name]
      ,CASE WHEN [IDW_Joins].[IDCO] IS NOT NULL THEN [IDW_Joins].[IDCO]
            WHEN [IDW_Costs].[IDCO] IS NOT NULL THEN [IDW_Costs].[IDCO]
            WHEN [SAP_IDCO].[IDCO_Description] IS NOT NULL THEN [SAP_IDCO].[IDCO_Description]
            ELSE '-'
            END AS [IDCO]
      ,CASE WHEN [IDW_Costs].[Conv_STK3_Calc] IS NOT NULL THEN [IDW_Costs].[Conv_STK3_Calc]
            WHEN [DCDE].[STK3_EUR] IS NOT NULL THEN [DCDE].[STK3_EUR]
            WHEN [SAP_Cost].[Standard_Cost] IS NOT NULL THEN [SAP_Cost].[Standard_Cost]
            WHEN [IDW_Joins].[STK_EUR] IS NOT NULL THEN [IDW_Joins].[STK_EUR]
            ELSE 0
            END AS [STK_EUR]
      ,[UNITED_MAIN_TABLE].[Qty]
       FROM [UNITED_MAIN_TABLE]
    -- SUPPLIER ID TARGET
    -- JOINING WITH SAP_SUPPLIER_CODE FOR VLOOKUP SAP SUPPLIERS_ID (SUPPLIER_CODE)
    LEFT JOIN [SupplyPlanning_Test].[dbo].[temp_data_SAP_Supplier_Code] as [SAP_Code]
    ON [UNITED_MAIN_TABLE].[#1] = [SAP_Code].[#]
    ----------------------------------------------------------------------------------------------
    LEFT JOIN [SupplyPlanning_Test].[dbo].[temp_data_IDW_balance] as [IDW_Joins]
    ON [UNITED_MAIN_TABLE].[#1] = [IDW_Joins].[#1]
    ----------------------------------------------------------------------------------------------
    LEFT JOIN [SupplyPlanning_Test].[dbo].[temp_data_VMI_Toyota_Rothenburg] as [VMI_Joins]
    ON [UNITED_MAIN_TABLE].[#1] = [VMI_Joins].[#1]
    ----------------------------------------------------------------------------------------------
    LEFT JOIN [SupplyPlanning_Test].[dbo].[temp_data_Arrivals_Report] as [GIT_Joins]
    ON [UNITED_MAIN_TABLE].[#1] = [GIT_Joins].[#1]
    ----------------------------------------------------------------------------------------------
    LEFT JOIN [SupplyPlanning_Test].[dbo].[temp_data_SAP_Supplier_Description] as [SAP_Name]
    ON [UNITED_MAIN_TABLE].[#1] = [SAP_Name].[#]
    --------------- JOINS FOR 'DESCRIPTION' AND 'BC' AND 'IDCO'-----------------------------------
    LEFT JOIN [SupplyPlanning_Test].[dbo].[temp_data_IDW_cost] as [IDW_Costs]
    ON [UNITED_MAIN_TABLE].[#1] = [IDW_Costs].[#1]
    LEFT JOIN [SupplyPlanning_Test].[dbo].[temp_data_SAP_Supplier_IDCO] as [SAP_IDCO]
    ON [UNITED_MAIN_TABLE].[ANC] = [SAP_IDCO].[Product_ID]
    LEFT JOIN  [SupplyPlanning_Test].[dbo].[temp_data_DCDE_support_table] as [DCDE]
    ON [UNITED_MAIN_TABLE].[#1] = [DCDE].[#]
    LEFT JOIN [SupplyPlanning_Test].[dbo].[temp_data_SAP_Cost] as [SAP_Cost]
    ON [UNITED_MAIN_TABLE].[#1] = [SAP_Cost].[#]
    ),
    FIXED_TABLE_MAIN_INVENTORY AS
    (
      SELECT
      [FILLED_UNITED_TABLE].[#1],
      [FILLED_UNITED_TABLE].[Source_Data],
      [FILLED_UNITED_TABLE].[#2],
      [Indop_Table].[Indop] as [Indop],
      [Fixed_Category].[Cat] as [Cat],
      [CMDT_Table].[CMDT] as [CMDT],
      [FILLED_UNITED_TABLE].[Plant] as [Plant],
      [FILLED_UNITED_TABLE].[ANC] as [ANC],
      [FILLED_UNITED_TABLE].[Description] as [Description],
      [FILLED_UNITED_TABLE].[Bal_Cat] as [Bal_Cat],
      [FILLED_UNITED_TABLE].[BC] as [BC],
      [FILLED_UNITED_TABLE].[PMS_Supplier],
      NULL as [Country_Code],
      NULL as [Country],
      NULL as [Region],
      [FILLED_UNITED_TABLE].[Supplier_ID] as [Supplier_ID],
      [FILLED_UNITED_TABLE].[Supplier_Name] as [Supplier_Name],
      NULL as [PQMGR],
      [FILLED_UNITED_TABLE].[IDCO] as [IDCO],
      [FILLED_UNITED_TABLE].[STK_EUR] as [STK_EUR],
      [FILLED_UNITED_TABLE].[Qty] as [Qty],
      [FILLED_UNITED_TABLE].[STK_EUR]*[FILLED_UNITED_TABLE].[Qty] AS [Value]
      FROM [FILLED_UNITED_TABLE]
      -- joining with [Cat_Fixed_table] for show certain category in balance category
      LEFT JOIN [SupplyPlanning_Test].[dbo].[Cat_Fixed_table] as [Fixed_Category]
      ON [FILLED_UNITED_TABLE].[Bal_Cat] = [Fixed_Category].[Bal Cat]
      -- joining with [Indop_Fixed_table] for show certaing Indop
      LEFT JOIN [SupplyPlanning_Test].[dbo].[Indop_Fixed_table] as [Indop_Table]
      ON [FILLED_UNITED_TABLE].[Plant] = [Indop_Table].[Plant]
      -- joining with [CMDT_Fixed_table] for show certaing CMDT
      LEFT JOIN [SupplyPlanning_Test].[dbo].[CMDT_Fixed_table] as [CMDT_Table]
      ON [FILLED_UNITED_TABLE].[IDCO] = [CMDT_Table].[IDCO]
    ),
    FIXED_DUPLICATES_MAIN_INVENTORY AS
    (
    SELECT DISTINCT
    [#1],
    [Source_Data],
    [#2],
    [Indop],
    [Cat],
    [CMDT],
    [Plant],
    [ANC],
    [Description],
    [Bal_Cat],
    [BC],
    [PMS_Supplier],
    [Country_Code],
    [Country],
    [Region],
    [Supplier_ID],
    [Supplier_Name],
    [PQMGR],
    FIXED_TABLE_MAIN_INVENTORY.[IDCO] as [IDCO],
    [STK_EUR],
    [Qty],
    [Value]
    FROM FIXED_TABLE_MAIN_INVENTORY
    ),
    FILL_EMPTY_SUPP_CODE_NAME AS
    (
    SELECT
    [FIXED_DUPLICATES_MAIN_INVENTORY].[#1],
    [Source_Data],
    [#2],
    [Indop],
    [Cat],
    [CMDT],
    [FIXED_DUPLICATES_MAIN_INVENTORY].[Plant],
    [FIXED_DUPLICATES_MAIN_INVENTORY].[ANC],
    [Description],
    [Bal_Cat],
    [BC],
    [PMS_Supplier],
    [Country_Code],
    [Country],
    [Region],
    -- [FIXED_DUPLICATES_MAIN_INVENTORY].[Supplier_ID],
    CASE WHEN [FIXED_DUPLICATES_MAIN_INVENTORY].[Supplier_ID] = '-' AND [Source_Data] = 'GIT' THEN [GIT_Joins].[SupplierID]
         ELSE [FIXED_DUPLICATES_MAIN_INVENTORY].[Supplier_ID]
    END AS [Supplier_ID],
    [Supplier_Name],
    [PQMGR],
    [IDCO],
    [STK_EUR],
    [Qty],
    [Value]
    FROM FIXED_DUPLICATES_MAIN_INVENTORY
    ----------------------------------------------------------------------------------------------
    LEFT JOIN [SupplyPlanning_Test].[dbo].[temp_data_VMI_Toyota_Rothenburg] as [VMI_Joins]
    ON [FIXED_DUPLICATES_MAIN_INVENTORY].[#1] = [VMI_Joins].[#1]
    ----------------------------------------------------------------------------------------------
    LEFT JOIN [SupplyPlanning_Test].[dbo].[temp_data_Arrivals_Report] as [GIT_Joins]
    ON [FIXED_DUPLICATES_MAIN_INVENTORY].[#1] = [GIT_Joins].[#1]
    ----------------------------------------------------------------------------------------------
    ),
    FILL_KEYSTWO_MAIN_TABLE AS
    (
    SELECT
    [#1],
    [Source_Data],
    CASE WHEN [Source_Data] = 'GIT' AND [FILL_EMPTY_SUPP_CODE_NAME].[#2] IS NULL THEN [FILL_EMPTY_SUPP_CODE_NAME].[Supplier_ID] + [FILL_EMPTY_SUPP_CODE_NAME].[Plant]
         ELSE [FILL_EMPTY_SUPP_CODE_NAME].[#2]
         END AS [#2],
    [Indop],
    [Cat],
    [CMDT],
    [Plant],
    [ANC],
    [Description],
    [Bal_Cat],
    [BC],
    [PMS_Supplier],
    [Country_Code],
    [Country],
    [Region],
    [Supplier_ID],
    [Supplier_Name],
    -- CASE WHEN [FILL_EMPTY_SUPP_CODE_NAME].[Supplier_Name] = '-' AND [Source_Data] = 'GIT' THEN [SAP_Suppliers].[Supplier_Description]
    --      ELSE [FILL_EMPTY_SUPP_CODE_NAME].[Supplier_Name]
    -- END AS [Supplier_Name],
    [PQMGR],
    [IDCO],
    [STK_EUR],
    [Qty],
    [Value]
    FROM FILL_EMPTY_SUPP_CODE_NAME
    ),
    
    FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE AS
    (
    SELECT
    [#1],
    [Source_Data],
    [#2],
    [Indop],
    [Cat],
    [CMDT],
    [Plant],
    [ANC],
    [Description],
    [Bal_Cat],
    [BC],
    [PMS_Supplier],
    [Country_Code],
    [Country],
    [Region],
    [Supplier_ID],
    CASE WHEN  [FILL_KEYSTWO_MAIN_TABLE].[Supplier_Name] = '-' AND [Source_Data] = 'GIT' THEN [SAP_Suppliers].[Supplier_Description]
         ELSE [FILL_KEYSTWO_MAIN_TABLE].[Supplier_Name]
    END AS [Supplier_Name],
    -- [Supplier_Name],
    [FILL_KEYSTWO_MAIN_TABLE].[PQMGR],
    [FILL_KEYSTWO_MAIN_TABLE].[IDCO],
    [FILL_KEYSTWO_MAIN_TABLE].[STK_EUR],
    [FILL_KEYSTWO_MAIN_TABLE].[Qty],
    [FILL_KEYSTWO_MAIN_TABLE].[Value]
    FROM FILL_KEYSTWO_MAIN_TABLE
    LEFT JOIN (SELECT DISTINCT
          [Supplier_Code]
          ,[Supplier_Description]
    FROM [SupplyPlanning_Test].[dbo].[temp_data_SAP_Suppliers]) as [SAP_Suppliers]
    ON [FILL_KEYSTWO_MAIN_TABLE].[Supplier_ID] = [SAP_Suppliers].[Supplier_Code]
    AND [FILL_KEYSTWO_MAIN_TABLE].[Source_Data] = 'GIT'
    AND [FILL_KEYSTWO_MAIN_TABLE].[Supplier_Name] = '-'
    ),
    FILL_EMPTY_VMI_SUPP_DESCR_MAIN_TABLE AS
    (
    SELECT
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[#1],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[Source_Data],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[#2],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[Indop],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[Cat],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[CMDT],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[Plant],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[ANC],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[Description],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[Bal_Cat],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[BC],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[PMS_Supplier],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[Country_Code],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[Country],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[Region],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[Supplier_ID],
    CASE WHEN  [Source_Data] = 'VMI' AND [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[Supplier_Name] = '-' THEN [SAP_Suppliers].[Supplier_Description]
         ELSE [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[Supplier_Name]
    END AS [Supplier_Name],
    -- [Supplier_Name],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[PQMGR],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[IDCO],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[STK_EUR],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[Qty],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[Value]
    FROM FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE
    LEFT JOIN (SELECT DISTINCT
          [Supplier_Code]
          ,[Supplier_Description]
    FROM [SupplyPlanning_Test].[dbo].[temp_data_SAP_Suppliers]) as [SAP_Suppliers]
    ON [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[Supplier_ID] = [SAP_Suppliers].[Supplier_Code]
    AND [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[Source_Data] = 'VMI'
    AND [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[Supplier_Name] = '-'
    ),
    FILL_EMPTY_SUPP_DESCR_MAIN_ZS_FORA_TABLE AS
    (
    SELECT
    [#1],
    [Source_Data],
    [#2],
    [Indop],
    [Cat],
    [CMDT],
    [Plant],
    [ANC],
    [Description],
    [Bal_Cat],
    [BC],
    [PMS_Supplier],
    [Country_Code],
    [Country],
    [Region],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[Supplier_ID] as [Supplier_ID],
    CASE WHEN [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[Supplier_Name] IS NULL AND [Source_Data] = 'GIT' AND [Plant] = 'ZS' THEN [ZS_FORA].[Supplier_Name]
         ELSE [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[Supplier_Name]
    END AS [Supplier_Name],
    -- [Supplier_Name],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[PQMGR] as [PQMGR],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[IDCO] as [IDCO],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[STK_EUR] as [STK_EUR],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[Qty] as [Qty],
    [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[Value] as [Value]
    FROM FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE
    LEFT JOIN [SupplyPlanning_Test].[dbo].[temp_data_FORA_ZS] as [ZS_FORA]
    ON [FILL_EMPTY_GIT_SUPP_DESCR_MAIN_TABLE].[Supplier_ID] = [ZS_FORA].[Supplier_ID]
    ),
    
    REPAIRED_MISSED_DATA_IN_SUPP_NAME AS
    (
    SELECT
    [#1],
    [Source_Data],
    [#2],
    [Indop],
    [Cat],
    [CMDT],
    [Plant],
    [ANC],
    [Description],
    [Bal_Cat],
    [BC],
    [PMS_Supplier],
    [Country_Code],
    [Country],
    [Region],
    [Supplier_ID],
    CASE WHEN [FILL_EMPTY_SUPP_DESCR_MAIN_ZS_FORA_TABLE].[Supplier_ID] IS NOT NULL  AND [Supplier_Name] ='-' THEN [SAP_Suppliers].[Supplier_Description]
         ELSE [FILL_EMPTY_SUPP_DESCR_MAIN_ZS_FORA_TABLE].[Supplier_Name]
          END AS [Supplier_Name],
    [PQMGR],
    [IDCO],
    [STK_EUR],
    [Qty],
    CASE WHEN [Value]<0 THEN '0'
    ELSE [Value]
    END AS [Value]
    FROM FILL_EMPTY_SUPP_DESCR_MAIN_ZS_FORA_TABLE
    LEFT JOIN (SELECT DISTINCT
          [Supplier_Code]
          ,[Supplier_Description]
    FROM [SupplyPlanning_Test].[dbo].[temp_data_SAP_Suppliers]) as [SAP_Suppliers]
    ON [SAP_Suppliers].[Supplier_Code] = [FILL_EMPTY_SUPP_DESCR_MAIN_ZS_FORA_TABLE].[Supplier_ID]
    AND [FILL_EMPTY_SUPP_DESCR_MAIN_ZS_FORA_TABLE].[Supplier_ID] IS NOT NULL
    AND [FILL_EMPTY_SUPP_DESCR_MAIN_ZS_FORA_TABLE].[Supplier_Name] ='-'
    ),
    ARRIVAL_QUANTITY_JOIN_PART AS
    (
    SELECT
    [REPAIRED_MISSED_DATA_IN_SUPP_NAME].[#1],
    [Source_Data],
    [#2],
    [Indop],
    [Cat],
    [CMDT],
    [REPAIRED_MISSED_DATA_IN_SUPP_NAME].[Plant],
    [REPAIRED_MISSED_DATA_IN_SUPP_NAME].[ANC],
    [Description],
    [Bal_Cat],
    [BC],
    [PMS_Supplier],
    [Country_Code],
    [Country],
    [Region],
    [Supplier_ID],
    [Supplier_Name],
    [PQMGR],
    [IDCO],
    [STK_EUR],
    [Qty],
    [Value],
    [Arrival_report].[wk0] AS [wk0],
    [Arrival_report].[wk1] AS [wk1],
    [Arrival_report].[wk2] AS [wk2],
    [Arrival_report].[wk3] AS [wk3],
    [Arrival_report].[wk4] AS [wk4],
    [Arrival_report].[wk5] AS [wk5],
    [Arrival_report].[wk6] AS [wk6],
    [Arrival_report].[wk7] AS [wk7],
    [Arrival_report].[wk8] AS [wk8],
    [Arrival_report].[wk9] AS [wk9],
    [Arrival_report].[wk10] AS [wk10],
    [Arrival_report].[wk11] AS [wk11],
    [Arrival_report].[wk12] AS [wk12],
    [Arrival_report].[wk0]+
    [Arrival_report].[wk1]+
    [Arrival_report].[wk2]+
    [Arrival_report].[wk3]+
    [Arrival_report].[wk4]+
    [Arrival_report].[wk5]+
    [Arrival_report].[wk6]+
    [Arrival_report].[wk7]+
    [Arrival_report].[wk8]+
    [Arrival_report].[wk9]+
    [Arrival_report].[wk10]+
    [Arrival_report].[wk11]+
    [Arrival_report].[wk12] AS [TTL_Arr_Qty]
    FROM REPAIRED_MISSED_DATA_IN_SUPP_NAME
    LEFT JOIN [SupplyPlanning_Test].[dbo].[temp_data_Arrivals_Report] as [Arrival_report]
    ON [REPAIRED_MISSED_DATA_IN_SUPP_NAME].[#1] = [Arrival_report].[#1]
    ),
    ARRIVAL_VALUE_JOIN_PART AS
    (
    SELECT
    [ARRIVAL_QUANTITY_JOIN_PART].[#1],
    [Source_Data],
    [#2],
    [Indop],
    [Cat],
    ISNULL([CMDT], '-') AS [CMDT],
    [ARRIVAL_QUANTITY_JOIN_PART].[Plant],
    [ARRIVAL_QUANTITY_JOIN_PART].[ANC],
    [Description],
    [Bal_Cat],
    [BC],
    [PMS_Supplier],
    [Country_Code],
    [Country],
    [Region],
    [Supplier_ID],
    CASE WHEN [ARRIVAL_QUANTITY_JOIN_PART].[Supplier_Name] IS NULL THEN [ARRIVAL_QUANTITY_JOIN_PART].[PMS_Supplier]
    ELSE [ARRIVAL_QUANTITY_JOIN_PART].[Supplier_Name]
    END AS [Supplier_Name],
    [PQMGR],
    [IDCO],
    [STK_EUR],
    [Qty],
    [Value],
    [ARRIVAL_QUANTITY_JOIN_PART].[wk0],
    [ARRIVAL_QUANTITY_JOIN_PART].[wk1],
    [ARRIVAL_QUANTITY_JOIN_PART].[wk2],
    [ARRIVAL_QUANTITY_JOIN_PART].[wk3],
    [ARRIVAL_QUANTITY_JOIN_PART].[wk4],
    [ARRIVAL_QUANTITY_JOIN_PART].[wk5],
    [ARRIVAL_QUANTITY_JOIN_PART].[wk6],
    [ARRIVAL_QUANTITY_JOIN_PART].[wk7],
    [ARRIVAL_QUANTITY_JOIN_PART].[wk8],
    [ARRIVAL_QUANTITY_JOIN_PART].[wk9],
    [ARRIVAL_QUANTITY_JOIN_PART].[wk10],
    [ARRIVAL_QUANTITY_JOIN_PART].[wk11],
    [ARRIVAL_QUANTITY_JOIN_PART].[wk12],
    [ARRIVAL_QUANTITY_JOIN_PART].[TTL_Arr_Qty],
    [STK_EUR]*[wk0] AS [wkmult0],
    [STK_EUR]*[wk1] AS [wkmult1],
    [STK_EUR]*[wk2] AS [wkmult2],
    [STK_EUR]*[wk3] AS [wkmult3],
    [STK_EUR]*[wk4] AS [wkmult4],
    [STK_EUR]*[wk5] AS [wkmult5],
    [STK_EUR]*[wk6] AS [wkmult6],
    [STK_EUR]*[wk7] AS [wkmult7],
    [STK_EUR]*[wk8] AS [wkmult8],
    [STK_EUR]*[wk9] AS [wkmult9],
    [STK_EUR]*[wk10] AS [wkmult10],
    [STK_EUR]*[wk11] AS [wkmult11],
    [STK_EUR]*[wk12] AS [wkmult12],
    [STK_EUR]*[wk0]+
    [STK_EUR]*[wk1]+
    [STK_EUR]*[wk2]+
    [STK_EUR]*[wk3]+
    [STK_EUR]*[wk4]+
    [STK_EUR]*[wk5]+
    [STK_EUR]*[wk6]+
    [STK_EUR]*[wk7]+
    [STK_EUR]*[wk8]+
    [STK_EUR]*[wk9]+
    [STK_EUR]*[wk10]+
    [STK_EUR]*[wk11]+
    [STK_EUR]*[wk12] AS [TTL_Arr_Value]
    FROM ARRIVAL_QUANTITY_JOIN_PART
    )
    SELECT * from ARRIVAL_VALUE_JOIN_PART
    """, conn)
    print(sql_query)
    conn.commit()
    exportDF = sql_query.to_csv(export_db +  '\\InvBalance_'+ time.strftime('%Y%m%d') +'.csv', encoding='utf-8', index=False)


