-- Databricks notebook source
-- DBTITLE 1,Scenarios: See Notebook: POC_1_create_json_scenarios for SCALA script to clear down or create these
-- see notebook POC_1_create_json_scenarios for Scala commands to reset/create json files for each sceanrio.
--
-- Scenario 1, Occurred: 2021-12-01 
-- SOH_1.json: {"HID":"H1","HDATE":"2021-12-01","HCUST":"C1","HCUSTNAME":"Charlie"}          -- NEW HEADER SO:H1
-- SOL_1.json: {"HID":"H1","LID":"L1","LDATE":"2021-12-01","PROD":"P1","AMT":13}             -- NEW LINE L1 for SO:H1
--             {"HID":"H1","LID":"L2","LDATE":"2021-12-01","PROD":"P2","AMT":17}             -- NEW LINE L2 for SO:H1
--
-- Scenario 2, Occurred: 2021-12-02,  
-- SOH_2.json: {"HID":"29H99","HDATE":"2021-12-02","HCUST":"C2","HCUSTNAME":"Bobby"}         -- NEW HEADER SO:29H99
-- SOL_2.json: {"HID":"H1","LID":"L3","LDATE":"2021-12-02","PROD":"P3","AMT":24}             -- NEW LINE for SO:H1
--             {"HID":"H1","LID":"L2","LDATE":"2021-12-02","PROD":"P2","AMT":25}             -- AMEND Qty for L2 on SO:H1
--
-- Scenario 3, Occurred: 2021-12-03, 
-- SOH_3.json:  "//"  -- ****BAD Data****- see Quality Constraints for this being skipped.   -- NO HEADERS
-- SOL_3.json:  {"HID":"29H99", "LID":"L4","LDATE":"2021-12-02","PROD":"P5","AMT":35}        -- NEW LATE Arriving LINE for SO:29H99
--              {"HID":"3H3333","LID":"L5","LDATE":"2021-12-03","PROD":"P6","AMT":37}        -- New Line for SO:3H3333 (whose HEADER never received - but build default HEADER anyway)
--
-- Scenario 4, 2021-12-04, Incoiming Order:29H99  Add 1 Line, Delete 1 Line ,   Order:3H3333 Delete Order Line received yesterday (without header)
-- SOH_4.json:  <****Not Created****>                                                        -- NO SOH headers file received
-- SOL_4.json: {"HID":"29H99", "LID":"L6","LDATE":"2021-12-04","PROD":"P6","AMT":10}         -- NEW LINE L6 for SO:29H99
--             {"HID":"29H99", "LID":"L4","LDATE":"2021-12-04","PROD":"P5","AMT":0 }         -- DELETE LINE L4 on SO:29H99  so will be flagged "IsLineDelete=True"
--             {"HID":"3H3333","LID":"L5","LDATE":"2021-12-04","PROD":"P6","AMT":0 }         -- DELETE LINE L5 on SO:3H3333 (Header was never actually Received)  SOL will be flagged "IsLineDelete=True"  (**)
--
-- (**) Suspect that we need to say if all lines in SOL Array are "IsLineDelete=True" then set SOH column "IsOrderDeleted=True" also?? at moment it is not changed- bnut Scenario 5 doe hard delete
--
-- Scenario 5, 2021-12-05, Incoiming Order:29H99  Add 1 Line, Delete 1 Line ,   Order:3H3333 Delete Order Line received yesterday (without header)
-- SOH_5.json: {"HID":"3H3333","HDATE":"2021-12-05","HCUST":"","HCUSTNAME":"Alfred"}         -- DELETE  Header 3H3333  -- but does this also mean ZAP lines ... hmmm  SOH will be flagged "IsOrderDelete=True"  
-- SOL_5.json: <****Not Created****>                                                          -- NO SOL headers file received 


-- COMMAND ----------

-- DBTITLE 1,List all files in the ingestion folders
select input_file_name() As FileName, TO_JSON(STRUCT(*)) AS JsonString 
from json.`/mnt/poc_dlt_scenarios/SO*`
ORDER BY REGEXP_REPLACE(input_file_name(),'^.+/SO[HL]_([^.]+).json','\$1'), FileName;

-- COMMAND ----------

-- DBTITLE 1,Query final pipeline (consumable) SILVER table of Order Status in SOH+SOL Array format containing only the LATEST complete view of a SO.
select * from DLT_DB_POC_2.SOH_with_SOL_Array_latest

-- COMMAND ----------

-- DBTITLE 1,Query all WATERMARKed versions of SOH+ARRAY(SOL) from which SOH_with_SOL_Array_latest is generated.
select * from DLT_DB_POC_2.S2_inc_SOHL_array_unduped  order by OrdNumDiv1K, OrdNum, Occurred desc, Received desc , AppendWaterMark desc

-- COMMAND ----------

-- DBTITLE 1,Current State of Ingested RAW Sales Order Header Data (BRONZE)
select * EXCEPT (TriggerSourceEventId)  from DLT_DB_POC_2.B0_inc_SOH_raw_json

-- COMMAND ----------

-- DBTITLE 1,Current State of Ingested RAW Sales Order LINE Data (BRONZE)
select * EXCEPT (TriggerSourceEventId) from DLT_DB_POC_2.B0_inc_SOL_raw_json


-- COMMAND ----------

-- DBTITLE 1,Current State of Ingested Sales Order Header Data (BRONZE) with some basic data value mapping
select * from DLT_DB_POC_2.B1_inc_SOH_ingest

-- COMMAND ----------

-- DBTITLE 1,Current State of Ingested Sales Order LINE Data (BRONZE) with some basic data value mapping
select * from DLT_DB_POC_2.B1_inc_SOL_ingest

-- COMMAND ----------

-- DBTITLE 1,What Orders (either SOH or SOL) were processed per Pipeline Run (watermark)
select distinct * from DLT_DB_POC_2.B2_inc_SOHL_keys_this_run

-- COMMAND ----------

-- DBTITLE 1,Which SOH (based on OCCURRED TimeStamp) we need to USE (from this run or prior ingestions)
select * from DLT_DB_POC_2.S0_inc_SOH_latest_unduped_for_filter

-- COMMAND ----------

-- DBTITLE 1,SOH rows for joining purposes based on the now know OCCURRED timestamps in previous logical [sub] query
select * from DLT_DB_POC_2.S1_inc_SOH_latest_unduped

-- COMMAND ----------

-- DBTITLE 1,SOL's (based on Line OCCURRED TimeStamp) we need to USE (from this run or prior ingestions)
select * from DLT_DB_POC_2.S0_inc_SOL_latest_unduped_for_filter

-- COMMAND ----------

-- DBTITLE 1,[WITH Line Detail] SOL rows for joining purposes based on the now know OCCURRED timestamps in previous logical [sub] query
select * from DLT_DB_POC_2.S1_inc_SOL_latest_unduped_subquery

-- COMMAND ----------

-- DBTITLE 1,SOL rows for joining purposes based on the previous logical SOL [sub] queries.
select * from DLT_DB_POC_2.S1_inc_SOL_latest_unduped

-- COMMAND ----------

-- DBTITLE 1,Orders detected in this run, BUILD a LATEST view and append to this table for subsequent materialisation to table SOH_with_SOL_Array_latest
select * from DLT_DB_POC_2.S2_inc_SOHL_array_unduped