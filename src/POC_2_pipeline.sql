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
-- SOL_5.json: <****Not Created****>         


-- COMMAND ----------

-- DBTITLE 1,Get a Unique ever Increasing value (like a UUID() but needs to be sortable) to WaterMark all Data for THIS run so we can use it to construct and join inside/outside this pipeline
CREATE TEMPORARY LIVE TABLE PipeLineVars AS  SELECT CAST(unix_micros(current_timestamp()) AS BIGINT) AS AppendWaterMark;

-- COMMAND ----------

-- DBTITLE 1,Bring in any NEW SOH RAW JSON data for consideration
CREATE INCREMENTAL LIVE TABLE B0_inc_SOH_raw_json (
  CONSTRAINT valid_SOH_received EXPECT (HID IS NOT NULL and HDATE IS NOT NULL AND HCUST IS NOT NULL) ON VIOLATION DROP ROW
)
PARTITIONED BY (OrdNumDiv1K,AppendWaterMark)
COMMENT "Bronze Stage 0 Base Raw Incoming bronze DLT table from SOH"
TBLPROPERTIES ("Quality" = "Bronze", "HasPII" = "No", "pipelines.autoOptimize.zOrderCols" = "HID",  "pipelines.cdc.tombstoneGCThresholdInSeconds" = "900")
AS SELECT 
    (SELECT AppendWaterMark from live.PipeLineVars) AS AppendWaterMark,
    CAST(DIV(COALESCE(CAST(REGEXP_REPLACE(HID,'[^0-9]','') AS LONG),0),1000) AS LONG) AS OrdNumDiv1K,
    *,
    COALESCE(CAST(HDATE AS TIMESTAMP),CURRENT_TIMESTAMP()) AS Occurred,
    ( CASE  -- simulate receiving orders over a period of days
          WHEN input_file_name() LIKE '%/SOH_1%' THEN CAST('2021-12-01 01:00:00.001001Z' AS TIMESTAMP)
          WHEN input_file_name() LIKE '%/SOH_2%' THEN CAST('2021-12-02 02:00:00.002002Z' AS TIMESTAMP)
          WHEN input_file_name() LIKE '%/SOH_3%' THEN CAST('2021-12-03 03:00:00.003003Z' AS TIMESTAMP)
          WHEN input_file_name() LIKE '%/SOH_4%' THEN CAST('2021-12-04 04:00:00.004004Z' AS TIMESTAMP)
          WHEN input_file_name() LIKE '%/SOH_5%' THEN CAST('2021-12-05 04:00:00.004004Z' AS TIMESTAMP)
          ELSE CURRENT_TIMESTAMP() 
      END ) AS Received ,
     input_file_name() as TriggerSourceEventId
FROM 
cloud_files("gs://databricks-991519771022734/991519771022734/mnt/poc_dlt_scenarios/SOH/", 
                  "json", 
                  map(
                      "cloudFiles.validateOptions", "true",
--                      "cloudFiles.projectId", "xiatech-research",
                      "cloudFiles.allowOverwrites", "true",
                      "cloudFiles.schemaEvolutionMode","addNewColumns",
                      "cloudFiles.inferColumnTypes", "true",
                      "cloudFiles.schemaHints", """HID STRING, HDATE TIMESTAMP, HCUST STRING, HCUSTNAME STRING""",
--                      "cloudFiles.multiline", "false",
                      "cloudFiles.includeExistingFiles", "true",
                      "cloudFiles.maxFilesPerTrigger", "999",
                      "cloudFiles.backfillInterval", "1 day"
                    )
);


-- COMMAND ----------

-- DBTITLE 1,Bring in any NEW SOL RAW JSON data for consideration
CREATE INCREMENTAL LIVE TABLE B0_inc_SOL_raw_json (
  CONSTRAINT valid_SOL_received EXPECT (HID IS NOT NULL AND LID IS NOT NULL) ON VIOLATION DROP ROW
)
PARTITIONED BY (OrdNumDiv1K,AppendWaterMark)
COMMENT "Bronze Stage 0 Base Raw Incoming bronze DLT table from SOL"
TBLPROPERTIES ("Quality" = "Bronze", "HasPII" = "No", "pipelines.autoOptimize.zOrderCols" = "HID",  "pipelines.cdc.tombstoneGCThresholdInSeconds" = "900")
AS 
SELECT 
    (SELECT AppendWaterMark from live.PipeLineVars)  AS AppendWaterMark,
    CAST(DIV(COALESCE(CAST(REGEXP_REPLACE(HID,'[^0-9]','') AS LONG),0),1000) AS LONG) AS OrdNumDiv1K,
    *,
    COALESCE(CAST(LDATE AS TIMESTAMP),CURRENT_TIMESTAMP()) AS Occurred,
    ( CASE  -- simulate receiving orders over a period of days
          WHEN input_file_name() LIKE '%/SOL_1%' THEN CAST('2021-12-01 01:01:00.001001Z' AS TIMESTAMP)
          WHEN input_file_name() LIKE '%/SOL_2%' THEN CAST('2021-12-02 02:02:00.002002Z' AS TIMESTAMP)
          WHEN input_file_name() LIKE '%/SOL_3%' THEN CAST('2021-12-03 03:03:00.003003Z' AS TIMESTAMP)
          WHEN input_file_name() LIKE '%/SOL_4%' THEN CAST('2021-12-04 04:04:00.004004Z' AS TIMESTAMP)
          WHEN input_file_name() LIKE '%/SOL_5%' THEN CAST('2021-12-04 04:04:00.004004Z' AS TIMESTAMP)
          ELSE CURRENT_TIMESTAMP() 
      END ) AS Received ,
   input_file_name() as TriggerSourceEventId
FROM 
cloud_files("gs://databricks-991519771022734/991519771022734/mnt/poc_dlt_scenarios/SOL/", 
                  "json", 
                  map(
                      "cloudFiles.validateOptions", "true",
                      "cloudFiles.allowOverwrites", "true",
                      "cloudFiles.schemaEvolutionMode","addNewColumns",
                      "cloudFiles.inferColumnTypes", "true",
                      "cloudFiles.schemaHints", """HID STRING, LID STRING, LDATE TIMESTAMP, PROD STRING, AMT DOUBLE""",
                      "cloudFiles.includeExistingFiles", "true",
                      "cloudFiles.maxFilesPerTrigger", "999",
                      "cloudFiles.backfillInterval", "1 day"
                    )
);

-- COMMAND ----------

-- DBTITLE 1,Add to RAW SO to existing SOH INGESTed table with some field renaming and decoration as needed. Calc if SOH is Deleted
CREATE INCREMENTAL LIVE TABLE B1_inc_SOH_ingest
PARTITIONED BY (OrdNumDiv1K,AppendWaterMark)
COMMENT "Bronze Stage 1 (BASE) DLT from RAW SOH with cleaned-up column datatypes/names."
TBLPROPERTIES ("Quality" = "Silver", "HasPII" = "MayBe", "pipelines.autoOptimize.zOrderCols" = "OrdNum",  "pipelines.cdc.tombstoneGCThresholdInSeconds" = "900")
AS SELECT  
  AppendWaterMark,
  CAST(OrdNumDiv1K AS LONG) AS OrdNumDiv1K,
  CONCAT("SalesOrderHeader_" , H.HID) AS OrdNum, 
  IF(CAST(H.HCUST AS STRING)="", True, False ) AS IsOrderDeleted,
  CAST(H.Occurred AS TIMESTAMP) AS Occurred,
  CAST(H.Received AS TIMESTAMP) AS Received,
  STRUCT ( 
    CAST(H.HCUST AS STRING) AS ID,
    CAST(H.HCUSTNAME AS STRING) AS Name
  ) AS Cust
FROM STREAM(live.B0_inc_SOH_raw_json) H

-- COMMAND ----------

-- DBTITLE 1,Add to RAW SOL to existing SOL INGESTed table with some field renaming and decoration as needed. Calc if SOL is Deleted
CREATE INCREMENTAL LIVE TABLE B1_inc_SOL_ingest
PARTITIONED BY (OrdNumDiv1K,AppendWaterMark)
COMMENT "Bronze Stage 1 (BASE) DLT from RAW SOL with cleaned-up column datatypes/names."
TBLPROPERTIES ("Quality" = "Silver", "HasPII" = "MayBe", "pipelines.autoOptimize.zOrderCols" = "OrdNum",  "pipelines.cdc.tombstoneGCThresholdInSeconds" = "900")
AS SELECT  
  AppendWaterMark,
  CAST(OrdNumDiv1K AS LONG) AS OrdNumDiv1K,
  CONCAT("SalesOrderHeader_" , L.HID) AS OrdNum, 
  CONCAT("SalesOrderLine_" , L.LID) AS LineId, 
  IF(CAST(L.AMT AS LONG) = 0, True, False ) AS IsLineDeleted,
  CAST(L.Occurred AS TIMESTAMP) AS Occurred,
  CAST(L.Received AS TIMESTAMP) AS Received,
  CAST(L.PROD AS STRING) Prod,
  CAST(L.AMT AS LONG) AS Amt
FROM STREAM(live.B0_inc_SOL_raw_json) L  

-- COMMAND ----------

-- DBTITLE 1,Use Incremental INGESTED data to work out which ORDERS (across SOH/SOL) we need to consider for appending to existing tables.
-- CREATE INCREMENTAL LIVE TABLE B2_inc_SOHL_keys_this_run 
-- PARTITIONED BY (AppendWaterMark,OrdNumDiv1K) -- can we use coonstraint to eliminate duplicates!  + Quaraninte s- GOOD FAIL
-- COMMENT "Bronze Stage 2 DLT Incremental INGESTED SOH/SOL data to work out which ORDERS we need to (append) build going forward."
-- TBLPROPERTIES ("Quality" = "Bronze", "HasPII" = "No", "pipelines.cdc.tombstoneGCThresholdInSeconds" = "900")
-- AS 
-- SELECT AppendWaterMark,OrdNumDiv1K,OrdNum
-- FROM (
--  SELECT 
--    H.AppendWaterMark,
--    H.OrdNumDiv1K,
--    H.OrdNum
--  FROM STREAM(live.B1_inc_SOH_ingest) H 
--  UNION ALL
--  SELECT 
--    L.AppendWaterMark,
--    L.OrdNumDiv1K,
--    L.OrdNum
--  FROM STREAM(live.B1_inc_SOL_ingest) L
-- )

-- COMMAND ----------

-- DBTITLE 1,Use Incremental INGESTED data to work out which ORDERS (across SOH/SOL) we need to consider for appending to existing tables.
CREATE INCREMENTAL LIVE TABLE B2_inc_SOHL_keys_this_run
PARTITIONED BY (AppendWaterMark,OrdNumDiv1K)
COMMENT "Bronze Stage 2 DLT Incremental INGESTED SOH/SOL data to work out which ORDERS we need to (append) build going forward."
TBLPROPERTIES ("Quality" = "Bronze", "HasPII" = "No", "pipelines.cdc.tombstoneGCThresholdInSeconds" = "900");
APPLY CHANGES INTO live.B2_inc_SOHL_keys_this_run 
FROM (
  SELECT 
    AppendWaterMark,
    OrdNumDiv1K,
    OrdNum,
    AppendWaterMark as LogicalOrderColumn
  FROM STREAM(live.B1_inc_SOH_ingest)
  UNION ALL
  SELECT 
    AppendWaterMark,
    OrdNumDiv1K,
    OrdNum,
    AppendWaterMark as LogicalOrderColumn
  FROM STREAM(live.B1_inc_SOL_ingest)
 )
KEYS (AppendWaterMark,OrdNumDiv1K, OrdNum)
SEQUENCE BY LogicalOrderColumn
COLUMNS * EXCEPT(LogicalOrderColumn)
;


-- COMMAND ----------

-- DBTITLE 1,Calc which SOH (based on OCCURRED TimeStamp) we need to USE (from this run or prior ingestions)
-- Calc Stage : Get list of unique Order Numers, with latest know OCCURRED, to filter from LIVE table for inclusion in any Append to SOH+ARRAY(SOL).
--
CREATE INCREMENTAL LIVE TABLE S0_inc_SOH_latest_unduped_for_filter
PARTITIONED BY (OrdNumDiv1K,AppendWaterMark)
COMMENT "Silver Stage 0 DLT Latest SOH details for filtering and merging purposes."
TBLPROPERTIES ("Quality" = "Silver", "HasPII" = "MayBe", "pipelines.autoOptimize.zOrderCols" = "OrdNum",  "pipelines.cdc.tombstoneGCThresholdInSeconds" = "900")
AS 
SELECT -- Get Latest SOH -- may be duplicates HERE for now!
  CDC.AppendWaterMark AS AppendWaterMark,
  CDC.OrdNumDiv1K AS OrdNumDiv1K,
  CDC.OrdNum AS OrdNum,
  SOH.MaxOccurred AS Occurred                         -- we will use Occured to pick what we call the "latest" later on
FROM 
  STREAM(live.B2_inc_SOHL_keys_this_run) AS CDC       -- list of keys to update using data that has now gone into actial live tables via stream now in this/past runs 
  JOIN (                                              -- we DONT use stream(live.B1_inc_SOH_ingest) as we need to get data even if from a previous run  
        SELECT OrdNumDiv1K, OrdNum, MAX(Occurred) AS MaxOccurred
        FROM   live.B1_inc_SOH_ingest
        GROUP BY OrdNumDiv1K,OrdNum
     ) AS SOH  
     USING (OrdNumDiv1K,OrdNum)


-- COMMAND ----------

-- DBTITLE 1,Build the SOH rows for joining purposes based on the now know OCCURRED timestamps in previous logical [sub] query
-- Append latest view of a SOH for incremental building next layer - by now any updates for this session should be in the relevant live table
--
CREATE INCREMENTAL LIVE TABLE S1_inc_SOH_latest_unduped
PARTITIONED BY (OrdNumDiv1K,AppendWaterMark)
COMMENT "Silver Stage 1 DLT Latest SOH data for an update session."
TBLPROPERTIES ("Quality" = "Silver", "HasPII" = "MayBe", "pipelines.autoOptimize.zOrderCols" = "OrdNum",  "pipelines.cdc.tombstoneGCThresholdInSeconds" = "900")
AS 
SELECT 
  SOHfilter.AppendWaterMark AS AppendWaterMark,
  SOHfilter.OrdNumDiv1K AS OrdNumDiv1K,
  SOHfilter.OrdNum AS OrdNum,
  MAX(SOH.IsOrderDeleted) AS IsOrderDeleted,
  MAX(SOHfilter.Occurred) AS Occurred,
  MAX(SOH.Received) AS Received,
  LAST(SOH.Cust) AS Cust
FROM 
  STREAM(live.S0_inc_SOH_latest_unduped_for_filter) AS SOHfilter       -- list of keys to update using data that has now gone into actial live tables via stream now in this/past runs
  JOIN live.B1_inc_SOH_ingest AS SOH 
    USING (OrdNumDiv1K,OrdNum,Occurred)
-- WHERE NOT SOH.IsOrderDeleted
GROUP BY                       -- Belt and braces to support agg predicates and ensure ONE SOH row per Order (but live table will accumulate al iterations of this)
  SOHfilter.AppendWaterMark,
  SOHfilter.OrdNumDiv1K,
  SOHfilter.OrdNum


-- COMMAND ----------

-- DBTITLE 1,Calc which SOL's (based on Line OCCURRED TimeStamp) we need to USE (from this run or prior ingestions)
-- Calc Stage : Get list of Order Detail to filter from LIVE table for inclusion in any Append to SOH+ARRAY(SOL).
--
CREATE INCREMENTAL LIVE TABLE S0_inc_SOL_latest_unduped_for_filter
PARTITIONED BY (OrdNumDiv1K,AppendWaterMark)
COMMENT "Silver Stage 0 DLT Latest SOL details for filtering and merging purposes."
TBLPROPERTIES ("Quality" = "Silver", "HasPII" = "MayBe", "pipelines.autoOptimize.zOrderCols" = "OrdNum",  "pipelines.cdc.tombstoneGCThresholdInSeconds" = "900")
AS 
SELECT  -- Get Latest SOL per Line Id -- may be dup-licates HERE for now!
    CDC.AppendWaterMark AS LatestAppendWaterMark,       -- we will use THIS AppendWaterMark to pick what we call the "latest" later on
    CDC.OrdNumDiv1K AS OrdNumDiv1K,
    CDC.OrdNum AS OrdNum,
    SOL.LineId AS LineId,
    SOL.MaxOccurred AS Occurred,                     -- we will use Occured to pick what we call the "latest" later on
    SOL.MaxReceived AS Received,                     -- we will use Received to pick what we call the "latest" later on
    SOL.PriorAppendWaterMark AS AppendWaterMark -- Belt&Braces: may be use this to limit scope lines to consider in next few steps & reloads/replays etc.
FROM 
  STREAM(live.B2_inc_SOHL_keys_this_run) AS CDC         -- list of keys to update using data that has now gone into actual live tables via stream now or past runs
  JOIN (                                              -- we DONT use stream(live.B1_inc_SOL_ingest) as we need to get data even if from a previous run
        SELECT OrdNumDiv1K, OrdNum, LineId, MAX(Occurred) as MaxOccurred, MAX(Received) as MaxReceived, MAX(AppendWaterMark) AS PriorAppendWaterMark
        FROM   live.B1_inc_SOL_ingest
        GROUP BY OrdNumDiv1K,OrdNum,LineId
     ) AS SOL
     USING (OrdNumDiv1K,OrdNum)


-- COMMAND ----------

-- DBTITLE 1,[WITH Line Detail] Build the SOL rows for joining purposes based on the now know OCCURRED timestamps in previous logical [sub] query
-- Append latest view of a SOH(DET) for incremental building next layer - by now any updates for this session should be in the relevant live table
-- Fingers crossed this is acceptable to Pipeline Execution! 
-- 
CREATE INCREMENTAL LIVE TABLE S1_inc_SOL_latest_unduped_subquery
PARTITIONED BY (OrdNumDiv1K,AppendWaterMark)
COMMENT "Silver Stage 1 DLT Latest SOL data for an update session."
TBLPROPERTIES ("Quality" = "Silver", "HasPII" = "MayBe", "pipelines.autoOptimize.zOrderCols" = "OrdNum",  "pipelines.cdc.tombstoneGCThresholdInSeconds" = "900")
AS 
SELECT  -- Get Latest SOL per Line Id - may still be duplicates if duplicate incoming files have been absorbed somehow
    OrdLines.LatestAppendWaterMark AS AppendWaterMark,   -- always user NOW for apppending as latest view of SOL
    OrdLines.OrdNumDiv1K AS OrdNumDiv1K,
    OrdLines.OrdNum AS OrdNum,
    OrdLines.LineId AS LineId,
    LDET.IsLineDeleted AS IsLineDeleted,                                -- used to filter out from final LATEST tables
    OrdLines.Occurred AS Occurred,
    OrdLines.Received AS Received,
    OrdLines.AppendWaterMark AS MostRecentLineAppendWaterMark,
    STRUCT(
        -- joined cols have to be referenced as LEFT table fields
        OrdLines.LineId AS LineId,    
        OrdLines.Occurred AS Occurred,
        OrdLines.Received AS Received,
        OrdLines.AppendWaterMark AS MostRecentLineAppendWaterMark,      -- keep this watermark here in case its useful
        OrdLines.LatestAppendWaterMark AS AppendWaterMark,              -- keep this watermark here in case its useful
        -- now all LDET non-joined columns presented here
        LDET.Prod AS Prod,    
        LDET.Amt AS Amt
    ) AS PerLineData
FROM 
  STREAM(live.S0_inc_SOL_latest_unduped_for_filter) AS OrdLines         -- list of keys to update using data that has now gone into actual live tables via stream now or past runs
  JOIN  live.B1_inc_SOL_ingest AS LDET                                  -- we DONT use stream(live.B1_inc_SOL_ingest) as we need to get data even if from a previous run if available
  USING (OrdNumDiv1K,OrdNum,LineId,Occurred,Received,AppendWaterMark)
--  (                                                                -- we DONT use stream(live.B1_inc_SOL_ingest) as we need to get data even if from a previous run if available
--        SELECT OrdNumDiv1K, OrdNum, LineId, Occurred,Received,AppendWaterMark, MAX(STRUCT(LDET.*)) AS LatestLineData
--        FROM   live.B1_inc_SOL_ingest AS LDET
--        GROUP BY OrdNumDiv1K,OrdNum,LineId,Occurred,Received,AppendWaterMark
--       ) AS SOL
--     USING (OrdNumDiv1K,OrdNum,LineId,Occurred)

-- COMMAND ----------

-- DBTITLE 1,Build the SOL rows for joining purposes based on the previous logical SOL [sub] queries.
-- Append latest view of a SOL(ARRAY) for incremental building next layer - by now any updates for this session should be in the relevant live table
-- Fingers crossed this is acceptable to Pipeline Execution! 
-- 
CREATE INCREMENTAL LIVE TABLE S1_inc_SOL_latest_unduped
PARTITIONED BY (OrdNumDiv1K,AppendWaterMark)
COMMENT "Silver Stage 2 DLT combining stuff."
TBLPROPERTIES ("Quality" = "Silver", "HasPII" = "MayBe", "pipelines.autoOptimize.zOrderCols" = "OrdNum",  "pipelines.cdc.tombstoneGCThresholdInSeconds" = "900")
AS 
SELECT                                       -- Outer query build overall latest line array for appending with SOH later.
  SOL.AppendWaterMark AS AppendWaterMark,
  SOL.OrdNumDiv1K AS OrdNumDiv1K,
  SOL.OrdNum AS OrdNum,
  MAX(SOL.Occurred) AS Occurred,
  MAX(SOL.Received) AS Received,
  MAX(SOL.MostRecentLineAppendWaterMark) AS MostRecentLineAppendWaterMark,         -- may be useful later - lkeave here for now
  SIZE(COLLECT_SET(SOL.LineId) FILTER(WHERE NOT SOL.IsLineDeleted)) AS LineCnt,    -- Using FILTER(WHERE NOT SOL.IsLineDeleted) we STILL build the SOL array so
  COLLECT_SET(SOL.PerLineData) FILTER(WHERE NOT SOL.IsLineDeleted)  AS Lines       -- we should still have net effect of BLANKING out deleted lines in SOH_with_SOL_Array_latest
FROM 
  STREAM(live.S1_inc_SOL_latest_unduped_subquery) AS SOL
GROUP BY 
  SOL.AppendWaterMark,
  SOL.OrdNumDiv1K, 
  SOL.OrdNum


-- COMMAND ----------

-- DBTITLE 1,For all Orders detected in this run, Orders detected in this run, BUILD a LATEST view and append to this table for subsequent materialisation to table SOH_with_SOL_Array_latest
-- Bring it together - append latest view of SOH + SOL JUST built above to create a usable Silver table containing SOH+ARRAY(SOL)
-- Demo: Could use a view like DLT_DB_POC_2.S2_inc_soh_sol_array_latest_only (from POC_3_Queries Notebook) to present LATEST view
-- This table can be constrained on AppendWaterMark to also "dynamically timetravel" 
-- on top of normal DELTA table timetravel directives though i suspect directives may be a lot faster.
--
CREATE INCREMENTAL LIVE TABLE S2_inc_SOHL_array_unduped
PARTITIONED BY (OrdNumDiv1K,AppendWaterMark)
COMMENT "Silver Stage 2 DLT combining SOH,SOL to SOH+ARRAY(SOL)."
TBLPROPERTIES ("Quality" = "Silver", "HasPII" = "MayBe", "pipelines.autoOptimize.zOrderCols" = "OrdNum",  "pipelines.cdc.tombstoneGCThresholdInSeconds" = "900")
AS 
SELECT 
  CDC.AppendWaterMark AS AppendWaterMark,                   -- So we know this is the LATEST version of this Object using TS from this run.
  CDC.OrdNumDiv1K AS OrdNumDiv1K,
  CDC.OrdNum AS OrdNum,
  COALESCE(SOH.Occurred,SOL.Occurred) AS Occurred,   -- default to SOL value in case SOH still not received
  COALESCE(SOH.Received,SOL.Received) AS Received,   -- default to SOL value in case SOH still not received
  SOH.IsOrderDeleted AS IsOrderDeleted,
  SOH.IsOrderDeleted AS IsOrderDeleted2,
  SOH.Cust AS Cust,
  COALESCE(SOL.LineCnt,0) AS LineCnt,
  SOL.Lines,
  -- LogicalOrderOfCdcEvents ends up equivalent to Sequencing by (Occurred,Received,LatestLineVersion,CurrentTimestamp)
  -- BIGINT can take a number as higher than 100000000000000000 which is way bigger than any unix_micros() 
  -- so a strigified concat of these can be relied on to be in correct order to UPSERT to SOH_with_SOL_Array_latest
CONCAT(  CAST(100000000000000000 + COALESCE(unix_micros(GREATEST(SOH.Occurred,SOL.Occurred)),0) AS STRING),     
         CAST(100000000000000000 + COALESCE(unix_micros(GREATEST(SOH.Received,SOL.Received)),0) AS STRING),
         CAST(100000000000000000 + COALESCE(SOL.MostRecentLineAppendWaterMark,0) AS STRING),           -- Extra sequencing to pick very latest S1_inc_SOL_latest_unduped as final update
         CAST(100000000000000000 + COALESCE(CDC.AppendWaterMark,0) AS STRING)
  )    AS LogicalOrderOfCdcEvents ,
CONCAT(  CAST(100000000000000000 + COALESCE(unix_micros(GREATEST(SOH.Occurred,SOL.Occurred)),0) AS STRING),     
         CAST(100000000000000000 + COALESCE(unix_micros(GREATEST(SOH.Received,SOL.Received)),0) AS STRING),
         CAST(100000000000000000 + COALESCE(SOL.MostRecentLineAppendWaterMark,0) AS STRING),           -- Extra sequencing to pick very latest S1_inc_SOL_latest_unduped as final update
         CAST(100000000000000000 + COALESCE(CDC.AppendWaterMark,0) AS STRING)
  )    AS LogicalOrderOfCdcEvents2
  
FROM 
  STREAM(live.B2_inc_SOHL_keys_this_run) AS CDC -- list of keys to update using data that has now gone into live tables via stream (this is primary table for left joining to  as SOH may not exist yet)

LEFT JOIN -- Use last know version of SOH in case not in STREAM() - but if in STREAM() it will now be in live.X
  live.S1_inc_SOH_latest_unduped AS SOH  USING(OrdNumDiv1K,OrdNum,AppendWaterMark)   -- Use the latest (AppendWaterMark) one we JUST (re)built above if any
       
LEFT JOIN -- Use last know version of SOL(ARRAY) in case not in STREAM() - but if in STREAM() it will now be in live.X
  live.S1_inc_SOL_latest_unduped AS SOL  USING(OrdNumDiv1K,OrdNum,AppendWaterMark)   -- Use the latest (AppendWaterMark) one we JUST (re)built above if any
 

-- COMMAND ----------

-- DBTITLE 1,FINALLY: Materialise (rebuild) latest de-duped table for primary queries.
-- CREATE LIVE TABLE SOH_with_SOL_Array_latest
-- PARTITIONED BY (OrdNumDiv1K)
-- COMMENT "Silver Stage 3 DLT Presenting a recalculated LATEST SOH+ARRAY(SOL) for rapid base reporting and building to GOLD layers."
-- TBLPROPERTIES ("Quality" = "Silver", "HasPII" = "MayBe", "pipelines.autoOptimize.zOrderCols" = "OrdNum,Occurred,Received",  "pipelines.cdc.tombstoneGCThresholdInSeconds" = "900")
-- AS 
-- SELECT 
--    * EXCEPT (DeDupHdrRow)
-- FROM (
--         SELECT 
--           * EXCEPT (AppendWaterMark),  -- AppendWaterMark shouldnt be needed beyond this point and allows INCREMENTAL to be efficient
--           ROW_NUMBER() OVER (PARTITION BY OrdNumDiv1K, OrdNum ORDER BY Occurred DESC, Received DESC, AppendWaterMark Desc) AS DeDupHdrRow 
--         FROM live.S2_inc_SOHL_array_unduped 
--      )
-- WHERE DeDupHdrRow = 1


-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE SOH_with_SOL_Array_latest
PARTITIONED BY (OrdNumDiv1K)
COMMENT "Silver Stage 3 DLT Presenting a recalculated LATEST SOH+ARRAY(SOL) for rapid base reporting and building to GOLD layers."
TBLPROPERTIES ("Quality" = "Silver", "HasPII" = "MayBe", "pipelines.autoOptimize.zOrderCols" = "OrdNum,Occurred,Received",  "pipelines.cdc.tombstoneGCThresholdInSeconds" = "900");

APPLY CHANGES INTO live.SOH_with_SOL_Array_latest 
FROM stream(live.S2_inc_SOHL_array_unduped)
  KEYS (OrdNumDiv1K, OrdNum)
APPLY AS DELETE WHEN IsOrderDeleted2
SEQUENCE BY LogicalOrderOfCdcEvents2
COLUMNS * EXCEPT(IsOrderDeleted2,LogicalOrderOfCdcEvents2)
;
