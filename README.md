# DataBricks-Delta-Live-Tables-POC
DataBricks Sales Order and Lines Pipeline POC . Final Table is SOH + ARRAY(SOL) LATEST view after edits

Using as a Learning Exercise for new Delta Live Table (DLT) Pipeline Feature currently under pre-release to limited audience.

Goal: 
Create 5 seperate (DLT pipeline run) sceanrios for ingesting Sales Orders and Sales Lines in a Combined DLT pipeline

Main PIPELINE Notebook is called : POC_2_pipeline
Note1) I have "Watermarked" All Incremental Data with a fixed value unix_micros(current_timestamp()) referenced as "AppendWaterMark".
Note2) I have partioned all tables using a all but last 3 digits of order number (once all alpha are stripped away) - this could easily be a DATE value.


For Pipeline config I have used these values but any values of own choosing are OK:
a) Target Location for saveing DLT data ("storage": "/mnt/poc_pipeline_2")
b) Name of the Database to use for registering the Materialised DLT tables:  ("target": "DLT_DB_POC_2")

Notebook to Create Scenario files per 5 scenario runs is: <B>POC_1_create_json_scenarios</B>

At =end of each sceanrio run, results can be checked using : <B>select * from DLT_DB_POC_2.SOH_with_SOL_Array_latest</B>


Scenarios are detailed by json file contents below:
<code>
-- Scenario 1, Occurred: 2021-12-01 
-- SOH_1.json: {"HID":"H1","HDATE":"2021-12-01","HCUST":"C1","HCUSTNAME":"Satnam"}           -- NEW HEADER SO:H1
-- SOL_1.json: {"HID":"H1","LID":"L1","LDATE":"2021-12-01","PROD":"P1","AMT":13}             -- NEW LINE L1 for SO:H1
--             {"HID":"H1","LID":"L2","LDATE":"2021-12-01","PROD":"P2","AMT":17}             -- NEW LINE L2 for SO:H1
--
-- Scenario 2, Occurred: 2021-12-02,  
-- SOH_2.json: {"HID":"29H99","HDATE":"2021-12-02","HCUST":"C2","HCUSTNAME":"Kurt"}          -- NEW HEADER SO:29H99
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
-- SOL_4.json: {"HID":"29H99", "LID":"L6","LDATE":"2021-12-02","PROD":"P6","AMT":10}         -- NEW LINE L6 for SO:29H99
--             {"HID":"29H99", "LID":"L4","LDATE":"2021-12-02","PROD":"P5","AMT":0 }         -- DELETE LINE L4 on SO:29H99  so will be flagged "IsLineDelete=True"
--             {"HID":"3H3333","LID":"L5","LDATE":"2021-12-03","PROD":"P6","AMT":0 }         -- DELETE LINE L5 on SO:3H3333 (Header was never actually Received)  SOL will be flagged "IsLineDelete=True"  (**)

-- (**) Suspect that we need to say if all lines in SOL Array are "IsLineDelete=True" then set SOH column "IsOrderDeleted=True" also?? at moment it is not changed- bnut Scenario 5 doe hard delete
-- Scenario 4, 2021-12-04, Incoiming Order:29H99  Add 1 Line, Delete 1 Line ,   Order:3H3333 Delete Order Line received yesterday (without header)
-- SOH_5.json: {"HID":"3H3333","HDATE":"2021-12-04","HCUST":"","HCUSTNAME":"Kurt"}           -- DELETE  Header 3H3333  -- but does this also mean ZAP lines ... hmmm  SOH will be flagged "IsOrderDelete=True"  
-- SOL_5.json: <****Not Created****>                                                         -- NO SOL headers file received 
  </code>
