// Databricks notebook source
// DBTITLE 1,ZAP Scenario 1 Json Files 
try {dbutils.fs.rm("/mnt/poc_dlt_scenarios/SOH/SOH_1.json")} catch {case _: Throwable => println("Silence!")} finally {  println("Zapped SOH_1")}
try {dbutils.fs.rm("/mnt/poc_dlt_scenarios/SOL/SOL_1.json")} catch {case _: Throwable => println("Silence!")} finally {  println("Zapped SOL_1")}

display((dbutils.fs.ls("/mnt/poc_dlt_scenarios/SOH/") ++ dbutils.fs.ls("/mnt/poc_dlt_scenarios/SOL/")))

// COMMAND ----------

// DBTITLE 1,CREATE Scenario 1, Occurred: 2021-12-01 , Order:H1=Header+2 Order Lines L1,L2
dbutils.fs.put("/mnt/poc_dlt_scenarios/SOH/SOH_1.json", """{"HID":"H1","HDATE":"2021-12-01","HCUST":"C1","HCUSTNAME":"Charlie"}""", true)

dbutils.fs.put("/mnt/poc_dlt_scenarios/SOL/SOL_1.json", """
{"HID":"H1","LID":"L1","LDATE":"2021-12-01","PROD":"P1","AMT":13}
{"HID":"H1","LID":"L2","LDATE":"2021-12-01","PROD":"P2","AMT":17}
""", true)

display((dbutils.fs.ls("/mnt/poc_dlt_scenarios/SOH/") ++ dbutils.fs.ls("/mnt/poc_dlt_scenarios/SOL/")))

// COMMAND ----------

// DBTITLE 1,ZAP Scenario 2 Json Files
try {dbutils.fs.rm("/mnt/poc_dlt_scenarios/SOH/SOH_2.json")} catch {case _: Throwable => println("Silence!")} finally {  println("Zapped SOH_2")}
try {dbutils.fs.rm("/mnt/poc_dlt_scenarios/SOL/SOL_2.json")} catch {case _: Throwable => println("Silence!")} finally {  println("Zapped SOL_2")}

display((dbutils.fs.ls("/mnt/poc_dlt_scenarios/SOH/") ++ dbutils.fs.ls("/mnt/poc_dlt_scenarios/SOL/")))

// COMMAND ----------

// DBTITLE 1,CREATE Scenario 2, Occurred: 2021-12-02,  Order:29H99 Header. Order:H1=Line L3, Line 2 Edit
dbutils.fs.put("/mnt/poc_dlt_scenarios/SOH/SOH_2.json", """{"HID":"29H99","HDATE":"2021-12-02","HCUST":"C2","HCUSTNAME":"Bobby"}""", true)

dbutils.fs.put("/mnt/poc_dlt_scenarios/SOL/SOL_2.json", """
{"HID":"H1","LID":"L3","LDATE":"2021-12-02","PROD":"P3","AMT":24}
{"HID":"H1","LID":"L2","LDATE":"2021-12-02","PROD":"P2","AMT":25}
""", true)

display((dbutils.fs.ls("/mnt/poc_dlt_scenarios/SOH/") ++ dbutils.fs.ls("/mnt/poc_dlt_scenarios/SOL/")))

// COMMAND ----------

// MAGIC %md Scenario 3

// COMMAND ----------

// DBTITLE 1,ZAP Scenario 3 Json Files
try {dbutils.fs.rm("/mnt/poc_dlt_scenarios/SOH/SOH_3.json")} catch {case _: Throwable => println("Silence!")} finally {  println("Zapped SOH_3")}
try {dbutils.fs.rm("/mnt/poc_dlt_scenarios/SOL/SOL_3.json")} catch {case _: Throwable => println("Silence!")} finally {  println("Zapped SOL_3")}

display((dbutils.fs.ls("/mnt/poc_dlt_scenarios/SOH/") ++ dbutils.fs.ls("/mnt/poc_dlt_scenarios/SOL/")))

// COMMAND ----------

// DBTITLE 1,CREATE Scenario 3, Occurred: 2021-12-03,  Order:29H99=Line L4 (Late), Order:3H3333=No Header, Line L5 (Early), BAD SOH file!
dbutils.fs.put("/mnt/poc_dlt_scenarios/SOH/SOH_3.json", "//", true)   // how does it deal with empty files?

dbutils.fs.put("/mnt/poc_dlt_scenarios/SOL/SOL_3.json", """
{"HID":"29H99","LID":"L4","LDATE":"2021-12-02","PROD":"P5","AMT":35}
{"HID":"3H3333","LID":"L5","LDATE":"2021-12-03","PROD":"P6","AMT":37}
""", true)

display((dbutils.fs.ls("/mnt/poc_dlt_scenarios/SOH/") ++ dbutils.fs.ls("/mnt/poc_dlt_scenarios/SOL/")))

// COMMAND ----------

// DBTITLE 1,ZAP Scenario 4 Json Files
try {dbutils.fs.rm("/mnt/poc_dlt_scenarios/SOH/SOH_4.json")} catch {case _: Throwable => println("Silence!")} finally {  println("Zapped SOH_4")}
try {dbutils.fs.rm("/mnt/poc_dlt_scenarios/SOL/SOL_4.json")} catch {case _: Throwable => println("Silence!")} finally {  println("Zapped SOL_4")}

display((dbutils.fs.ls("/mnt/poc_dlt_scenarios/SOH/") ++ dbutils.fs.ls("/mnt/poc_dlt_scenarios/SOL/")))

// COMMAND ----------

// DBTITLE 1,CREATE Scenario 4, 2021-12-04, Order:29H99= Add 1 Line, Delete 1 Line , Order:3H3333=Delete Order Line received yesterday
dbutils.fs.put("/mnt/poc_dlt_scenarios/SOL/SOL_4.json", """
{"HID":"29H99","LID":"L6","LDATE":"2021-12-04","PROD":"P6","AMT":10}
{"HID":"29H99","LID":"L4","LDATE":"2021-12-04","PROD":"P5","AMT":0}
{"HID":"3H3333","LID":"L5","LDATE":"2021-12-04","PROD":"P6","AMT":0}
""", true)

display((dbutils.fs.ls("/mnt/poc_dlt_scenarios/SOH/") ++ dbutils.fs.ls("/mnt/poc_dlt_scenarios/SOL/")))

// COMMAND ----------

// DBTITLE 1,ZAP Scenario 5 Json Files
try {dbutils.fs.rm("/mnt/poc_dlt_scenarios/SOH/SOH_5.json")} catch {case _: Throwable => println("Silence!")} finally {  println("Zapped SOH_5")}

display((dbutils.fs.ls("/mnt/poc_dlt_scenarios/SOH/") ++ dbutils.fs.ls("/mnt/poc_dlt_scenarios/SOL/")))

// COMMAND ----------

// DBTITLE 1,CREATE Scenario 5, 2021-12-05, Order:3H3333  Deleted 
dbutils.fs.put("/mnt/poc_dlt_scenarios/SOH/SOH_5.json", """{"HID":"3H3333","HDATE":"2021-12-05","HCUST":"","HCUSTNAME":"Alfred"}""", true)   // HCUST="" means DELETE ORDER in pipeline Logic

display((dbutils.fs.ls("/mnt/poc_dlt_scenarios/SOH/") ++ dbutils.fs.ls("/mnt/poc_dlt_scenarios/SOL/")))