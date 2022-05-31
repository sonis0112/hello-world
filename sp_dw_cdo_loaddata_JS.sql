USE role AR_DEV_E_DWS_D_CDP_ETL_OPTUM_ROLE;
USE WAREHOUSE ECT_DEV_CRS_WH;
USE DATABASE ECT_DEV_CRS_DB;
USE SCHEMA CRS_COMPACT;
alter session set timezone = 'America/Chicago';

create or replace procedure CRS_COMPACT.sp_dw_cdo_loaddata()
returns string
language JAVASCRIPT
as
$$
var lv_step = 1;
var output_array = [];

// helper funtion for logging
function log ( msg ) {
output_array.push(msg)
}

try
{
//pull latest timestamps from the date control table
lv_step =2;
var cmd = `select to_varchar(T1.START_DTTM, 'YYYY-MM-DD HH:MI:SS'), to_varchar(T1.CURR_TIME, 'YYYY-MM-DD HH:MI:SS') from (select TO_TIMESTAMP_NTZ(max(START_DTTM)) START_DTTM, TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP(0)) CURR_TIME from "CRS_ETL"."DATE_CNTL" WHERE TABLE_NM = 'DW_CDO') T1
`;
var stmt = snowflake.createStatement( { sqlText: cmd } );
var result1 = stmt.execute();
result1.next();
var start_dt = result1.getColumnValue(1);
var end_dt = result1.getColumnValue(2);
log('start_dt: '+ start_dt);
log('end_dt: '+ end_dt);

//generate temp table by applying all transformations to load data into target table 
lv_step =3;
var query_result = `
CREATE OR REPLACE TEMPORARY TABLE "CRS_COMPACT"."TEMP_DW_CDO" AS (
SELECT
concat('HSH' || CDO_ID) as DW_CDO_REC_ID
,CDO_ID
,'HSH' as DW_SYS_REF_CD
,CDO as CDO_NM
,'' as DATA_SECUR_RULE_LIST
,'' as DATA_QLTY_ISS_LIST
, CASE WHEN Is_Active = 0 OR operation = "DELETE" THEN FALSE ELSE TRUE END as DW_SRC_REC_STS_CD
,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP(0)) as DW_CREAT_DTTM
,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP(0)) as DW_CHG_DTTM
FROM "CRS_COMPACT"."Luna_contract_plantype_lookup_CDO"
)`;

log('query_result:'+ query_result);
snowflake.createStatement({sqlText: query_result}).execute();

//insert or update to the target table DW_CDO in foundation layer 
lv_step =4;
var merge_query = `
MERGE INTO "CRS_Foundation"."DW_CDO" REF
USING "CRS_COMPACT"."TEMP_DW_CDO" TREF ON REF.CDO_ID = TREF.CDO_ID
WHEN MATCHED THEN
UPDATE SET
REF.DW_CDO_REC_ID = TREF.DW_CDO_REC_ID,
REF.CDO_ID = TREF.CDO_ID,
REF.DW_SYS_REF_CD = TREF.DW_SYS_REF_CD,
REF.CDO_NM = TREF.CDO_NM,
REF.DATA_SECUR_RULE_LIST = TREF.DATA_SECUR_RULE_LIST,
REF.DATA_QLTY_ISS_LIST = TREF.DATA_QLTY_ISS_LIST,
REF.DW_SRC_REC_STS_CD = TREF.DW_SRC_REC_STS_CD,
REF.DW_CREAT_DTTM = TREF.DW_CREAT_DTTM,
REF.DW_CHG_DTTM = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP(0))
WHEN NOT MATCHED THEN
INSERT
(
REF.DW_CDO_REC_ID,REF.CDO_ID,REF.DW_SYS_REF_CD,REF.CDO_NM,REF.DATA_SECUR_RULE_LIST,REF.DATA_QLTY_ISS_LIST,REF.DW_SRC_REC_STS_CD,REF.DW_CREAT_DTTM,REF.DW_CHG_DTTM
)
VALUES
(
TREF.DW_CDO_REC_ID,TREF.CDO_ID,TREF.DW_SYS_REF_CD,TREF.CDO_NM,TREF.DATA_SECUR_RULE_LIST,TREF.DATA_QLTY_ISS_LIST,TREF.DW_SRC_REC_STS_CD,TREF.DW_CREAT_DTTM,TREF.DW_CHG_DTTM
)
`;

log('merge_query:'+ merge_query);
snowflake.createStatement({sqlText: merge_query}).execute();

//update end date with current timestamp 
lv_step =5;
var update_end_dt = `
update "CRS_ETL"."DATE_CNTL" DTC
set DTC.END_DTTM = '${end_dt}'
WHERE START_DTTM = '${start_dt}'
AND TABLE_NM = 'DW_CDO'
`;

log('update_end_dt:'+ update_end_dt);
snowflake.createStatement({sqlText: update_end_dt}).execute();


//insert a new row into date cntl table for the next timeframe 
lv_step =6;
var update_dt = `
INSERT INTO "CRS_ETL"."DATE_CNTL" (TABLE_NM,START_DTTM,END_DTTM,LAYER) VALUES('DW_CDO','${end_dt}','9999-12-31 00:00:00.000','CTC')
`;

log('update_dt:'+ update_dt);
snowflake.createStatement({sqlText: update_dt}).execute();

return output_array;
}

catch(err)
{
ret = "Failed at Step " + lv_step
ret += "\n Failed: Code: " + err.code + "\n State: " + err.state;
ret += "\n Message: " + err.message;
ret += "\nStack Trace:\n" + err.stackTraceTxt;
throw err;

}$$