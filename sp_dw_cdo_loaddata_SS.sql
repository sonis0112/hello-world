USE role AR_DEV_E_DWS_D_CDP_ETL_OPTUM_ROLE;
USE WAREHOUSE ECT_DEV_CRS_WH;
USE DATABASE ECT_DEV_CRS_DB;
USE SCHEMA CRS_COMPACT;
alter session set timezone = 'America/Chicago';

create or replace procedure CRS_COMPACT.sp_dw_cdo_loaddata()
returns string
language sql
as
$$
declare
lv_step integer;
start_dt date;
end_dt date;

begin

//pull latest timestamps from the date control table
lv_step :=1;
select to_varchar(T1.START_DTTM, 'YYYY-MM-DD HH:MI:SS') into :start_dt, to_varchar(T1.CURR_TIME, 'YYYY-MM-DD HH:MI:SS') into :end_dt from (select TO_TIMESTAMP_NTZ(max(START_DTTM)) START_DTTM, TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP(0)) CURR_TIME from "CRS_ETL"."DATE_CNTL" WHERE TABLE_NM = 'DW_CDO') T1;

call log.public.captureLog('lv_step' || '  ' || :lv_step);
call log.public.captureLog('start_dt' || '  ' || :start_dt);
call log.public.captureLog('end_dt' || '  ' || :end_dt);

//generate temp table by applying all transformations to load data into target table 
lv_step :=2;
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
);

call log.public.captureLog('lv_step' || '  ' || :lv_step);
call log.public.captureLog('Created the temporary table : CRS_COMPACT.TEMP_DW_CDO');

//insert or update to the target table DW_CDO in foundation layer 
lv_step :=3;
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
);

call log.public.captureLog('lv_step' || '  ' || :lv_step);
call log.public.captureLog('Merge query : CRS_Foundation.DW_CDO');

//update end date with current timestamp 
lv_step :=4;
update "CRS_ETL"."DATE_CNTL" DTC
set DTC.END_DTTM = :end_dt
WHERE START_DTTM = :start_dt
AND TABLE_NM = 'DW_CDO';

call log.public.captureLog('lv_step' || '  ' || :lv_step);
call log.public.captureLog('Update query : CRS_ETL.DATE_CNTL');

//insert a new row into date cntl table for the next timeframe 
lv_step :=5;
INSERT INTO "CRS_ETL"."DATE_CNTL" (TABLE_NM,START_DTTM,END_DTTM,LAYER) VALUES('DW_CDO',:end_dt,'9999-12-31 00:00:00.000','CTC');

call log.public.captureLog('lv_step' || '  ' || :lv_step);
call log.public.captureLog('Insert query : CRS_ETL.DATE_CNTL');

return 'passed';

exception
  when other then
    return object_construct( 'Failed at Step',lv_step,
                            'Failed Code: ', sqlcode,
                            'SQLERRM', sqlerrm,
                            'State: ', sqlstate);
end;							
$$