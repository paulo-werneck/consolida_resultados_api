----------------------------------------------------------------------------
-- Autor: Paulo Werneck
-- Data: 06/07/2020
-- Desc: DDL - Tabela do AWS Athena com os retornos da api modelo crediario_cdc
----------------------------------------------------------------------------

CREATE EXTERNAL TABLE TURING_PRD_SCOREAPI_USAGE.TB_CREDIARIO_CDC_OUTPUT
(
	ID_TRANSACAO STRING,
	CD_CLI BIGINT,
	DATA STRING,
	SEGMENTO STRING,
	HIST_VV DOUBLE,
	SCORE DOUBLE
)
PARTITIONED BY (dt_particao bigint)
STORED AS PARQUET
LOCATION 's3://bkt-api-viavarejo-prd/report/target/turing_prd_scoreapi_usage/tb_crediario_cdc_output/';
