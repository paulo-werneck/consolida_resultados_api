----------------------------------------------------------------------------
-- Autor: Paulo Werneck
-- Data: 05/08/2020
-- Desc: DDL - Tabela do AWS Athena com as respostas do dia corrente da api do modelo crediario_cdc
----------------------------------------------------------------------------

CREATE EXTERNAL TABLE TURING_PRD_SCOREAPI_USAGE.TB_STAGE_CREDIARIO_CDC_OUTPUT
(
	ID_TRANSACAO            STRING,
	CD_CLI                  BIGINT,
	DATA                    STRING,
	SEGMENTO                STRING,
	HIST_VV                 DOUBLE,
	SCORE                   DOUBLE,
	DTH_INGESTAO_ARQUIVO    TIMESTAMP,
    DT_PARTICAO             DATE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://bkt-api-viavarejo-prd/report/stage/crediario_cdc/output/'
TBLPROPERTIES("skip.header.line.count"="1");