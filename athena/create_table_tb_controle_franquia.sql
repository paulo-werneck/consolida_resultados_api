----------------------------------------------------------------------------
-- Autor: Paulo Werneck
-- Data: 05/08/2020
-- Desc: DDL - Tabela do AWS Athena para controle dos limites de uso da franquia
----------------------------------------------------------------------------

CREATE EXTERNAL TABLE TURING_PRD_SCOREAPI_USAGE.TB_CONTROLE_FRANQUIA
(
  ID SMALLINT,
  DT_INICIO_VIGENCIA TIMESTAMP,
  DT_FIM_VIGENCIA TIMESTAMP,
  VL_MAX_FRANQUIA DOUBLE,
  VL_FATOR_EXCEDENTE DOUBLE
)
COMMENT "TABELA PARA CONTROLE DOS LIMITES DE USO DA FRANQUIA"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://bkt-api-viavarejo-prd/report/target/turing_prd_scoreapi_usage/tb_controle_franquia/';