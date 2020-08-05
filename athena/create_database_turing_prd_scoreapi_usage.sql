----------------------------------------------------------------------------
-- Autor: Paulo Werneck
-- Data: 06/07/2020
-- Desc: DDL - Database do AWS Athena com as consolidacoes da api
----------------------------------------------------------------------------

CREATE DATABASE TURING_PRD_SCOREAPI_USAGE
LOCATION 's3://bkt-api-viavarejo-prd/report/target';
