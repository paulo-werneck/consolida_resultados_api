CREATE DATABASE TURING_PRD_SCOREAPI_USAGE
LOCATION 's3://bkt-api-viavarejo-prd/report/target';



CREATE EXTERNAL TABLE TURING_PRD_SCOREAPI_USAGE.TB_CREDIARIO_CDC_OUTPUT
(
	ID_TRANSACAO STRING,
	CD_CLI BIGINT,
	DATA STRING,
	SEGMENTO TINYINT,
	HIST_VV DOUBLE,
	SCORE DOUBLE
)
PARTITIONED BY (dt_particao bigint)
STORED AS PARQUET
LOCATION 's3://bkt-api-viavarejo-prd/report/target/turing_prd_scoreapi_usage/tb_crediario_cdc_output/';



CREATE EXTERNAL TABLE TURING_PRD_SCOREAPI_USAGE.TB_CREDIARIO_CDC_INPUT
(
	ID_TRANSACAO                        STRING,
    ID                                  INT,
    DATA_CADASTRO                       STRING,
    DATA_NASCIMENTO                     STRING,
    CEP                                 INT,
    SEXO                                STRING,
    CD_CRD_PON_DIA                      INT,
    CD_CRD_PON_A15                      INT,
    CD_CRD_PON_A30                      INT,
    CD_CRD_PON_A45                      INT,
    CD_CRD_PON_A60                      INT,
    CD_CRD_PON_A90                      INT,
    CD_CRD_PON_M90                      INT,
    QT_CRD_ACO                          INT,
    QT_CRD_ANI                          INT,
    QT_CRD_ANI_PRL                      INT,
    VC                                  INT,
    VV                                  INT,
    UF_FILIAL                           STRING,
    SEGMENTO                            INT,
    SCORE_VV_2018                       INT,
    PERC_AC15                           FLOAT,
    PERC_AC30                           FLOAT,
    PERC_AC45                           FLOAT,
    PERC_AC60                           FLOAT,
    PERC_AC90                           FLOAT,
    PERC_PARCELAS_DIA                   FLOAT,
    ANISTIA_EVER                        INT,
    ACORDO_EVER                         INT,
    ACORDO_AN_EVER                      INT,
    ID_FILIAL                           INT,
    CLI_TIPO                            INT,
    MOTIVO_M                            BOOLEAN,
    DATREF_ABT                          INT,
    DATREF_ABT_PARTITION                INT,
    DTH_PROC_CARGA                      BIGINT,
    DTH_PROC_CARGA_PARTITION            BIGINT,
    CD_CLI_FINAL                        BIGINT,
    DATREF_VAV                          INT,
    DTH_PROC_CARGA_VAV                  BIGINT,
    CD_CLI_VAV                          BIGINT,
    FLAG_PAG_CARTAO_15M                 INT,
    FLAG_PAG_VV_15M                     INT,
    FLAG_PAG_OUTRAS_15M                 INT,
    QT_MESES_COMPROU_CC_12M             BIGINT,
    QT_MESES_COMPROU_CC_15M             BIGINT,
    QT_COMPRA_DIN_9M                    BIGINT,
    QT_COMPRA_DIN_15M                   BIGINT,
    QT_COMPRA_3M_VV                     BIGINT,
    QT_COMPRA_6M_VV                     BIGINT,
    QT_COMPRA_12M_VV                    BIGINT,
    QT_COMPRA_15M_VV                    BIGINT,
    QT_COMPRA_SERV_3M                   BIGINT,
    QT_COMPRA_SERV_6M                   BIGINT,
    QT_COMPRA_SERV_9M                   BIGINT,
    QT_COMPRA_CC_3M                     BIGINT,
    QT_COMPRA_CC_6M                     BIGINT,
    TOTAL_ITENS_6M                      BIGINT,
    TOTAL_ITENS_12M                     BIGINT,
    TOTAL_ITENS_15M                     BIGINT,
    QT_ITENS_DIF_12M                    BIGINT,
    QT_ITENS_DIF_15M                    BIGINT,
    MED_PLANO_CC_3M                     FLOAT,
    MED_PLANO_CC_6M                     FLOAT,
    MED_PLANO_CC_9M                     FLOAT,
    MED_PLANO_CC_12M                    FLOAT,
    MED_PLANO_CC_15M                    FLOAT,
    MAX_PLANO_CC_3M                     FLOAT,
    MAX_PLANO_CC_6M                     FLOAT,
    MAX_PLANO_CC_12M                    FLOAT,
    MAX_PLANO_CC_15M                    FLOAT,
    QT_COMPRA_MERC_12M                  BIGINT,
    QT_COMPRA_MERC_15M                  BIGINT,
    QT_FORMA_PGT_12M                    INT,
    QT_FORMA_PGT_15M                    INT,
    FLAG_COMPRA_VV_1                    INT,
    FLAG_COMPRA_VV_2                    INT,
    FLAG_COMPRA_VV_3                    INT,
    FLAG_COMPRA_VV_4                    INT,
    FLAG_COMPRA_VV_5                    INT,
    FLAG_COMPRA_VV_6                    INT,
    FLAG_COMPRA_VV_7                    INT,
    FLAG_COMPRA_VV_8                    INT,
    FLAG_COMPRA_VV_9                    INT,
    FLAG_COMPRA_VV_10                   INT,
    FLAG_COMPRA_VV_11                   INT,
    FLAG_COMPRA_VV_12                   INT,
    FLAG_COMPRA_VV_13                   INT,
    FLAG_COMPRA_VV_14                   INT,
    FLAG_COMPRA_VV_15                   INT,
    PERC_COMPRA_MERC_3M                 FLOAT,
    PERC_COMPRA_MERC_6M                 FLOAT,
    PERC_COMPRA_MERC_12M                FLOAT,
    PERC_COMPRA_MERC_15M                FLOAT,
    MAX_PERCSALMIN_VLCOMPRA_CC9M        FLOAT,
    MAX_PERCSALMIN_VLCOMPRA_DD12M       FLOAT,
    MAX_PERCSALMIN_VLCOMPRA_DD15M       FLOAT,
    MAX_PERCSALMIN_VLCOMPRA6M           FLOAT,
    MAX_PERCSALMIN_VLCOMPRA12M          FLOAT,
    MAX_PERCSALMIN_VLCOMPRA15M          FLOAT,
    MAX_PERCSALMIN_VLPARC15M            FLOAT,
    MED_PERCSALMIN_VLCOMPRA_CC3M        FLOAT,
    MED_PERCSALMIN_VLCOMPRA_CC12M       FLOAT,
    MED_PERCSALMIN_VLCOMPRA_DD12M       FLOAT,
    MED_PERCSALMIN_VLCOMPRA3M           FLOAT,
    MED_PERCSALMIN_VLCOMPRA12M          FLOAT,
    MED_PERCSALMIN_VLCOMPRA15M          FLOAT,
    PERC_COMPRA_SERV_3M                 FLOAT,
    PERC_COMPRA_SERV_6M                 FLOAT,
    PERC_VLCOMPRA_MERC_3M               FLOAT,
    PERC_VLCOMPRA_MERC_6M               FLOAT,
    PERC_VLCOMPRA_MERC_9M               FLOAT,
    PERC_VLCOMPRA_MERC_12M              FLOAT,
    PERC_VLCOMPRA_MERC_15M              FLOAT,
    PERC_VLCOMPRA_SERV_3M               FLOAT,
    PERC_VLCOMPRA_SERV_6M               FLOAT,
    PERC_VLCOMPRA_SERV_9M               FLOAT,
    PERC_VLCOMPRA_SERV_12M              FLOAT,
    PERC_VLPAG_CC_12M                   FLOAT,
    PERC_VLPAG_CC_15M                   FLOAT,
    PERC_VLPAG_DD_9M                    FLOAT,
    PERC_VLPAG_DD_12M                   FLOAT,
    PERC_VLPAG_DD_15M                   FLOAT,
    PERCSALMIN_TOT_PAG12M               FLOAT,
    PERCSALMIN_TOT_PAG15M               FLOAT,
    VALORT_PG_CC_1                      FLOAT,
    VALORT_PG_CC_2                      FLOAT,
    VALORT_PG_CC_3                      FLOAT,
    VALORT_PG_CC_4                      FLOAT,
    VALORT_PG_CC_5                      FLOAT,
    VALORT_PG_CC_6                      FLOAT,
    VALORT_PG_CC_7                      FLOAT,
    VALORT_PG_CC_8                      FLOAT,
    VALORT_PG_CC_9                      FLOAT,
    VALORT_PG_CC_10                     FLOAT,
    VALORT_PG_CC_11                     FLOAT,
    VALORT_PG_CC_12                     FLOAT,
    VALORT_PG_CC_13                     FLOAT,
    VALORT_PG_CC_14                     FLOAT,
    VALORT_PG_CC_15                     FLOAT,
    VALOR_PARC_CC_1                     FLOAT,
    VALOR_PARC_CC_2                     FLOAT,
    VALOR_PARC_CC_3                     FLOAT,
    VALOR_PARC_CC_4                     FLOAT,
    VALOR_PARC_CC_5                     FLOAT,
    VALOR_PARC_CC_6                     FLOAT,
    VALOR_PARC_CC_7                     FLOAT,
    VALOR_PARC_CC_8                     FLOAT,
    VALOR_PARC_CC_9                     FLOAT,
    VALOR_PARC_CC_10                    FLOAT,
    VALOR_PARC_CC_11                    FLOAT,
    VALOR_PARC_CC_12                    FLOAT,
    VALOR_PARC_CC_13                    FLOAT,
    VALOR_PARC_CC_14                    FLOAT,
    VALOR_PARC_CC_15                    FLOAT,
    SALARIOMINIMO_1                     BIGINT,
    SALARIOMINIMO_2                     BIGINT,
    SALARIOMINIMO_3                     BIGINT,
    SALARIOMINIMO_4                     BIGINT,
    SALARIOMINIMO_5                     BIGINT,
    SALARIOMINIMO_6                     BIGINT,
    SALARIOMINIMO_7                     BIGINT,
    SALARIOMINIMO_8                     BIGINT,
    SALARIOMINIMO_9                     BIGINT,
    SALARIOMINIMO_10                    BIGINT,
    SALARIOMINIMO_11                    BIGINT,
    SALARIOMINIMO_12                    BIGINT,
    SALARIOMINIMO_13                    BIGINT,
    SALARIOMINIMO_14                    BIGINT,
    SALARIOMINIMO_15                    BIGINT,
    QT_COMPRAS_1                        BIGINT,
    QT_COMPRAS_2                        BIGINT,
    QT_COMPRAS_3                        BIGINT,
    QT_COMPRAS_4                        BIGINT,
    QT_COMPRAS_5                        BIGINT,
    QT_COMPRAS_6                        BIGINT,
    QT_COMPRAS_7                        BIGINT,
    QT_COMPRAS_8                        BIGINT,
    QT_COMPRAS_9                        BIGINT,
    QT_COMPRAS_10                       BIGINT,
    QT_COMPRAS_11                       BIGINT,
    QT_COMPRAS_12                       BIGINT,
    QT_COMPRAS_13                       BIGINT,
    QT_COMPRAS_14                       BIGINT,
    QT_COMPRAS_15                       BIGINT,
    VL_MED_PAG_PARC_15M                 FLOAT,
    PERC_QT_CPR_VV_6M_7A9M              FLOAT,
    PERC_QT_CPR_VV_9M_10A12M            FLOAT,
    PERC_QT_CPR_VV_12M_13A15M           FLOAT,
    MED_PERCSALMIN_VV_VLPARC6M          FLOAT,
    MED_PERCSALMIN_VV_VLPARC12M         FLOAT,
    PERC_COMPRA_DIN_15M                 FLOAT,
    FORMA_PGT_12M                       STRING,
    FORMA_PGT_15M                       STRING,
    QT_MESES_COMPROU_VV_12M             INT,
    QT_MESES_COMPROU_VV_15M             INT,
    RECORRENCIA_VV                      STRING,
    PERTENCE_VAV                        INT,
    DATREF_PREJ                         INT,
    DTH_PROC_CARGA_PREJ                 BIGINT,
    CD_CLI_PREJ                         BIGINT,
    FLAG_PREJUIZO_EVER                  INT,
    PERTENCE_PREJ                       INT,
    DATREF_ECOM                         INT,
    DTH_PROC_CARGA_ECOM                 BIGINT,
    CD_CLI_ECOM                         BIGINT,
    HIST_ONLINE24M                      INT,
    QT_FREQ_ONLINE_24M                  BIGINT,
    PERTENCE_ECOM                       INT,
    DATREF_SEG                          INT,
    DTH_PROC_CARGA_SEG                  BIGINT,
    CD_CLI_SEG                          BIGINT,
    CONT_HISTORICO_ALL60                INT,
    CONT_HISTORICO_CURTO                INT,
    CONT_HISTORICO_LONGO                INT,
    TEMPO_SHIST                         STRING,
    PERTENCE_SEG                        INT
)
PARTITIONED BY (dt_particao bigint)
STORED AS PARQUET
LOCATION 's3://bkt-api-viavarejo-prd/report/target/turing_prd_scoreapi_usage/tb_crediario_cdc_input/';