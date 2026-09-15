-- Oracle 19c+
-- Run with an account allowed to create objects in schema VSS_ODS.

CREATE SEQUENCE VSS_ODS.SEQ_RAW_QTTG_BHXH
    START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;

CREATE TABLE VSS_ODS.RAW_QTTG_BHXH (
    ID                         NUMBER(19)         NOT NULL,
    NLD_ID                     NUMBER(19)         NOT NULL,
    SO_SO_BHXH                 VARCHAR2(50 CHAR),
    THANG_BD                   VARCHAR2(20 CHAR),
    THANG_KT                   VARCHAR2(20 CHAR),
    TT_TG_BHXH                 VARCHAR2(200 CHAR),
    DT_TG_BHXH                 VARCHAR2(200 CHAR),
    NAM_TG_BHXH                NUMBER(5),
    THANG_TG_BHXH              NUMBER(5),
    NAM_TG_BHXH_BB             NUMBER(5),
    THANG_TG_BHXH_BB           NUMBER(5),
    TT_TG_BHTN                 VARCHAR2(200 CHAR),
    DT_TG_BHTN                 VARCHAR2(200 CHAR),
    NAM_TG_BHTN                NUMBER(5),
    THANG_TG_BHTN              NUMBER(5),
    TT_TG_BHYT                 VARCHAR2(200 CHAR),
    DT_TG_BHYT                 VARCHAR2(200 CHAR),
    NAM_TG_BHYT                NUMBER(5),
    THANG_TG_BHYT              NUMBER(5),
    NAM_NO_BHXH                NUMBER(5),
    THANG_NO_BHXH              NUMBER(5),
    NAM_NO_BHTN                NUMBER(5),
    THANG_NO_BHTN              NUMBER(5),
    TT_TG_BH                   VARCHAR2(200 CHAR),
    DT_TG_BH                   VARCHAR2(200 CHAR),
    TU_THANG_DVI               VARCHAR2(20 CHAR),
    DEN_THANG_DVI              VARCHAR2(20 CHAR),
    DEN_THANG_HTTT             VARCHAR2(20 CHAR),
    DEN_THANG_BHTN             VARCHAR2(20 CHAR),
    THANG_BD_LT                VARCHAR2(20 CHAR),
    THANG_KT_LT                VARCHAR2(20 CHAR),
    SO_THANG_LT                NUMBER(5),
    IS_ERRORS                  NUMBER(1),
    NGHI_VIEC                  NUMBER(1),
    IS_CONTINUE                NUMBER(1),
    TRUY_DONG                  NUMBER(1),
    DEN_NGAY                   VARCHAR2(20 CHAR),
    MA_CD                      VARCHAR2(50 CHAR),
    MA_NHH                     VARCHAR2(50 CHAR),
    DD_MA_DON_VI               VARCHAR2(50 CHAR),
    DD_THANG_DONG_DEN_XH       VARCHAR2(20 CHAR),
    DD_TY_LE_NO_BHXH           NUMBER(10,4),
    DD_THANG_DONG_DEN_YT       VARCHAR2(20 CHAR),
    DD_TY_LE_NO_BHYT           NUMBER(10,4),
    DD_THANG_DONG_DEN_TN       VARCHAR2(20 CHAR),
    DD_TY_LE_NO_BHTN           NUMBER(10,4),
    DD_THANG_DONG_DEN_TNLD     VARCHAR2(20 CHAR),
    DD_TY_LE_NO_TNLD           NUMBER(10,4),
    RAW_RESPONSE               CLOB,
    CREATED_AT                 TIMESTAMP(6) DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT PK_RAW_QTTG_BHXH PRIMARY KEY (ID)
);

CREATE INDEX VSS_ODS.IDX_RAW_QTTG_BHXH_NLD
    ON VSS_ODS.RAW_QTTG_BHXH (NLD_ID);

COMMENT ON TABLE VSS_ODS.RAW_QTTG_BHXH IS
    'Master raw API 2.3 type=BHXH - flatten thangDongDen vao cac cot DD_*';
