SET ECHO OFF
column dt new_val timestring
SET TERM OFF
select to_char(sysdate, 'YYYYMMDD-HH24MISS') dt from dual;
SET TERM ON
spool D:\Users\Tzeyang\MNP_INSERTION_TEST_LOG2..sql
SET SERVEROUTPUT ON
DECLARE
    ARC_ROW_COUNT NUMBER;
    PR_ROW_COUNT NUMBER;
BEGIN
/********************NPCDB.XNP_SUBSCRIBER********************/
/*Records arranged to be insert*/
    SELECT COUNT(ROWNUM) INTO PR_ROW_COUNT
    FROM NPCDB.XNP_SUBSCRIBER_ID@NPCDBTB
        WHERE /*Converting PORT_ID into date using substring before comparing*/
        (
            TO_DATE
                (
                    SUBSTR(PORT_ID,2,4) || '-' || SUBSTR(PORT_ID,6,2) || '-' || SUBSTR(PORT_ID,8,2),'YYYY-MM-DD'
                )
        )> 
        (
            SELECT MAX
                (
                    TO_DATE
                        (
                            (
                                SUBSTR(PORT_ID,2,4) || '-' || SUBSTR(PORT_ID,6,2) || '-' || SUBSTR(PORT_ID,8,2)
                            ),'YYYY-MM-DD'
                        )
                )
            FROM NPCDB.XNP_SUBSCRIBER_ID
        )
        AND
        (
            TO_DATE
                (
                    SUBSTR(PORT_ID,2,4) || '-' || SUBSTR(PORT_ID,6,2) || '-' || SUBSTR(PORT_ID,8,2),'YYYY-MM-DD'
                )
        )<=
        (
            SELECT MAX
                (TO_DATE
                    (
                        (
                            SUBSTR(PORT_ID,2,4) || '-' || SUBSTR(PORT_ID,6,2) || '-' || SUBSTR(PORT_ID,8,2)
                        ),'YYYY-MM-DD'
                    )+30 /*specify the days interval here*/
                )
            FROM NPCDB.XNP_SUBSCRIBER_ID 
        );
    DBMS_OUTPUT.PUT_LINE(to_char(sysdate, 'YYYY-MM-DD_HH24:MI:SS')||' QUERIES EXECUTED ON NPCDB.XNP_SUBSCRIBER_ID  ');
    DBMS_OUTPUT.PUT_LINE(PR_ROW_COUNT||' ROWS ARRANGED IN NPCDB.XNP_SUBSCRIBER');
/*Insert the records*/ 
    INSERT INTO NPCDB.XNP_SUBSCRIBER_ID
        (PORT_ID,
        COMPANY_NAME,
        COMPANY_REG_NUM,
        ACCOUNT_NUM,
        CUSTOMER_NAME,
        SIM_CARD_NUM,
        NIC,
        CNIC,
        ARMED_FORCES_ID,
        PASSPORT_NUM,
        DATE_OF_BIRTH,
        CONTACT_PHONE,
        FAX,
        CITY,
        STREET,
        NUM,
        LOCALITY,
        POSTCODE)    
        SELECT * FROM NPCDB.XNP_SUBSCRIBER_ID@NPCDBTB
    WHERE /*Converting PORT_ID into date using substring before comparing*/
    (
        TO_DATE
            (
                SUBSTR(PORT_ID,2,4) || '-' || SUBSTR(PORT_ID,6,2) || '-' || SUBSTR(PORT_ID,8,2),'YYYY-MM-DD'
            )
    )> 
    (
        SELECT MAX
            (
                TO_DATE
                    (
                        (
                            SUBSTR(PORT_ID,2,4) || '-' || SUBSTR(PORT_ID,6,2) || '-' || SUBSTR(PORT_ID,8,2)
                        ),'YYYY-MM-DD'
                    )
            )
        FROM NPCDB.XNP_SUBSCRIBER_ID
    )
    AND
    (
        TO_DATE
            (
                SUBSTR(PORT_ID,2,4) || '-' || SUBSTR(PORT_ID,6,2) || '-' || SUBSTR(PORT_ID,8,2),'YYYY-MM-DD'
            )
    )<=
    (
        SELECT MAX
            (TO_DATE
                (
                    (
                        SUBSTR(PORT_ID,2,4) || '-' || SUBSTR(PORT_ID,6,2) || '-' || SUBSTR(PORT_ID,8,2)
                    ),'YYYY-MM-DD'
                )+30 /*specify the days interval here*/
            )
        FROM NPCDB.XNP_SUBSCRIBER_ID 
    );
    ARC_ROW_COUNT:=SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE(ARC_ROW_COUNT||' ROWS AFFECTED IN NPCDB.XNP_SUBSCRIBER');
/*Insertion Validation*/
    IF ARC_ROW_COUNT = PR_ROW_COUNT THEN
        DBMS_OUTPUT.PUT_LINE('RECORDS MATCHED IN NPCDB.XNP_SUBSCRIBER');
        DBMS_OUTPUT.PUT_LINE('');
    ELSE
        DBMS_OUTPUT.PUT_LINE('RECORDS NOT MATCHED ,PROCEED ROLLBACK IN NPCDB.XNP_SUBSCRIBER');
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('');
    END IF;


/********************NPCDB.XNP_REGION_CODE********************/
/*Records arranged to be insert*/
    SELECT COUNT(ROWNUM) INTO PR_ROW_COUNT 
    FROM NPCDB.XNP_REGION_CODE@NPCDBTB SOURCE
    LEFT JOIN NPCDB.XNP_REGION_CODE DEST
    ON SOURCE.REGION_CODE = DEST.REGION_CODE
        WHERE 
            DEST.REGION_CODE IS NULL
        OR 
            (
                SOURCE.REGION_CODE = DEST.REGION_CODE 
                AND SOURCE.REGION_NAME != DEST.REGION_NAME
            );
    DBMS_OUTPUT.PUT_LINE(to_char(sysdate, 'YYYY-MM-DD_HH24:MI:SS')||' QUERIES EXECUTED ON NPCDB.XNP_REGION_CODE  ');
    DBMS_OUTPUT.PUT_LINE (PR_ROW_COUNT||' ROWS ARRANGED IN NPCDB.XNP_REGION_CODE');
/*Insert the records*/    
    MERGE INTO NPCDB.XNP_REGION_CODE DEST
    USING NPCDB.XNP_REGION_CODE SOURCE
    ON (DEST.REGION_CODE = SOURCE.REGION_CODE)
    WHEN MATCHED THEN
    UPDATE
    SET
        DEST.REGION_NAME = SOURCE.REGION_NAME
            WHERE
                SOURCE.REGION_CODE = DEST.REGION_CODE 
                AND SOURCE.REGION_NAME != DEST.REGION_NAME
    WHEN NOT MATCHED THEN
    INSERT VALUES
        (SOURCE.REGION_CODE, SOURCE.REGION_NAME);   
    ARC_ROW_COUNT:=SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE(ARC_ROW_COUNT || ' AFFECTED IN NPCDB.XNP_REGION_CODE');
/*Insertion Validation*/
    IF ARC_ROW_COUNT = PR_ROW_COUNT THEN
        DBMS_OUTPUT.PUT_LINE('RECORDS MATCHED IN NPCDB.XNP_REGION_CODE');
        DBMS_OUTPUT.PUT_LINE('');
    ELSE
        DBMS_OUTPUT.PUT_LINE('RECORDS NOT MATCHED ,PROCEED ROLLBACK IN NPCDB.XNP_REGION_CODE');
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('');
    END IF;


/********************NPCDB.XNP_CAUSE_CODE********************/
/*Records arranged to be insert*/
    SELECT COUNT(ROWNUM) INTO PR_ROW_COUNT 
    FROM NPCDB.XNP_CAUSE_CODE@NPCDBTB SOURCE
    LEFT JOIN NPCDB.XNP_CAUSE_CODE DEST
    ON SOURCE.CAUSE_ID = DEST.CAUSE_ID
        WHERE 
            DEST.CAUSE_ID IS NULL
        OR 
            (
                SOURCE.CAUSE_ID = DEST.CAUSE_ID
                AND SOURCE.CAUSE_TYPE != DEST.CAUSE_TYPE
            )
        OR
            (
                SOURCE.CAUSE_ID = DEST.CAUSE_ID
                AND SOURCE.CAUSE_TEXT != DEST.CAUSE_TEXT
            )
        OR
            (
                SOURCE.CAUSE_ID = DEST.CAUSE_ID
                AND SOURCE.DISPLAY != DEST.DISPLAY
            );
    DBMS_OUTPUT.PUT_LINE(to_char(sysdate, 'YYYY-MM-DD_HH24:MI:SS')||' QUERIES EXECUTED ON NPCDB.XNP_CAUSE_CODE ');
    DBMS_OUTPUT.PUT_LINE (PR_ROW_COUNT||' ROWS ARRANGED IN NPCDB.XNP_CAUSE_CODE');
/*Insert the records*/
    MERGE INTO NPCDB.XNP_CAUSE_CODE DEST
    USING NPCDB.XNP_CAUSE_CODE@NPCDBTB SOURCE
    ON (DEST.CAUSE_ID = SOURCE.CAUSE_ID)
    WHEN MATCHED THEN
    UPDATE
    SET
        DEST.CAUSE_TYPE = SOURCE.CAUSE_TYPE,
        DEST.CAUSE_TEXT = SOURCE.CAUSE_TEXT,
        DEST.DISPLAY = SOURCE.DISPLAY
            WHERE   
                (
                    SOURCE.CAUSE_ID = DEST.CAUSE_ID
                    AND SOURCE.CAUSE_TYPE != DEST.CAUSE_TYPE
                )
            OR
                (
                    SOURCE.CAUSE_ID = DEST.CAUSE_ID
                    AND SOURCE.CAUSE_TEXT != DEST.CAUSE_TEXT
                )
            OR
                (
                    SOURCE.CAUSE_ID = DEST.CAUSE_ID
                    AND SOURCE.DISPLAY != DEST.DISPLAY
                )
    WHEN NOT MATCHED THEN
    INSERT VALUES
        (
            SOURCE.CAUSE_ID,
            SOURCE.CAUSE_TYPE,
            SOURCE.CAUSE_TEXT,
            SOURCE.DISPLAY
        );
    ARC_ROW_COUNT := SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE(ARC_ROW_COUNT || ' AFFECTED IN NPCDB.XNP_CAUSE_CODE');
/*Insertion Validation*/
    IF ARC_ROW_COUNT = PR_ROW_COUNT THEN
        DBMS_OUTPUT.PUT_LINE('RECORDS MATCHED IN NPCDB.XNP_CAUSE_CODE');
        DBMS_OUTPUT.PUT_LINE('');
    ELSE
        DBMS_OUTPUT.PUT_LINE('RECORDS NOT MATCHED ,PROCEED ROLLBACK IN NPCDB.XNP_CAUSE_CODE');
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('');
    END IF;  
    

/********************NPCDB.XNP_SERVICE_TYPE********************/
/*Records arranged to be insert*/
    SELECT COUNT(ROWNUM) INTO PR_ROW_COUNT 
    FROM NPCDB.XNP_SERVICE_TYPES@NPCDBTB SOURCE
    LEFT JOIN NPCDB.XNP_SERVICE_TYPES DEST
    ON SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
        WHERE 
            DEST.SERVICE_TYPE IS NULL
        OR 
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.DESCRIPTION != DEST.DESCRIPTION
            );
    DBMS_OUTPUT.PUT_LINE(to_char(sysdate, 'YYYY-MM-DD_HH24:MI:SS')||' QUERIES EXECUTED ON NPCDB.XNP_SERVICE_TYPES');
    DBMS_OUTPUT.PUT_LINE (PR_ROW_COUNT||' ROWS ARRANGED IN NPCDB.XNP_SERVICE_TYPES');
/*Insert the records*/        
    MERGE INTO NPCDB.XNP_SERVICE_TYPES DEST
    USING NPCDB.XNP_SERVICE_TYPES@NPCDBTB SOURCE
    ON (DEST.SERVICE_TYPE = SOURCE.SERVICE_TYPE)
    WHEN MATCHED THEN
    UPDATE
    SET
        DEST.DESCRIPTION = SOURCE.DESCRIPTION
        WHERE
            SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
            AND SOURCE.DESCRIPTION != DEST.DESCRIPTION
    WHEN NOT MATCHED THEN
    INSERT VALUES
        (SOURCE.SERVICE_TYPE,
        SOURCE.DESCRIPTION);
    ARC_ROW_COUNT := SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE(ARC_ROW_COUNT || ' AFFECTED IN NPCDB.XNP_SERVICE_TYPES');
/*Insertion Validation*/
    IF ARC_ROW_COUNT = PR_ROW_COUNT THEN
        DBMS_OUTPUT.PUT_LINE('RECORDS MATCHED IN NPCDB.XNP_SERVICE_TYPES');
        DBMS_OUTPUT.PUT_LINE('');
    ELSE
        DBMS_OUTPUT.PUT_LINE('RECORDS NOT MATCHED ,PROCEED ROLLBACK IN NPCDB.XNP_SERVICE_TYPES');
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('');
    END IF; 
    
    
/********************NPCDB.XNP_MSG_TYPE********************/
/*Records arranged to be insert*/
    SELECT COUNT(ROWNUM) INTO PR_ROW_COUNT 
    FROM NPCDB.XNP_MSG_TYPE@NPCDBTB SOURCE
    LEFT JOIN NPCDB.XNP_MSG_TYPE DEST
    ON SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
        WHERE 
            DEST.SERVICE_TYPE IS NULL
        OR 
            DEST.MSG_TYPE IS NULL
        OR
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.DESCRIPTION != DEST.DESCRIPTION
            )
        OR
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.FLOW != DEST.FLOW
            )
        OR
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.FIRST_IN_FLOW != DEST.FIRST_IN_FLOW
            )
        OR
            (   SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.RECV_FROM != DEST.RECV_FROM
            )
        OR
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.INIT_STATE1 != DEST.INIT_STATE1
            )
        OR
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.INIT_STATE2 != DEST.INIT_STATE2
            )
        OR
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.CHECK_TIMER_1 != DEST.CHECK_TIMER_1
            )
        OR
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.CHECK_TIMER_2 != DEST.CHECK_TIMER_2
            )
        OR
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.CHECK_DUE_DATE != DEST.CHECK_DUE_DATE
            )
        OR
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.VALIDATE_RANGE_OWNER != DEST.VALIDATE_RANGE_OWNER
            )
        OR
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.TO_RECIP_ACC != DEST.TO_RECIP_ACC
            )
        OR
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.TO_DONOR_ACC != DEST.TO_DONOR_ACC
            )
        OR
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.TO_ORIG_ACC != DEST.TO_ORIG_ACC
            )
        OR
            (  
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.TO_ALL_ACC != DEST.TO_ALL_ACC
            )
        OR
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.TO_ORIG_REJ != DEST.TO_ORIG_REJ
            )
        OR
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.NEXT_STATE_ACC != DEST.NEXT_STATE_ACC
            )
        OR
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.NEXT_STATE_REJ != DEST.NEXT_STATE_REJ
            )
        OR
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.SET_TIMER != DEST.SET_TIMER
            )
        OR 
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.REJ_CODE_REQD != DEST.REJ_CODE_REQD
            )
        OR
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.DESTINATION_DIRECTION != DEST.DESTINATION_DIRECTION
            )
        OR
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.PHASE != DEST.PHASE
            )
        OR
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.CATEGORY != DEST.CATEGORY
            )
        OR
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.DEL_SUBSCRIBER != DEST.DEL_SUBSCRIBER
            )
        OR
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.MOVE_PORT_FORWARD != DEST.MOVE_PORT_FORWARD
            )
        OR
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.UPDATE_DUE_DATE != DEST.UPDATE_DUE_DATE
            )
        OR
            (
                SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                AND SOURCE.ACTION_REQUIRED != DEST.ACTION_REQUIRED
            );
    DBMS_OUTPUT.PUT_LINE(to_char(sysdate, 'YYYY-MM-DD_HH24:MI:SS')||' QUERIES EXECUTED ON NPCDB.XNP_MSG_TYPE');
    DBMS_OUTPUT.PUT_LINE (PR_ROW_COUNT||' ROWS ARRANGED IN NPCDB.XNP_MSG_TYPE');
    /*Insert query for XNP_MSG_TYPE*/
    MERGE INTO NPCDB.XNP_MSG_TYPE DEST
    USING NPCDB.XNP_MSG_TYPE@NPCDBTB SOURCE
    ON (DEST.SERVICE_TYPE = SOURCE.SERVICE_TYPE AND DEST.MSG_TYPE = SOURCE.MSG_TYPE)
    WHEN MATCHED THEN
    UPDATE
    SET
        DEST.DESCRIPTION = SOURCE.DESCRIPTION,
        DEST.FLOW = SOURCE.FLOW,
        DEST.FIRST_IN_FLOW = SOURCE.FIRST_IN_FLOW,
        DEST.RECV_FROM = SOURCE.RECV_FROM,
        DEST.INIT_STATE1 = SOURCE.INIT_STATE1,
        DEST.INIT_STATE2 = SOURCE.INIT_STATE2,
        DEST.CHECK_TIMER_1 = SOURCE.CHECK_TIMER_1,
        DEST.CHECK_TIMER_2 = SOURCE.CHECK_TIMER_2,
        DEST.CHECK_DUE_DATE = SOURCE.CHECK_DUE_DATE,
        DEST.VALIDATE_RANGE_OWNER = SOURCE.VALIDATE_RANGE_OWNER,
        DEST.TO_RECIP_ACC = SOURCE.TO_RECIP_ACC,
        DEST.TO_DONOR_ACC = SOURCE.TO_DONOR_ACC,
        DEST.TO_ORIG_ACC = SOURCE.TO_ORIG_ACC,
        DEST.TO_ALL_ACC = SOURCE.TO_ALL_ACC,
        DEST.TO_ORIG_REJ = SOURCE.TO_ORIG_REJ,
        DEST.NEXT_STATE_ACC = SOURCE.NEXT_STATE_ACC,
        DEST.NEXT_STATE_REJ = SOURCE.NEXT_STATE_REJ,
        DEST.SET_TIMER = SOURCE.SET_TIMER,
        DEST.REJ_CODE_REQD = SOURCE.REJ_CODE_REQD,
        DEST.DESTINATION_DIRECTION = SOURCE.DESTINATION_DIRECTION,
        DEST.PHASE = SOURCE.PHASE,
        DEST.CATEGORY = SOURCE.CATEGORY,
        DEST.DEL_SUBSCRIBER = SOURCE.DEL_SUBSCRIBER,
        DEST.MOVE_PORT_FORWARD = SOURCE.MOVE_PORT_FORWARD,
        DEST.UPDATE_DUE_DATE = SOURCE.UPDATE_DUE_DATE,
        DEST.ACTION_REQUIRED = SOURCE.ACTION_REQUIRED
        WHERE
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.DESCRIPTION != DEST.DESCRIPTION
                )
            OR
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.FLOW != DEST.FLOW
                )
            OR
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.FIRST_IN_FLOW != DEST.FIRST_IN_FLOW
                )
            OR
                (   SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.RECV_FROM != DEST.RECV_FROM
                )
            OR
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.INIT_STATE1 != DEST.INIT_STATE1
                )
            OR
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.INIT_STATE2 != DEST.INIT_STATE2
                )
            OR
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.CHECK_TIMER_1 != DEST.CHECK_TIMER_1
                )
            OR
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.CHECK_TIMER_2 != DEST.CHECK_TIMER_2
                )
            OR
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.CHECK_DUE_DATE != DEST.CHECK_DUE_DATE
                )
            OR
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.VALIDATE_RANGE_OWNER != DEST.VALIDATE_RANGE_OWNER
                )
            OR
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.TO_RECIP_ACC != DEST.TO_RECIP_ACC
                )
            OR
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.TO_DONOR_ACC != DEST.TO_DONOR_ACC
                )
            OR
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.TO_ORIG_ACC != DEST.TO_ORIG_ACC
                )
            OR
                (  
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.TO_ALL_ACC != DEST.TO_ALL_ACC
                )
            OR
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.TO_ORIG_REJ != DEST.TO_ORIG_REJ
                )
            OR
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.NEXT_STATE_ACC != DEST.NEXT_STATE_ACC
                )
            OR
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.NEXT_STATE_REJ != DEST.NEXT_STATE_REJ
                )
            OR
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.SET_TIMER != DEST.SET_TIMER
                )
            OR 
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.REJ_CODE_REQD != DEST.REJ_CODE_REQD
                )
            OR
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.DESTINATION_DIRECTION != DEST.DESTINATION_DIRECTION
                )
            OR
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.PHASE != DEST.PHASE
                )
            OR
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.CATEGORY != DEST.CATEGORY
                )
            OR
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.DEL_SUBSCRIBER != DEST.DEL_SUBSCRIBER
                )
            OR
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.MOVE_PORT_FORWARD != DEST.MOVE_PORT_FORWARD
                )
            OR
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.UPDATE_DUE_DATE != DEST.UPDATE_DUE_DATE
                )
            OR
                (
                    SOURCE.SERVICE_TYPE = DEST.SERVICE_TYPE
                    AND SOURCE.MSG_TYPE = DEST.MSG_TYPE
                    AND SOURCE.ACTION_REQUIRED != DEST.ACTION_REQUIRED
                )
    WHEN NOT MATCHED THEN
    INSERT VALUES
        (
            SOURCE.SERVICE_TYPE,
            SOURCE.MSG_TYPE,
            SOURCE.DESCRIPTION,
            SOURCE.FLOW,
            SOURCE.FIRST_IN_FLOW,
            SOURCE.RECV_FROM,
            SOURCE.INIT_STATE1,
            SOURCE.INIT_STATE2,
            SOURCE.CHECK_TIMER_1,
            SOURCE.CHECK_TIMER_2,
            SOURCE.CHECK_DUE_DATE,
            SOURCE.VALIDATE_RANGE_OWNER,
            SOURCE.TO_RECIP_ACC,
            SOURCE.TO_DONOR_ACC,
            SOURCE.TO_ORIG_ACC,
            SOURCE.TO_ALL_ACC,
            SOURCE.TO_ORIG_REJ,
            SOURCE.NEXT_STATE_ACC,
            SOURCE.NEXT_STATE_REJ,
            SOURCE.SET_TIMER,
            SOURCE.REJ_CODE_REQD,
            SOURCE.DESTINATION_DIRECTION,
            SOURCE.PHASE,
            SOURCE.CATEGORY,
            SOURCE.DEL_SUBSCRIBER,
            SOURCE.MOVE_PORT_FORWARD,
            SOURCE.UPDATE_DUE_DATE,
            SOURCE.ACTION_REQUIRED
        );
    ARC_ROW_COUNT := SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE(ARC_ROW_COUNT || ' AFFECTED IN NPCDB.XNP_MSG_TYPE');
    /*Insertion Validation*/
    IF ARC_ROW_COUNT = PR_ROW_COUNT THEN
        DBMS_OUTPUT.PUT_LINE('RECORDS MATCHED IN NPCDB.XNP_MSG_TYPE');
        DBMS_OUTPUT.PUT_LINE('');
    ELSE
        DBMS_OUTPUT.PUT_LINE('RECORDS NOT MATCHED ,PROCEED ROLLBACK IN NPCDB.XNP_MSG_TYPE');
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('');
    END IF;
    
 
 
/********************NPCDB.XNP_PARTICIPANT********************/
/*Records arranged to be insert*/
    SELECT COUNT(ROWNUM) INTO PR_ROW_COUNT 
    FROM NPCDB.XNP_PARTICIPANT@NPCDBTB SOURCE
    LEFT JOIN NPCDB.XNP_PARTICIPANT DEST
    ON SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID
        WHERE 
            DEST.PARTICIPANT_ID IS NULL
        OR 
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.PARTICIPANT_NAME != DEST.PARTICIPANT_NAME
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.PARTICIPANT_TYPE != DEST.PARTICIPANT_TYPE
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.PERSON != DEST.PERSON
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.PHONE_1 != DEST.PHONE_1
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.PHONE_2 != DEST.PHONE_2
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.MOBILE != DEST.MOBILE
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.DEPARTMENT != DEST.DEPARTMENT
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.POSTAL != DEST.POSTAL
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.CITY != DEST.CITY
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.COUNTRY != DEST.COUNTRY
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.FAX != DEST.FAX
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.E_MAIL != DEST.E_MAIL
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.STATUS != DEST.STATUS
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.AUDIT_OUTPUT_TYPE != DEST.AUDIT_OUTPUT_TYPE
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.UNSOLICITED_MSG_COM != DEST.UNSOLICITED_MSG_COM
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.UPDATED_BY = DEST.UPDATED_BY
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.UPDATE_TIME != DEST.UPDATE_TIME
            );
    DBMS_OUTPUT.PUT_LINE(to_char(sysdate, 'YYYY-MM-DD_HH24:MI:SS')||' QUERIES EXECUTED ON NPCDB.XNP_PARTICIPANT');
    DBMS_OUTPUT.PUT_LINE (PR_ROW_COUNT||' ROWS ARRANGED IN NPCDB.XNP_PARTICIPANT');
/*Insert the records*/
    MERGE INTO NPCDB.XNP_PARTICIPANT DEST
    USING NPCDB.XNP_PARTICIPANT@NPCDBTB SOURCE
    ON (DEST.PARTICIPANT_ID = SOURCE.PARTICIPANT_ID)
    WHEN MATCHED THEN
    UPDATE
    SET
        DEST.PARTICIPANT_NAME = SOURCE.PARTICIPANT_NAME, 
        DEST.PARTICIPANT_TYPE = SOURCE.PARTICIPANT_TYPE, 
        DEST.PERSON = SOURCE.PERSON, 
        DEST.PHONE_1 = SOURCE.PHONE_1, 
        DEST.PHONE_2 = SOURCE.PHONE_2,
        DEST.MOBILE = SOURCE.MOBILE, 
        DEST.DEPARTMENT = SOURCE.DEPARTMENT, 
        DEST.ADDRESS = SOURCE.ADDRESS, 
        DEST.POSTAL = SOURCE.POSTAL, 
        DEST.CITY = SOURCE.CITY, 
        DEST.COUNTRY = SOURCE.COUNTRY, 
        DEST.FAX = SOURCE.FAX, 
        DEST.E_MAIL = SOURCE.E_MAIL, 
        DEST.STATUS = SOURCE.STATUS, 
        DEST.AUDIT_OUTPUT_TYPE = SOURCE.AUDIT_OUTPUT_TYPE, 
        DEST.UNSOLICITED_MSG_COM = SOURCE.UNSOLICITED_MSG_COM, 
        DEST.UPDATED_BY = SOURCE.UPDATED_BY, 
        DEST.UPDATE_TIME = SOURCE.UPDATE_TIME
        WHERE
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.PARTICIPANT_NAME != DEST.PARTICIPANT_NAME
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.PARTICIPANT_TYPE != DEST.PARTICIPANT_TYPE
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.PERSON != DEST.PERSON
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.PHONE_1 != DEST.PHONE_1
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.PHONE_2 != DEST.PHONE_2
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.MOBILE != DEST.MOBILE
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.DEPARTMENT != DEST.DEPARTMENT
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.POSTAL != DEST.POSTAL
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.CITY != DEST.CITY
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.COUNTRY != DEST.COUNTRY
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.FAX != DEST.FAX
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.E_MAIL != DEST.E_MAIL
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.STATUS != DEST.STATUS
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.AUDIT_OUTPUT_TYPE != DEST.AUDIT_OUTPUT_TYPE
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.UNSOLICITED_MSG_COM != DEST.UNSOLICITED_MSG_COM
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.UPDATED_BY = DEST.UPDATED_BY
            )
        OR
            (
                SOURCE.PARTICIPANT_ID = DEST.PARTICIPANT_ID 
                AND SOURCE.UPDATE_TIME != DEST.UPDATE_TIME
            )
    WHEN NOT MATCHED THEN
         INSERT VALUES
         (SOURCE.PARTICIPANT_ID, 
         SOURCE.PARTICIPANT_NAME, 
         SOURCE.PARTICIPANT_TYPE, 
         SOURCE.PERSON, 
         SOURCE.PHONE_1, 
         SOURCE.PHONE_2, 
         SOURCE.MOBILE, 
         SOURCE.DEPARTMENT, 
         SOURCE.ADDRESS, 
         SOURCE.POSTAL, 
         SOURCE.CITY, 
         SOURCE.COUNTRY, 
         SOURCE.FAX, 
         SOURCE.E_MAIL, 
         SOURCE.STATUS, 
         SOURCE.AUDIT_OUTPUT_TYPE, 
         SOURCE.UNSOLICITED_MSG_COM, 
         SOURCE.UPDATED_BY, 
         SOURCE.UPDATE_TIME
         );
         ARC_ROW_COUNT := SQL%ROWCOUNT;
        DBMS_OUTPUT.PUT_LINE(ARC_ROW_COUNT || ' AFFECTED IN NPCDB.XNP_PARTICIPANT');
/*Insertion Validation*/
    IF ARC_ROW_COUNT = PR_ROW_COUNT THEN
        DBMS_OUTPUT.PUT_LINE('RECORDS MATCHED IN NPCDB.XNP_PARTICIPANT');
        DBMS_OUTPUT.PUT_LINE('');
    ELSE
        DBMS_OUTPUT.PUT_LINE('RECORDS NOT MATCHED ,PROCEED ROLLBACK IN NPCDB.XNP_PARTICIPANT');
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('');
    END IF;
    
    
/********************NPCDB.XNP_PORT_DATA********************/ 
/*Records arranged to be insert*/
    SELECT COUNT(ROWNUM)INTO PR_ROW_COUNT FROM NPCDB.XNP_PORT_DATA@NPCDBTB SOURCE
        WHERE SOURCE.CREATE_TIME > (SELECT MAX(CREATE_TIME)FROM NPCDB.XNP_PORT_DATA)
        AND SOURCE.CREATE_TIME <= (SELECT MAX (CREATE_TIME)+30 FROM NPCDB.XNP_PORT_DATA);
    DBMS_OUTPUT.PUT_LINE(to_char(sysdate, 'YYYY-MM-DD_HH24:MI:SS')||' QUERIES EXECUTED ON NPCDB.XNP_PORT_DATA');
    DBMS_OUTPUT.PUT_LINE(PR_ROW_COUNT||' ROWS ARRANGED IN NPCDB.XNP_PORT_DATA');
/*Insert record using merge*/    
    MERGE INTO NPCDB.XNP_PORT_DATA DEST
    USING NPCDB.XNP_PORT_DATA@NPCDBTB SOURCE
    ON (DEST.PORT_ID = SOURCE.PORT_ID)
    WHEN MATCHED THEN
    UPDATE
    SET
        DEST.MSG_TYPE = SOURCE.MSG_TYPE, 
        DEST.SERVICE_TYPE = SOURCE.SERVICE_TYPE,
        DEST.STATUS = SOURCE.STATUS, 
        DEST.STATUS_UPDATE_TIME = SOURCE.STATUS_UPDATE_TIME, 
        DEST.MONTH = SOURCE.MONTH, 
        DEST.RECIPIENT_ID = SOURCE.RECIPIENT_ID, 
        DEST.ORIG_DONOR_ID = SOURCE.ORIG_DONOR_ID, 
        DEST.TRANS_TIME = SOURCE.TRANS_TIME,
        DEST.PORT_DUE_DATE = SOURCE.PORT_DUE_DATE, 
        DEST.REJECT_CODE = SOURCE.REJECT_CODE, 
        DEST.NETWORK_ID = SOURCE.NETWORK_ID, 
        DEST.CHANGE_TIME = SOURCE.CHANGE_TIME, 
        DEST.CHANGED_BY = SOURCE.CHANGED_BY, 
        DEST.CREATE_TIME = SOURCE.CREATE_TIME, 
        DEST.CREATED_BY = SOURCE.CREATED_BY,
        DEST.CREATE_CRDB_TIME = SOURCE.CREATE_CRDB_TIME, 
        DEST.DONOR_ID = SOURCE.DONOR_ID, 
        DEST.UPDATED_BY = SOURCE.UPDATED_BY, 
        DEST.UPDATE_TIME = SOURCE.UPDATE_TIME, 
        DEST.CAUSE_TYPE = SOURCE.CAUSE_TYPE, 
        DEST.TYPE_OF_CONNECTION = SOURCE.TYPE_OF_CONNECTION,
        DEST.LINKED_PORT_ID = SOURCE.LINKED_PORT_ID, 
        DEST.ORIG_DUE_DATE = SOURCE.ORIG_DUE_DATE, 
        DEST.NUM_RANGES = SOURCE.NUM_RANGES, 
        DEST.PORT_REQ_FORM_ID = SOURCE.PORT_REQ_FORM_ID
    WHERE
        SOURCE.CREATE_TIME > (SELECT MAX(CREATE_TIME)FROM NPCDB.XNP_PORT_DATA)
        AND SOURCE.CREATE_TIME <= (SELECT MAX (CREATE_TIME)+30 FROM NPCDB.XNP_PORT_DATA)
    WHEN NOT MATCHED THEN
    INSERT VALUES
        ( 
        SOURCE.PORT_ID,
        SOURCE.MSG_TYPE, 
        SOURCE.SERVICE_TYPE, 
        SOURCE.STATUS, 
        SOURCE.STATUS_UPDATE_TIME, 
        SOURCE.MONTH, 
        SOURCE.RECIPIENT_ID, 
        SOURCE.ORIG_DONOR_ID, 
        SOURCE.TRANS_TIME, 
        SOURCE.PORT_DUE_DATE, 
        SOURCE.REJECT_CODE, 
        SOURCE.NETWORK_ID, 
        SOURCE.CHANGE_TIME, 
        SOURCE.CHANGED_BY, 
        SOURCE.CREATE_TIME, 
        SOURCE.CREATED_BY, 
        SOURCE.CREATE_CRDB_TIME, 
        SOURCE.DONOR_ID, 
        SOURCE.UPDATED_BY, 
        SOURCE.UPDATE_TIME, 
        SOURCE.CAUSE_TYPE, 
        SOURCE.TYPE_OF_CONNECTION, 
        SOURCE.LINKED_PORT_ID, 
        SOURCE.ORIG_DUE_DATE, 
        SOURCE.NUM_RANGES, 
        SOURCE.PORT_REQ_FORM_ID)
    WHERE
        SOURCE.CREATE_TIME > (SELECT MAX(CREATE_TIME)FROM NPCDB.XNP_PORT_DATA)
    AND SOURCE.CREATE_TIME <= (SELECT MAX (CREATE_TIME)+30 FROM NPCDB.XNP_PORT_DATA);
    ARC_ROW_COUNT:=SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE(ARC_ROW_COUNT||' ROWS AFFECTED IN NPCDB.XNP_PORT_DATA');
/*Insertion Validation*/
    IF ARC_ROW_COUNT = PR_ROW_COUNT THEN
        DBMS_OUTPUT.PUT_LINE('RECORDS MATCHED IN NPCDB.XNP_PORT_DATA');
        DBMS_OUTPUT.PUT_LINE('');
    ELSE
        DBMS_OUTPUT.PUT_LINE('RECORDS NOT MATCHED ,PROCEED ROLLBACK IN NPCDB.XNP_PORT_DATA');
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('');
    END IF;

/********************NPCDB.XNP_PORT_MSG********************/ 
/*Records arranged to be insert*/
        SELECT COUNT(ROWNUM)INTO PR_ROW_COUNT FROM NPCDB.XNP_PORT_MSG@NPCDBTB SOURCE
            WHERE SOURCE.CREATE_TIME > (SELECT MAX(CREATE_TIME)FROM NPCDB.XNP_PORT_MSG)
            AND SOURCE.CREATE_TIME <= (SELECT MAX (CREATE_TIME)+30 FROM NPCDB.XNP_PORT_MSG);
        DBMS_OUTPUT.PUT_LINE(to_char(sysdate, 'YYYY-MM-DD_HH24:MI:SS')||' QUERIES EXECUTED ON NPCDB.XNP_PORT_MSG');
        DBMS_OUTPUT.PUT_LINE(PR_ROW_COUNT||' ROWS ARRANGED IN NPCDB.XNP_PORT_MSG');
/*Insert the records*/
    MERGE INTO NPCDB.XNP_PORT_MSG DEST
    USING NPCDB.XNP_PORT_MSG@NPCDBTB SOURCE
    ON (DEST.REQ_SEQ = SOURCE.REQ_SEQ)
    WHEN MATCHED THEN
    UPDATE
    SET
        DEST.SERVICE_TYPE = SOURCE.SERVICE_TYPE,
        DEST.MSG_TYPE = SOURCE.MSG_TYPE,
        DEST.MONTH = SOURCE.MONTH,
        DEST.LATEST_MSG = SOURCE.LATEST_MSG,
        DEST.TRANSPORT_TYPE = SOURCE.TRANSPORT_TYPE,
        DEST.TRANSPORT_FROM = SOURCE.TRANSPORT_FROM,
        DEST.TRANSPORT_TO = SOURCE.TRANSPORT_TO,
        DEST.DONOR_ID = SOURCE.DONOR_ID,
        DEST.RECIPIENT_ID = SOURCE.RECIPIENT_ID,
        DEST.NP_DUE_DATE = SOURCE.NP_DUE_DATE,
        DEST.TRANS_TIME = SOURCE.TRANS_TIME,
        DEST.REJECT_CODE = SOURCE.REJECT_CODE,
        DEST.NETWORK_ID = SOURCE.NETWORK_ID,
        DEST.PORT_ID = SOURCE.PORT_ID,
        DEST.ORIGINATOR_ID = SOURCE.ORIGINATOR_ID,
        DEST.PARTICIPANT_ID = SOURCE.PARTICIPANT_ID,
        DEST.MSG_ORIG_TIME = SOURCE.MSG_ORIG_TIME,
        DEST.MSG_EXP_TIME = SOURCE.MSG_EXP_TIME,
        DEST.CREATE_TIME = SOURCE.CREATE_TIME,
        DEST.CAUSE_TYPE = SOURCE.CAUSE_TYPE,
        DEST.PORT_REQ_FORM_ID = SOURCE.PORT_REQ_FORM_ID,
        DEST.TYPE_OF_CONNECTION = SOURCE.TYPE_OF_CONNECTION,
        DEST.ACTION_CODE = SOURCE.ACTION_CODE,
        DEST.NEW_DUE_DATE = SOURCE.NEW_DUE_DATE,
        DEST.WAS_PROCESSED = SOURCE.WAS_PROCESSED,
        DEST.NUM_RANGES = SOURCE.NUM_RANGES,
        DEST.REJECTED_MSG_TYPE = SOURCE.REJECTED_MSG_TYPE,
        DEST.USERID = SOURCE.USERID,
        DEST.COMMENTS_1 = SOURCE.COMMENTS_1,
        DEST.COMMENTS_2 = SOURCE.COMMENTS_2
    WHERE 
        SOURCE.CREATE_TIME > (SELECT MAX(CREATE_TIME)FROM NPCDB.XNP_PORT_MSG)
    AND SOURCE.CREATE_TIME <= (SELECT MAX (CREATE_TIME)+30 FROM NPCDB.XNP_PORT_MSG)
    WHEN NOT MATCHED THEN
    INSERT VALUES
        (SOURCE.REQ_SEQ,
        SOURCE.SERVICE_TYPE,
        SOURCE.MSG_TYPE,
        SOURCE.MONTH,
        SOURCE.LATEST_MSG,
        SOURCE.TRANSPORT_TYPE,
        SOURCE.TRANSPORT_FROM,
        SOURCE.TRANSPORT_TO,
        SOURCE.DONOR_ID,
        SOURCE.RECIPIENT_ID,
        SOURCE.NP_DUE_DATE,
        SOURCE.TRANS_TIME,
        SOURCE.REJECT_CODE,
        SOURCE.NETWORK_ID,
        SOURCE.PORT_ID,
        SOURCE.ORIGINATOR_ID,
        SOURCE.PARTICIPANT_ID,
        SOURCE.MSG_ORIG_TIME,
        SOURCE.MSG_EXP_TIME,
        SOURCE.CREATE_TIME,
        SOURCE.CAUSE_TYPE,
        SOURCE.PORT_REQ_FORM_ID,
        SOURCE.TYPE_OF_CONNECTION,
        SOURCE.ACTION_CODE,
        SOURCE.NEW_DUE_DATE,
        SOURCE.WAS_PROCESSED,
        SOURCE.NUM_RANGES,
        SOURCE.REJECTED_MSG_TYPE,
        SOURCE.USERID,
        SOURCE.COMMENTS_1,
        SOURCE.COMMENTS_2)
    WHERE
        SOURCE.CREATE_TIME > (SELECT MAX(CREATE_TIME)FROM NPCDB.XNP_PORT_MSG)
    AND SOURCE.CREATE_TIME <= (SELECT MAX(CREATE_TIME)+30 FROM NPCDB.XNP_PORT_MSG);
    ARC_ROW_COUNT:=SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE(ARC_ROW_COUNT||' ROWS AFFECTED IN NPCDB.XNP_PORT_MSG');
/*Insertion Validation*/
    IF ARC_ROW_COUNT = PR_ROW_COUNT THEN
        DBMS_OUTPUT.PUT_LINE('RECORDS MATCHED IN NPCDB.XNP_PORT_MSG');
        DBMS_OUTPUT.PUT_LINE('');
    ELSE
        DBMS_OUTPUT.PUT_LINE('RECORDS NOT MATCHED ,PROCEED ROLLBACK IN NPCDB.XNP_PORT_MSG');
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('');
    END IF;


/********************NPCDB.XNP_PORTED_NUMBERS********************/ 
/*Records arranged to be insert*/
        SELECT COUNT(ROWNUM)INTO PR_ROW_COUNT FROM NPCDB.XNP_PORTED_NUMBERS@NPCDBTB SOURCE
            WHERE SOURCE.UPDATE_TIME > (SELECT MAX(UPDATE_TIME)FROM NPCDB.XNP_PORTED_NUMBERS)
            AND SOURCE.UPDATE_TIME <= (SELECT MAX (UPDATE_TIME)+30 FROM NPCDB.XNP_PORTED_NUMBERS);
        DBMS_OUTPUT.PUT_LINE(to_char(sysdate, 'YYYY-MM-DD_HH24:MI:SS')||' QUERIES EXECUTED ON NPCDB.XNP_PORTED_NUMBERS');
        DBMS_OUTPUT.PUT_LINE(PR_ROW_COUNT||' ROWS ARRANGED IN NPCDB.XNP_PORTED_NUMBERS');
/*Insert the records*/
    MERGE INTO NPCDB.XNP_PORTED_NUMBERS DEST
    USING NPCDB.XNP_PORTED_NUMBERS@NPCDBTB SOURCE
    ON (DEST.NUMBER_FROM = SOURCE.NUMBER_FROM)
    WHEN MATCHED THEN
    UPDATE
    SET
        DEST.PORT_ID = SOURCE.PORT_ID,
        DEST.STATUS= SOURCE.STATUS,
        DEST.UPDATE_TIME = SOURCE.UPDATE_TIME,
        DEST.CURRENT_OWNER = SOURCE.CURRENT_OWNER,
        DEST.PREV_OWNER = SOURCE.PREV_OWNER,
        DEST.ORIG_OWNER = SOURCE.ORIG_OWNER,
        DEST.ROUTE = SOURCE.ROUTE,
        DEST.PRIMARY_NUMBER = SOURCE.PRIMARY_NUMBER,
        DEST.DATA_NUMBER = SOURCE.DATA_NUMBER,
        DEST.FAX_NUMBER = SOURCE.FAX_NUMBER
    WHERE 
        SOURCE.UPDATE_TIME > (SELECT MAX(UPDATE_TIME)FROM NPCDB.XNP_PORTED_NUMBERS)
    AND SOURCE.UPDATE_TIME <= (SELECT MAX (UPDATE_TIME)+30 FROM NPCDB.XNP_PORTED_NUMBERS)
    WHEN NOT MATCHED THEN
    INSERT VALUES
        (SOURCE.NUMBER_FROM,
        SOURCE.NUMBER_TO,
        SOURCE.PORT_ID,
        SOURCE.STATUS,
        SOURCE.UPDATE_TIME,
        SOURCE.CURRENT_OWNER,
        SOURCE.PREV_OWNER,
        SOURCE.ORIG_OWNER,
        SOURCE.ROUTE,
        SOURCE.PRIMARY_NUMBER,
        SOURCE.DATA_NUMBER,
        SOURCE.FAX_NUMBER)
    WHERE
        SOURCE.UPDATE_TIME > (SELECT MAX(UPDATE_TIME)FROM NPCDB.XNP_PORTED_NUMBERS)
        AND SOURCE.UPDATE_TIME <= (SELECT MAX (UPDATE_TIME)+30 FROM NPCDB.XNP_PORTED_NUMBERS);
    ARC_ROW_COUNT := SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE(ARC_ROW_COUNT || ' AFFECTED IN NPCDB.XNP_PORTED_NUMBERS');
/*Insertion Validation*/
    IF ARC_ROW_COUNT = PR_ROW_COUNT THEN
        DBMS_OUTPUT.PUT_LINE('RECORDS MATCHED IN NPCDB.XNP_PORTED_NUMBERS');
        DBMS_OUTPUT.PUT_LINE('');
    ELSE
        DBMS_OUTPUT.PUT_LINE('RECORDS NOT MATCHED ,PROCEED ROLLBACK IN NPCDB.XNP_PORTED_NUMBERS');
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('');
    END IF;
    
    
/********************NPCDB.XNP_TIMER_VIOLATION********************/
/*Records arranged to be insert*/
    Select count(rownum) 
    INTO PR_ROW_COUNT
    FROM NPCDB.XNP_TIMER_VIOLATION@NPCDBTB
        WHERE VIOLATION_TIME > (SELECT MAX(VIOLATION_TIME)FROM NPCDB.XNP_TIMER_VIOLATION)
        AND VIOLATION_TIME <= (SELECT MAX (VIOLATION_TIME)+30 FROM NPCDB.XNP_TIMER_VIOLATION);
    DBMS_OUTPUT.PUT_LINE(to_char(sysdate, 'YYYY-MM-DD_HH24:MI:SS')||' QUERIES EXECUTED ON NPCDB.XNP_TIMER_VIOLATION');
    DBMS_OUTPUT.PUT_LINE (PR_ROW_COUNT||' ROWS ARRANGED IN NPCDB.XNP_TIMER_VIOLATION');
/*Insert the records*/    
    INSERT INTO NPCDB.XNP_TIMER_VIOLATION
        (PORT_ID,
        PARTICIPANT_ID,
        TIMER_TYPE,
        MSG_TYPE,
        VIOLATION_TIME)
    SELECT * FROM NPCDB.XNP_TIMER_VIOLATION@NPCDBTB
        WHERE VIOLATION_TIME > (SELECT MAX(VIOLATION_TIME)FROM NPCDB.XNP_TIMER_VIOLATION)
        AND VIOLATION_TIME <= (SELECT MAX (VIOLATION_TIME)+30 FROM NPCDB.XNP_TIMER_VIOLATION);
    ARC_ROW_COUNT:=SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE(ARC_ROW_COUNT || ' AFFECTED IN NPCDB.XNP_TIMER_VIOLATION');
/*Insertion Validation*/
    IF ARC_ROW_COUNT = PR_ROW_COUNT THEN
        DBMS_OUTPUT.PUT_LINE('RECORDS MATCHED IN NPCDB.XNP_TIMER_VIOLATION');
        DBMS_OUTPUT.PUT_LINE('');
    ELSE
        DBMS_OUTPUT.PUT_LINE('RECORDS NOT MATCHED ,PROCEED ROLLBACK IN NPCDB.XNP_TIMER_VIOLATION');
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('');
    END IF;
    
    
/********************NPCDB.XNP_PORT_IN_PROCESS_CHECK********************/ 
/*Records arranged to be insert*/
    SELECT COUNT(ROWNUM)INTO PR_ROW_COUNT FROM NPCDB.XNP_PORT_IN_PROCESS_CHECK@NPCDBTB SOURCE
        WHERE SOURCE.CREATE_TIME > (SELECT MAX(CREATE_TIME)FROM NPCDB.XNP_PORT_IN_PROCESS_CHECK)
        AND SOURCE.CREATE_TIME <= (SELECT MAX (CREATE_TIME)+30 FROM NPCDB.XNP_PORT_IN_PROCESS_CHECK);
    DBMS_OUTPUT.PUT_LINE(to_char(sysdate, 'YYYY-MM-DD_HH24:MI:SS')||' QUERIES EXECUTED ON NPCDB.XNP_PORT_IN_PROCESS_CHECK');
    DBMS_OUTPUT.PUT_LINE(PR_ROW_COUNT||' ROWS ARRANGED IN NPCDB.XNP_PORT_IN_PROCESS_CHECK');
/*Insert the records*/
    INSERT INTO NPCDB.XNP_PORT_IN_PROCESS_CHECK
        (PORT_ID, 
        TELEPHONE_NUMBER, 
        CREATE_TIME)
    SELECT * FROM NPCDB.XNP_PORT_IN_PROCESS_CHECK@NPCDBTB
        WHERE CREATE_TIME > (SELECT MAX(CREATE_TIME)FROM NPCDB.XNP_PORT_IN_PROCESS_CHECK)
        AND CREATE_TIME <= (SELECT MAX (CREATE_TIME)+30 FROM NPCDB.XNP_PORT_IN_PROCESS_CHECK);
        ARC_ROW_COUNT := SQL%ROWCOUNT;
        DBMS_OUTPUT.PUT_LINE(ARC_ROW_COUNT || ' AFFECTED IN NPCDB.XNP_PORT_IN_PROCESS_CHECK');
/*Insertion Validation*/
    IF ARC_ROW_COUNT = PR_ROW_COUNT THEN
        DBMS_OUTPUT.PUT_LINE('RECORDS MATCHED IN NPCDB.XNP_PORT_IN_PROCESS_CHECK');
        DBMS_OUTPUT.PUT_LINE('');
    ELSE
        DBMS_OUTPUT.PUT_LINE('RECORDS NOT MATCHED ,PROCEED ROLLBACK IN NPCDB.XNP_PORT_IN_PROCESS_CHECK');
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('');
    END IF;
    
    
/********************NPCDB.XNP_PORT_MSG_RANGE********************/ 
/*Records arranged to be insert*/
    SELECT COUNT(ROWNUM)INTO PR_ROW_COUNT FROM NPCDB.XNP_PORT_MSG_RANGE@NPCDBTB SOURCE
        WHERE SOURCE.CREATE_TIME > (SELECT MAX(CREATE_TIME)FROM NPCDB.XNP_PORT_MSG_RANGE)
        AND SOURCE.CREATE_TIME <= (SELECT MAX (CREATE_TIME)+30 FROM NPCDB.XNP_PORT_MSG_RANGE);
    DBMS_OUTPUT.PUT_LINE(to_char(sysdate, 'YYYY-MM-DD_HH24:MI:SS')||' QUERIES EXECUTED ON NPCDB.XNP_PORT_MSG_RANGE');
    DBMS_OUTPUT.PUT_LINE(PR_ROW_COUNT||' ROWS ARRANGED IN NPCDB.XNP_PORT_MSG_RANGE');
/*Insert the records*/
    INSERT INTO NPCDB.XNP_PORT_MSG_RANGE
        (PORT_ID, 
        REQ_SEQ, 
        RANGE_ID, 
        NUMBER_FROM, 
        NUMBER_TO, 
        MONTH, 
        DATA_NUMBER, 
        FAX_NUMBER, 
        NETWORK_ID, 
        REGION_CODE,
        NEW_ROUTE, 
        NP_DUE_DATE, 
        REJECT_CMTS, 
        CREATE_TIME,
        UPDATE_TIME)
    SELECT * FROM NPCDB.XNP_PORT_MSG_RANGE@NPCDBTB
        WHERE CREATE_TIME > (SELECT MAX(CREATE_TIME)FROM NPCDB.XNP_PORT_MSG_RANGE)
        AND CREATE_TIME <= (SELECT MAX (CREATE_TIME)+30 FROM NPCDB.XNP_PORT_MSG_RANGE);
    ARC_ROW_COUNT := SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE(ARC_ROW_COUNT || ' AFFECTED IN NPCDB.XNP_PORT_MSG_RANGE');
/*Insertion Validation*/
    IF ARC_ROW_COUNT = PR_ROW_COUNT THEN
        DBMS_OUTPUT.PUT_LINE('RECORDS MATCHED IN NPCDB.XNP_PORT_MSG_RANGE');
        DBMS_OUTPUT.PUT_LINE('');
    ELSE
        DBMS_OUTPUT.PUT_LINE('RECORDS NOT MATCHED ,PROCEED ROLLBACK IN NPCDB.XNP_PORT_MSG_RANGE');
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('');
END IF;


/********************NPCDB.XNP_PORT_DATA_RANGE********************/
/*Records arranged to be insert*/
    SELECT COUNT(ROWNUM)INTO PR_ROW_COUNT FROM NPCDB.XNP_PORT_DATA_RANGE@NPCDBTB SOURCE
        WHERE SOURCE.CREATE_TIME > (SELECT MAX(CREATE_TIME)FROM NPCDB.XNP_PORT_DATA_RANGE)
        AND SOURCE.CREATE_TIME <= (SELECT MAX (CREATE_TIME)+30 FROM NPCDB.XNP_PORT_DATA_RANGE);
    DBMS_OUTPUT.PUT_LINE(to_char(sysdate, 'YYYY-MM-DD_HH24:MI:SS')||' QUERIES EXECUTED ON NPCDB.XNP_PORT_DATA_RANGE');
    DBMS_OUTPUT.PUT_LINE(PR_ROW_COUNT||' ROWS ARRANGED IN NPCDB.XNP_PORT_DATA_RANGE');
/*Insert the records*/
    INSERT INTO NPCDB.XNP_PORT_DATA_RANGE 
        (PORT_ID,
        RANGE_ID,
        NUMBER_FROM,
        NUMBER_TO,
        MONTH,
        DATA_NUMBER,
        FAX_NUMBER,
        NETWORK_ID,
        REGION_CODE,
        NEW_ROUTE,
        NP_DUE_DATE,
        REJECT_CMTS,
        STATUS,MSG_TYPE,
        CREATE_TIME)
    SELECT * FROM NPCDB.XNP_PORT_DATA_RANGE@NPCDBTB
        WHERE CREATE_TIME > (SELECT MAX(CREATE_TIME)FROM NPCDB.XNP_PORT_DATA_RANGE)
        AND CREATE_TIME <= (SELECT MAX (CREATE_TIME)+30 FROM NPCDB.XNP_PORT_DATA_RANGE);
        ARC_ROW_COUNT := SQL%ROWCOUNT;
        DBMS_OUTPUT.PUT_LINE(ARC_ROW_COUNT || ' AFFECTED IN NPCDB.XNP_PORT_DATA_RANGE');
/*Insertion Validation*/
    IF ARC_ROW_COUNT = PR_ROW_COUNT THEN
        DBMS_OUTPUT.PUT_LINE('RECORDS MATCHED IN NPCDB.XNP_PORT_DATA_RANGE');
        DBMS_OUTPUT.PUT_LINE('');
    ELSE
        DBMS_OUTPUT.PUT_LINE('RECORDS NOT MATCHED ,PROCEED ROLLBACK IN NPCDB.XNP_PORT_DATA_RANGE');
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('');
    END IF;


/********************NPCDB.XNP_PORT_REJECT_CODES********************/ 
/*Records arranged to be insert*/
    SELECT COUNT(ROWNUM)INTO PR_ROW_COUNT FROM NPCDB.XNP_PORT_REJECT_CODES@NPCDBTB SOURCE
         WHERE /*Converting PORT_ID into date using substring before comparing*/
        (TO_DATE
            (
                SUBSTR(PORT_ID,2,4) || '-' || SUBSTR(PORT_ID,6,2) || '-' || SUBSTR(PORT_ID,8,2),'YYYY-MM-DD'
            )
        )> 
        (SELECT MAX
            (TO_DATE
                (
                    (
                        SUBSTR(PORT_ID,2,4) || '-' || SUBSTR(PORT_ID,6,2) || '-' || SUBSTR(PORT_ID,8,2)
                    ),'YYYY-MM-DD'
                )
            )FROM NPCDB.XNP_PORT_REJECT_CODES
        )
        AND
        (TO_DATE
            (
                SUBSTR(PORT_ID,2,4) || '-' || SUBSTR(PORT_ID,6,2) || '-' || SUBSTR(PORT_ID,8,2),'YYYY-MM-DD'
            )
        )<=
        (SELECT MAX
            (TO_DATE
                (
                    (
                        SUBSTR(PORT_ID,2,4) || '-' || SUBSTR(PORT_ID,6,2) || '-' || SUBSTR(PORT_ID,8,2)
                    ),'YYYY-MM-DD'
                )+30 /*specify the days interval here*/
            )FROM NPCDB.XNP_PORT_REJECT_CODES
        );
    DBMS_OUTPUT.PUT_LINE(to_char(sysdate, 'YYYY-MM-DD_HH24:MI:SS')||' QUERIES EXECUTED ON NPCDB.XNP_PORT_REJECT_CODES');
    DBMS_OUTPUT.PUT_LINE(PR_ROW_COUNT||' ROWS ARRANGED IN NPCDB.XNP_PORT_REJECT_CODES');
/*Insert the records*/
     INSERT INTO NPCDB.XNP_PORT_REJECT_CODES
        (PORT_ID, 
        REQ_SEQ,
        NUMBER_FROM,
        REJECT_CODE,
        REJECT_CODE_SEQ)
    SELECT * FROM NPCDB.XNP_PORT_REJECT_CODES@NPCDBTB
        WHERE /*Converting PORT_ID into date using substring before comparing*/
        (TO_DATE
            (
                SUBSTR(PORT_ID,2,4) || '-' || SUBSTR(PORT_ID,6,2) || '-' || SUBSTR(PORT_ID,8,2),'YYYY-MM-DD'
            )
        )> 
        (SELECT MAX
            (TO_DATE
                (
                    (
                        SUBSTR(PORT_ID,2,4) || '-' || SUBSTR(PORT_ID,6,2) || '-' || SUBSTR(PORT_ID,8,2)
                    ),'YYYY-MM-DD'
                )
            )FROM NPCDB.XNP_PORT_REJECT_CODES
        )
        AND
        (TO_DATE
            (
                SUBSTR(PORT_ID,2,4) || '-' || SUBSTR(PORT_ID,6,2) || '-' || SUBSTR(PORT_ID,8,2),'YYYY-MM-DD'
            )
        )<=
        (SELECT MAX
            (TO_DATE
                (
                    (
                        SUBSTR(PORT_ID,2,4) || '-' || SUBSTR(PORT_ID,6,2) || '-' || SUBSTR(PORT_ID,8,2)
                    ),'YYYY-MM-DD'
                )+30 /*specify the days interval here*/
            )FROM NPCDB.XNP_PORT_REJECT_CODES
        );
    ARC_ROW_COUNT := SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE(ARC_ROW_COUNT || ' AFFECTED IN NPCDB.XNP_PORT_REJECT_CODES');
/*Insertion Validation*/
    IF ARC_ROW_COUNT = PR_ROW_COUNT THEN
        DBMS_OUTPUT.PUT_LINE('RECORDS MATCHED IN NPCDB.XNP_PORT_REJECT_CODES');
        DBMS_OUTPUT.PUT_LINE('');
    ELSE
        DBMS_OUTPUT.PUT_LINE('RECORDS NOT MATCHED ,PROCEED ROLLBACK IN NPCDB.XNP_PORT_REJECT_CODES');
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('');
    END IF;
END;    
/
SPOOL OFF
