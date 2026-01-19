CALL APZ_EXISTSPROC('SHS_Job_ImportNirData');
--GO
CREATE PROCEDURE SHS_Job_ImportNirData (

)
LANGUAGE SQLSCRIPT
SQL SECURITY INVOKER

AS
BEGIN
	-- gen tự động UDO: OARS	@OARS10   @ARS101
	/*
    Bảng tạm #TEMP_NIR_DATA đã được tạo với cấu trúc:
    - ID:  NVARCHAR (Sample Name)
    - DateG: TIMESTAMP (Date/Time)
    - CharCode:  NVARCHAR (Mã chỉ tiêu:  CH31, CH30, CH04, CH27...)
    - Result: DECIMAL (Giá trị)
    */
    DECLARE maxEntry INT;

	DECLARE tbl TABLE 
	(
		"ID" NVARCHAR(50),
		"DateG" TIMESTAMP,
		"CharCode" NVARCHAR(250),
		"Result" NUMERIC(19,6)
	);
	
	IF (SELECT COUNT(1) FROM SYS.M_TEMPORARY_TABLES 
			WHERE SCHEMA_NAME = CURRENT_SCHEMA 
				AND IS_TEMPORARY = 'TRUE' 
				AND TEMPORARY_TABLE_TYPE = 'LOCAL'
				AND TABLE_NAME = '#TEMP_NIR_DATA') > 0 
	THEN
		EXEC 'SELECT * FROM "#TEMP_NIR_DATA"' INTO tbl;	
		EXEC 'DROP TABLE "#TEMP_NIR_DATA"';
	END IF;
	
    SELECT MAX("DocEntry") 
    	INTO maxEntry
        DEFAULT 1
    FROM "@OARS10";
    
    -- insert bảng cha ới các giá trị mặc định
    INSERT INTO "@OARS10" ("DocEntry", "DocNum", "U_Formula", "U_Status", "Remark"
                        , "Period", "Instance", "Series", "Handwrtten", "Canceled", "Object", "LogInst", "UserSign", "Transfered", "Status"
                        , "CreateDate", "CreateTime", "UpdateDate", "UpdateTime", "DataSource", "RequestStatus", "Creator")
    SELECT :maxEntry + 1, :maxEntry + 1, 'F04', 'C', 'Imported from NIR system'
            , 27, 0, -1, 'N', 'N', 'OARS10', NULL, 1, 'N', 'O'
            , CURRENT_DATE, TO_INTEGER(TO_VARCHAR(CURRENT_TIME, 'HH24mi')), CURRENT_DATE, TO_INTEGER(TO_VARCHAR(CURRENT_TIME, 'HH24mi')), 'S', 'W', 'manager'

    FROM DUMMY;

    -- insert bảng con với dữ liệu từ bảng tạm
    INSERT INTO "@ARS101" ("DocEntry", "LineId", "VisOrder", "Object", "LogInst",
                           "U_ID", "U_DateG", "U_CharCode", "U_Result")
    SELECT :maxEntry + 1, ROW_NUMBER() OVER (ORDER BY t."ID", t."CharCode"), ROW_NUMBER() OVER (ORDER BY t."ID", t."CharCode") - 1, 'OARS10', NULL
            , t."ID", t."DateG", t."CharCode", t."Result"
    	FROM :tbl t;
    
    -- gán lại autoKeys
    UPDATE ONNM SET "AutoKey" = (SELECT MAX("DocEntry") FROM "@OARS10") + 1
    WHERE "ObjectCode" = 'OARS10';
    
	--SELECT * FROM :tbl;
END;









